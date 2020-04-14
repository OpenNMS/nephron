/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.nephron;

import java.io.Serializable;
import java.util.Objects;

import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class ResponseTimeTest {
    private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);
    private static final Duration WINDOW_DURATION = Duration.standardMinutes(20);
    private Instant baseTime = new Instant(0);

    @Rule
    public TestPipeline p = TestPipeline.create();

    private enum TestLocation {
        DEFAULT("Default"),
        MINION("Minion");

        String name;

        TestLocation(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private enum TestService {
        MINION_JMX,
        MINION_RPC
    }

    private static class ResponseTimeInfo implements Serializable {
        private final String location;
        private final String service;
        private final double responseTimeMs;
        private final long timestampMs;

        public ResponseTimeInfo(String location, String service, double responseTimeMs, long timestampMs) {
            this.location = Objects.requireNonNull(location);
            this.service = Objects.requireNonNull(service);
            this.responseTimeMs = responseTimeMs;
            this.timestampMs = timestampMs;
        }

        public String getLocation() {
            return location;
        }

        public Double getResponseTimeMs() {
            return responseTimeMs;
        }
    }

    static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
    static final Duration TEN_MINUTES = Duration.standardMinutes(10);

    static class CalculateLocationAverages
            extends PTransform<PCollection<ResponseTimeInfo>, PCollection<KV<String, Double>>> {
        private final Duration windowDuration;
        private final Duration allowedLateness;

        CalculateLocationAverages(Duration windowDuration, Duration allowedLatenesss) {
            this.windowDuration = windowDuration;
            this.allowedLateness = allowedLatenesss;
        }

        @Override
        public PCollection<KV<String, Double>> expand(PCollection<ResponseTimeInfo> infos) {
            return infos
                    .apply(
                            "ResponseTimeFixedWindows",
                            Window.<ResponseTimeInfo>into(FixedWindows.of(windowDuration))
                                    // We will get early (speculative) results as well as cumulative
                                    // processing of late data.
                                    .triggering(
                                            AfterWatermark.pastEndOfWindow()
                                                    .withEarlyFirings(
                                                            AfterProcessingTime.pastFirstElementInPane()
                                                                    .plusDelayOf(FIVE_MINUTES))
                                                    .withLateFirings(
                                                            AfterProcessingTime.pastFirstElementInPane()
                                                                    .plusDelayOf(TEN_MINUTES)))
                                    .withAllowedLateness(allowedLateness)
                                    .accumulatingFiredPanes())
                    .apply("ExtractAndAvgResponseTime", new ExtractAndAvgResponseTime());
        }
    }

    public static class ExtractAndAvgResponseTime
            extends PTransform<PCollection<ResponseTimeInfo>, PCollection<KV<String, Double>>> {

        @Override
        public PCollection<KV<String, Double>> expand(PCollection<ResponseTimeInfo> responseTimeInfo) {
            return responseTimeInfo
                    .apply(
                            MapElements.into(
                                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                                    .via((ResponseTimeInfo rInfo) -> KV.of(rInfo.getLocation(), rInfo.getResponseTimeMs())))
                    .apply(Mean.perKey());
        }
    }

    @Test
    public void canDoIt() {
        BoundedWindow window = new IntervalWindow(baseTime, WINDOW_DURATION);
        TestStream<ResponseTimeInfo> infos =
                TestStream.create(SerializableCoder.of(ResponseTimeInfo.class))
                        // Start at the epoch
                        .advanceWatermarkTo(baseTime)
                        .addElements(event(TestLocation.DEFAULT, TestService.MINION_JMX, 0.01d, Duration.ZERO))
                        .addElements(event(TestLocation.MINION, TestService.MINION_JMX, 100d, Duration.standardMinutes(1)))
                        .advanceWatermarkTo(window.maxTimestamp())
                        // ...
                        .advanceWatermarkToInfinity();
        PCollection<KV<String, Double>> locationAverages =
                p.apply(infos).apply(new CalculateLocationAverages(WINDOW_DURATION, ALLOWED_LATENESS));

        String defaultLoc = TestLocation.DEFAULT.getName();
        String minionLoc = TestLocation.MINION.getName();

        IntervalWindow targetWindow = new IntervalWindow(baseTime, WINDOW_DURATION);
        PAssert.that(locationAverages)
                .inWindow(targetWindow)
                .containsInAnyOrder(
                        KV.of(defaultLoc, 0.01d),
                        KV.of(minionLoc, 100d));

        p.run().waitUntilFinish();
    }

    private TimestampedValue<ResponseTimeInfo> event(
            TestLocation location, TestService service, double responseTimeMs, Duration baseTimeOffset) {
        return TimestampedValue.of(
                        new ResponseTimeInfo(
                                location.getName(), service.toString(), responseTimeMs, baseTime.plus(baseTimeOffset).getMillis()),
                baseTime.plus(baseTimeOffset));
    }
}
