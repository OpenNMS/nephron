/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
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

package org.opennms.nephron.cortex;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.junit.MockServerRule;
import org.mockserver.verify.VerificationTimes;

import prometheus.PrometheusTypes;

public class CortexIoTest {

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    public void test(SerializableFunction<CortexIo.Write, CortexIo.WriteFn<Integer>> createWriteFn) {

        var mockServerClient = mockServerRule.getClient();

        String pushPath = "/api/v1/push";
        mockServerClient
                .when(request().withMethod("POST").withPath(pushPath))
                .respond(response().withStatusCode(201));

        var cortexWrite = CortexIo.write("http://localhost:" + mockServerClient.getPort() + pushPath, createWriteFn);


        var testStreamBuilder = TestStream.create(VarIntCoder.of());

        var windowSizeSeconds = 5;
        var numWindows = 3;

        for (int i = 0; i < windowSizeSeconds * numWindows; i++) {
            Instant timestamp = Instant.ofEpochMilli(i * 1000);
            testStreamBuilder = testStreamBuilder.addElements(TimestampedValue.of(i, timestamp));
            testStreamBuilder = testStreamBuilder.advanceWatermarkTo(timestamp);
        }
        var testStream = testStreamBuilder.advanceWatermarkToInfinity();

        pipeline
                .apply(testStream)
                .apply(
                        Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(windowSizeSeconds)))
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                )
                .apply(MapElements.via(addKey))
                .apply(Combine.perKey(CortexIoTest::sum))
                .apply(Values.create())
                .apply(cortexWrite)
        ;

        var pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        mockServerClient.verify(
                request().withPath(pushPath),
                VerificationTimes.exactly(numWindows)
        );

    }

    public static int sum(Iterable<Integer> iter) {
        var sum = 0;
        for (var i: iter) {
            sum += i;
        }
        return sum;
    }

    public static SimpleFunction<Integer, KV<Integer, Integer>> addKey = new SimpleFunction<>() {
        @Override
        public KV<Integer, Integer> apply(Integer input) {
            // use the same key for all elements
            // -> the result windows contains exactly one key / value entry
            return KV.of(1, input);
        }
    };

    @Test
    public void test1() {
        test(CortexIo.writeFn(CortexIoTest::buildFromIntAndTimestamp));
    }

    @Test
    public void test2() {
        test(CortexIo.writeFn(CortexIoTest::buildFromProcessContext));
    }

    @Test
    public void test3() {
        test(CortexIo.writeFn(CortexIoTest::buildFromProcessContextAndWindow));
    }


    public static void buildFromIntAndTimestamp(
            Integer value,
            Instant timestamp,
            PrometheusTypes.TimeSeries.Builder builder
    ) {
        builder.addLabels(
                PrometheusTypes.Label.newBuilder()
                        .setName("evenOrAdd")
                        .setValue(value % 2 == 0 ? "even" : "odd")
        );
        builder.addSamples(
                PrometheusTypes.Sample.newBuilder()
                        .setTimestamp(timestamp.getMillis())
                        .setValue(value)
        );
    }

    public static void buildFromProcessContext(
            DoFn<Integer, Void>.ProcessContext processContext,
            PrometheusTypes.TimeSeries.Builder builder
    ) {
        var value = processContext.element();
        var timestamp = processContext.timestamp();
        var pane = processContext.pane();
        builder.addLabels(
                PrometheusTypes.Label.newBuilder()
                        .setName("evenOrAdd")
                        .setValue(value % 2 == 0 ? "even" : "odd")
        );
        builder.addLabels(
                PrometheusTypes.Label.newBuilder()
                        .setName("pane")
                        .setValue(pane.getTiming().name() + '-' + pane.getIndex())
        );
        builder.addSamples(
                PrometheusTypes.Sample.newBuilder()
                        .setTimestamp(timestamp.getMillis())
                        .setValue(value)
        );
    }

    public static void buildFromProcessContextAndWindow(
            DoFn<Integer, Void>.ProcessContext processContext,
            BoundedWindow window,
            PrometheusTypes.TimeSeries.Builder builder
    ) {
        var value = processContext.element();
        var timestamp = processContext.timestamp();
        builder.addLabels(
                PrometheusTypes.Label.newBuilder()
                        .setName("evenOrAdd")
                        .setValue(value % 2 == 0 ? "even" : "odd")
        );
        builder.addLabels(
                PrometheusTypes.Label.newBuilder()
                        .setName("maxWindowTimestamp")
                        .setValue(String.valueOf(window.maxTimestamp()))
        );
        builder.addSamples(
                PrometheusTypes.Sample.newBuilder()
                        .setTimestamp(timestamp.getMillis())
                        .setValue(value)
        );
    }

}
