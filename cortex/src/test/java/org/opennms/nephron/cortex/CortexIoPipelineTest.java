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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.verify.VerificationTimes;

/**
 * Tests CortexIo without a Cortex backend but in the context of a pipeline.
 */
public class CortexIoPipelineTest {

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private static final String PUSH_PATH = "/api/v1/push";

    public void test(
            MockServerClient mockServerClient,
            CortexIo.Write cortexWrite,
            int windowSizeSeconds,
            int numWindows,
            int numExpectedSamples
    ) {

        mockServerClient
                .when(request().withMethod("POST").withPath(PUSH_PATH))
                .respond(response().withStatusCode(201));

        var testStreamBuilder = TestStream.create(VarIntCoder.of());

        for (int w = 0; w < numWindows; w++) {
            for (int s = 0; s < windowSizeSeconds; s++) {
                Instant timestamp = Instant.ofEpochMilli(w * windowSizeSeconds * 1000);
                testStreamBuilder = testStreamBuilder.addElements(TimestampedValue.of(w * windowSizeSeconds + s, timestamp));
                testStreamBuilder = testStreamBuilder.advanceWatermarkTo(timestamp);
            }
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
                .apply(MapElements.via(ADD_KEY))
                .apply(cortexWrite)
        ;

        var pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        mockServerClient.verify(
                request().withPath(PUSH_PATH),
                VerificationTimes.exactly(numExpectedSamples)
        );

    }

    public static SimpleFunction<Integer, KV<Integer, Integer>> ADD_KEY = new SimpleFunction<>() {
        @Override
        public KV<Integer, Integer> apply(Integer input) {
            // use the same key for all elements
            // -> when elements are combined per key the result windows contains exactly one key / value entry
            return KV.of(1, input);
        }
    };

    @Test
    public void samplesGetAccumulated() {
        var mockServerClient = mockServerRule.getClient();
        var cortexWrite = CortexIo.of(
                "http://localhost:" + mockServerClient.getPort() + PUSH_PATH,
                CortexIoPipelineTest::buildTimeSeries,
                VarIntCoder.of(),
                VarIntCoder.of(),
                CortexIoPipelineTest::plus,
                Duration.standardSeconds(5)
        ).withMaxBatchSize(1);
        test(mockServerClient, cortexWrite, 5, 3, 3); // a sample for each window
    }

    @Test
    public void samplesGetPassedThrough() {
        var mockServerClient = mockServerRule.getClient();
        var cortexWrite = CortexIo.of(
                "http://localhost:" + mockServerClient.getPort() + PUSH_PATH,
                CortexIoPipelineTest::buildTimeSeries
        ).withMaxBatchSize(1);
        test(mockServerClient, cortexWrite, 5, 3, 5 * 3); // a sample for each input value
    }

    public static void buildTimeSeries(
            Integer key,
            Integer value,
            Instant timestamp,
            int index,
            TimeSeriesBuilder builder
    ) {
        builder
                .addLabel("index", index)
                .addSample(timestamp.getMillis(), value);
    }

    private static int plus(int i1, int i2) {
        return i1 + i2;
    }
}
