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

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import static io.restassured.RestAssured.*;

import io.restassured.RestAssured;

public class CortexIoIT {

    final int cortexHttpPort = 9009;

    final String metricName = "test_metric";

    // docker run -d --name cortex -v /home/swachter/projects/opennms/nephron/cortex/src/test/resources/cortex.yaml:/etc/cortex/cortex.yaml -p 9009:9009 -p 9005:9005 cortexproject/cortex:v1.9.0 -config.file=/etc/cortex/cortex.yaml

    @Rule
    public GenericContainer cortex = new GenericContainer("cortexproject/cortex:v1.9.0")
            .withExposedPorts(cortexHttpPort)
            .withClasspathResourceMapping("cortex.yaml", "/etc/cortex/cortex.yaml", BindMode.READ_ONLY)
            .withCommand("-config.file=/etc/cortex/cortex.yaml")
            ;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    public void test(SerializableFunction<CortexIo.Write, CortexIo.WriteFn<Integer>> createWriteFn) {

        String pushPath = "/api/v1/push";

        var mappedCortexHttpPort = cortex.getMappedPort(cortexHttpPort);

        var cortexWrite = CortexIo
                .write("http://localhost:" + mappedCortexHttpPort + pushPath, createWriteFn)
                .withOrgId("opennms")
                .withMetricName(metricName);

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
                .apply(MapElements.via(CortexIoTest.addKey))
                .apply(Combine.perKey(CortexIoTest::sum))
                .apply(Values.create())
                .apply(cortexWrite)
        ;

        var pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        RestAssured.port = mappedCortexHttpPort;

        var body = with()
                .param("query", metricName + "{evenOrOdd=\"odd\"}")
                .param("start", 0)
                .param("end", 30)
                .param("step", 2)
                .get("/prometheus/api/v1/query_range")
                .body();

        System.out.println(body.prettyPrint());

    }

    @Test
    public void test1() {
        test(CortexIo.writeFn(CortexIoTest::buildFromIntAndTimestamp));
    }

}
