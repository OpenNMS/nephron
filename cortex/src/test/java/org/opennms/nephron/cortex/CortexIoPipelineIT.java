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

import static io.restassured.RestAssured.with;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.opennms.nephron.cortex.CortexIoPipelineTest.ADD_KEY;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.restassured.RestAssured;

/**
 * Tests CortexIo with a cortex backend when used in a pipeline.
 */
public class CortexIoPipelineIT {

    private static final Logger LOG = LoggerFactory.getLogger(CortexIoPipelineIT.class);

    private static final int CORTEX_HTTP_PORT = 9009;

    private static final String TEST_METRIC = "test_metric";

    private static boolean USE_LOCAL_CORTEX = false;
    // docker run -d --name cortex -v /home/swachter/projects/opennms/nephron/cortex/src/test/resources/cortex.yaml:/etc/cortex/cortex.yaml -p 9009:9009 -p 9005:9005 cortexproject/cortex:v1.9.0 -config.file=/etc/cortex/cortex.yaml

    @Rule
    public GenericContainer cortex = new GenericContainer("cortexproject/cortex:v1.9.0")
            .withExposedPorts(CORTEX_HTTP_PORT)
            .withClasspathResourceMapping("cortex.yaml", "/etc/cortex/cortex.yaml", BindMode.READ_ONLY)
            .withCommand("-config.file=/etc/cortex/cortex.yaml")
            ;

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    private int cortexPort() {
        return USE_LOCAL_CORTEX ? 9009 : cortex.getMappedPort(CORTEX_HTTP_PORT);
    }

    private static CortexIo.BuildTimeSeries<Integer, Integer> BUILD_TIME_SERIES =
            (key, value, timestamp, index, builder) -> builder
                    .addLabel("idx", String.valueOf(index))
                    .addSample(timestamp.getMillis(), value);

    private CortexIo.Write<Integer, Integer> setupWrite(String metricName) {
        return CortexIo
                .of("http://localhost:" + cortexPort() + "/api/v1/push", BUILD_TIME_SERIES)
                .withMetricName(metricName)
                ;
    }

    @Test
    public void manyWindowsSamples() {

        // generate many windows, each containing a single value
        // -> window completions may be processed in parallel and may happen out of order
        // -> the Cortex sink attaches unique indexes as necessary

        var metricName = "test";

        var cortexWrite = setupWrite(metricName);

        var testStreamBuilder = TestStream.create(VarIntCoder.of());

        var startSecond = 60;
        var windowSizeSeconds = 1;
        var windows = 1000;

        for (int i = 0; i < windows; i++) {
            Instant timestamp = Instant.ofEpochMilli((startSecond + i * windowSizeSeconds) * 1000);
            testStreamBuilder = testStreamBuilder.addElements(TimestampedValue.of(startSecond + i, timestamp));
            testStreamBuilder = testStreamBuilder.advanceWatermarkTo(timestamp);
        }
        var testStream = testStreamBuilder.advanceWatermarkToInfinity();

        pipeline
                .apply(testStream)
                .apply(
                        Window.<Integer>into(FixedWindows.of(Duration.standardSeconds(windowSizeSeconds)))
                                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
                                .triggering(AfterWatermark.pastEndOfWindow())
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                )
                .apply(MapElements.via(ADD_KEY))
                .apply(cortexWrite)
        ;

        var pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();

        var mqrs1 = pipelineResult.metrics().queryMetrics(MetricsFilter.builder().addNameFilter(MetricNameFilter.named(CortexIo.CORTEX_METRIC_NAMESPACE, CortexIo.CORTEX_WRITE_FAILURE_METRIC_NAME)).build());
        var writeFailures = StreamSupport.stream(Spliterators.spliteratorUnknownSize(mqrs1.getCounters().iterator(), Spliterator.ORDERED), false).mapToInt(mr -> mr.getAttempted().intValue()).sum();
        assertThat("writeFailures", writeFailures, is(0));

        var mqrs2 = pipelineResult.metrics().queryMetrics(MetricsFilter.builder().addNameFilter(MetricNameFilter.named(CortexIo.CORTEX_METRIC_NAMESPACE, CortexIo.CORTEX_RESPONSE_FAILURE_METRIC_NAME)).build());
        var responseFailures = StreamSupport.stream(Spliterators.spliteratorUnknownSize(mqrs2.getCounters().iterator(), Spliterator.ORDERED), false).mapToInt(mr -> mr.getAttempted().intValue()).sum();
        assertThat("responseFailures", responseFailures, is(0));

        RestAssured.port = cortexPort();

        var response = with()
                .param("query", metricName)
                .param("start", startSecond)
                .param("end", startSecond + (windows-1) * windowSizeSeconds)
                .param("step", windowSizeSeconds)
                .get("/prometheus/api/v1/query_range");

        var responseString = response.body().asPrettyString();

        // navigate to the result value list
        var responseMap = response.as(Map.class);
        var data = (Map)responseMap.get("data");
        var result = (List)data.get("result");
        var resultItem0 = (Map)result.get(0);
        var values = (List<List>)resultItem0.get("values");
        // collect all distinct values returned by querying Cortex
        var distinct = new HashSet<Integer>();
        for (var v: values) {
            distinct.add(Integer.valueOf((String)v.get(1)));
        }

        // `distinct` should contain consecutive values for all seconds
        var missing = new ArrayList<Integer>();
        for (int i = startSecond; i < startSecond + windows; i++) {
            if (!distinct.contains(i)) missing.add(i);
        }

        assertThat("missing values in Cortex response -  missing: " + missing + "; response: " + responseString, missing, hasSize(0));

    }

}
