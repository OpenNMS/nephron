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
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.restassured.RestAssured;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

/**
 * Tests CortexIo with a cortex backend but not in the context of a pipeline.
 * <p>
 * Lifecycle methods of the {@link CortexIo.WriteFn} class have to be called manually.
 */
public class CortexIoIT {

    private static final int CORTEX_HTTP_PORT = 9009;

    private static final String TEST_METRIC = "test_metric";

    private static boolean USE_LOCAL_CORTEX = false;
    // docker run -d --name cortex -v /home/swachter/projects/opennms/nephron/cortex/src/test/resources/cortex.yaml:/etc/cortex/cortex.yaml -p 9009:9009 -p 9005:9005 cortexproject/cortex:v1.9.0 -config.file=/etc/cortex/cortex.yaml

    @Rule
    public GenericContainer cortex = new GenericContainer("cortexproject/cortex:v1.9.0")
            .withExposedPorts(CORTEX_HTTP_PORT)
            .withClasspathResourceMapping("cortex.yaml", "/etc/cortex/cortex.yaml", BindMode.READ_ONLY)
            .withCommand("-config.file=/etc/cortex/cortex.yaml");

    private int cortexPort() {
        return USE_LOCAL_CORTEX ? 9009 : cortex.getMappedPort(CORTEX_HTTP_PORT);
    }

    public static abstract class CallResult {
        public final Call call;

        public CallResult(Call call) {
            this.call = call;
        }

        public static class Failure extends CallResult {
            public final Exception exception;

            public Failure(Call call, Exception exception) {
                super(call);
                this.exception = exception;
            }
        }

        public static class Success extends CallResult {
            public final Response response;

            public Success(Call call, Response response) {
                super(call);
                this.response = response;
            }
        }
    }

    /**
     * An extension of the {@link CortexIo.WriteFn} class that records the results of calling cortex.
     */
    private static class TestWriteFn extends CortexIo.WriteFn<Double> {

        private CortexIo.BuildFromElementAndTimestamp<Double> build;
        private List<CallResult> results = Collections.synchronizedList(new ArrayList<>());

        public TestWriteFn(CortexIo.Write<Double> spec, CortexIo.BuildFromElementAndTimestamp<Double> build) {
            super(spec);
            this.build = build;
        }

        public List<CallResult> getResults() {
            return results;
        }

        @ProcessElement
        public void processElement(@Element Double element, @Timestamp Instant timestamp) throws Exception {
            outputTimeSeries(builder -> build.accept(element, timestamp, builder));
        }

        @Override
        public void doOnFailure(Call call, IOException e) {
            results.add(new CallResult.Failure(call, e));
        }

        @Override
        public void doOnResponse(Call call, Response response) {
            results.add(new CallResult.Success(call, response));
        }
    }

    private static CortexIo.BuildFromElementAndTimestamp<Double> BUILD_FROM_ELEMENT_AND_TIMESTAMP =
            (element, timestamp, builder) -> builder.addSample(timestamp.getMillis(), element);

    private static CortexIo.CreateWriteFn<Double> CREATE_WRITE_FN =
            spec -> new TestWriteFn(spec, BUILD_FROM_ELEMENT_AND_TIMESTAMP);

    private TestWriteFn setupWriteFn(String metricName) {
        var cortexWrite = CortexIo
                .write("http://localhost:" + cortexPort() + "/api/v1/push", CREATE_WRITE_FN)
                .withMetricName(metricName);
        return (TestWriteFn) cortexWrite.createWriteFn();
    }

    @Test
    public void singleSample() throws Exception {

        var timestamp = Instant.ofEpochMilli(100_000);
        var value = 77.7;

        String metricName = "singleSample";
        TestWriteFn writeFn = setupWriteFn(metricName);

        writeFn.setup();
        writeFn.startBundle();

        writeFn.processElement(value, timestamp);

        writeFn.finishBundle();
        writeFn.closeClient();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> writeFn.getResults().size(), greaterThanOrEqualTo(1));

        var result0 = writeFn.getResults().get(0);

        assertThat(result0, instanceOf(CallResult.Success.class));

        RestAssured.port = cortexPort();

        var response = with()
                .param("query", metricName)
                .param("time", timestamp.getMillis() / 1000)
                .get("/prometheus/api/v1/query");

        response.body().prettyPrint();

        response
                .then()
                .assertThat()
                .body("status", equalTo("success"))
                .body("data.result[0].value[1]", equalTo("77.7"));
    }

    @Test
    public void multipleSamples() throws Exception {

        var start = Instant.ofEpochMilli(100_000);

        String metricName = "multipleSamples";
        TestWriteFn writeFn = setupWriteFn(metricName);

        writeFn.setup();
        writeFn.startBundle();

        var timestamp = start;
        for (int i = 0; i <= 1000; i++) {
            writeFn.processElement((double)i, timestamp);
            timestamp = timestamp.plus(1000);
        }

        writeFn.finishBundle();
        writeFn.closeClient();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> writeFn.getResults().size(), greaterThanOrEqualTo(1));

        var result0 = writeFn.getResults().get(0);

        assertThat(result0, instanceOf(CallResult.Success.class));

        RestAssured.port = cortexPort();

        var response = with()
                .param("query", metricName)
                .param("start", start.getMillis() / 1000)
                .param("end", timestamp.getMillis() / 1000)
                .param("step", 500)
                .get("/prometheus/api/v1/query_range");

        response.body().prettyPrint();

        response
                .then()
                .assertThat()
                .body("status", equalTo("success"))
                .body("data.result[0].values[0][1]", equalTo("0"))
                .body("data.result[0].values[2][1]", equalTo("1000"));
    }

    @Test
    public void outOfOrderSampleInSingleBundleIsIgnored() throws Exception {

        var timestamp = Instant.ofEpochMilli(100_000);

        String metricName = "outOfOrderSampleFails";
        TestWriteFn writeFn = setupWriteFn(metricName);

        writeFn.setup();

        writeFn.startBundle();
        writeFn.processElement(1.0, timestamp);
        // write a different value one second earlier -> the value gets ignored
        writeFn.processElement(2.0, timestamp.minus(1000));
        writeFn.finishBundle();

        writeFn.closeClient();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> writeFn.getResults().size(), greaterThanOrEqualTo(1));

        var result0 = writeFn.getResults().get(0);

        assertThat(result0, instanceOf(CallResult.Success.class));

        assertThat(((CallResult.Success)result0).response.code(), equalTo(200));

        RestAssured.port = cortexPort();

        var response = with()
                .param("query", metricName)
                .param("time", timestamp.getMillis() / 1000)
                .get("/prometheus/api/v1/query");

        response.body().prettyPrint();

        response
                .then()
                .assertThat()
                .body("status", equalTo("success"))
                .body("data.result[0].value[1]", equalTo("1"));

        var response2 = with()
                .param("query", metricName)
                .param("time", timestamp.minus(1000).getMillis() / 1000)
                .get("/prometheus/api/v1/query");

        response2.body().prettyPrint();

        response2
                .then()
                .assertThat()
                .body("status", equalTo("success"))
                .body("data.result.size()", equalTo(0));
    }

    @Test
    public void outOfOrderBundlesInSeparateBundlesFail() throws Exception {

        var timestamp = Instant.ofEpochMilli(100_000);

        String metricName = "outOfOrderBundlesInSeparateBundlesFail";
        TestWriteFn writeFn = setupWriteFn(metricName);

        writeFn.setup();

        writeFn.startBundle();
        writeFn.processElement(1.0, timestamp);
        writeFn.finishBundle();
        writeFn.startBundle();
        // try to write a different value at the same timestamp -> this should fail
        writeFn.processElement(2.0, timestamp.minus(1000));
        writeFn.finishBundle();

        writeFn.closeClient();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> writeFn.getResults().size(), greaterThanOrEqualTo(1));

        var result0 = writeFn.getResults().get(0);
        var result1 = writeFn.getResults().get(1);

        assertThat(result0, instanceOf(CallResult.Success.class));
        assertThat(result1, instanceOf(CallResult.Success.class));

        assertThat(((CallResult.Success)result0).response.code(), equalTo(200));
        assertThat(((CallResult.Success)result1).response.code(), equalTo(400));
    }

    @Test
    public void updateSampleFails() throws Exception {

        var timestamp = Instant.ofEpochMilli(100_000);

        String metricName = "updateSampleFails";
        TestWriteFn writeFn = setupWriteFn(metricName);

        writeFn.setup();

        writeFn.startBundle();
        writeFn.processElement(1.0, timestamp);
        writeFn.finishBundle();

        writeFn.startBundle();
        // try to write a different value at the same timestamp -> this should fail
        writeFn.processElement(2.0, timestamp);
        writeFn.finishBundle();

        writeFn.closeClient();

        await().atMost(15, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> writeFn.getResults().size(), greaterThanOrEqualTo(2));

        var result0 = writeFn.getResults().get(0);
        var result1 = writeFn.getResults().get(1);

        assertThat(result0, instanceOf(CallResult.Success.class));
        assertThat(result1, instanceOf(CallResult.Success.class));

        assertThat(((CallResult.Success)result0).response.code(), equalTo(200));
        assertThat(((CallResult.Success)result1).response.code(), equalTo(400));
    }

}
