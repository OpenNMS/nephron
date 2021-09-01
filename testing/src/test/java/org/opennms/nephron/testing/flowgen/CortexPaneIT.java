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

package org.opennms.nephron.testing.flowgen;

import static io.restassured.RestAssured.with;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opennms.nephron.Pipeline.registerCoders;
import static org.opennms.nephron.testing.flowgen.TotalVolumeTest.countVolumes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.CompoundKey;
import org.opennms.nephron.FlowSummaryData;
import org.opennms.nephron.Pipeline;
import org.opennms.nephron.RefType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

import io.restassured.RestAssured;

public class CortexPaneIT {

    private static Logger LOG = LoggerFactory.getLogger(CortexPaneIT.class);

    private static final int CORTEX_HTTP_PORT = 9009;

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

    @Test
    public void test() {

        FlowGenOptions options = PipelineOptionsFactory.fromArgs(
                "--defaultMaxInputDelayMs=30000",
                "--runner=FlinkRunner",
//                "--flinkMaster=localhost:8081",
                "--parallelism=4",
                "--lateProcessingDelayMs=240000",
                "--earlyProcessingDelayMs=5000",
                "--playbackMode=true",
                "--numWindows=10",
                "--flowsPerWindow=10000",
                "--lastSwitchedSigmaMs=25000",
                "--numExporters=2",
                "--numInterfaces=2",
                "--numApplications=2",
                "--numHosts=2",
                "--numEcns=2",
                "--numDscps=2",
                "--flowsPerSecond=0",
                "--cortexWriteUrl=http://localhost:" + cortexPort() + "/api/v1/push"
        ).withValidation().as(FlowGenOptions.class);

        // the first exporter has a frozen clock that always returns the start time
        SyntheticFlowTimestampPolicyFactory tpf =
                SyntheticFlowTimestampPolicyFactory.withLimitedDelay(options, Pipeline.ReadFromKafka::getTimestamp);

        var sourceConfig = SourceConfig.of(options, skewedLastSwitchedPolicy(options), tpf);

        var expected = TotalVolumeTest.aggregateInMemory(options, FlowDocuments.stream(sourceConfig));

        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(pipeline);

        PCollection<FlowSummaryData> rawFlowSummaries = pipeline
                .apply(SyntheticFlowSource.readFromSyntheticSource(sourceConfig))
                .apply(new Pipeline.CalculateFlowStatistics(options));

        //Pipeline.attachWriteToElastic(options, accumulatedPanesFlowSummaries);
        Pipeline.attachWriteToCortex(options, rawFlowSummaries);

        rawFlowSummaries.apply(countVolumes());

        PipelineResult mainResult = pipeline.run();

        PipelineResult.State state = mainResult.waitUntilFinish(Duration.standardMinutes(3));

        LOG.debug("Pipeline result state: " + state);

        var metrics = mainResult.metrics();

        for (var entry : expected.entrySet()) {
            var key = entry.getKey();
            var agg = entry.getValue();
            assertTrue(TotalVolumeTest.check(metrics, true, key, agg));
            assertTrue(TotalVolumeTest.check(metrics, false, key, agg));
        }

        // check that expected metric values are stored in Cortex

        RestAssured.port = cortexPort();

        // for each result that was calculated in memory:
        for (var entry : expected.entrySet()) {
            var key = entry.getKey();
            var agg = entry.getValue();

            // query for the corresponding metric and sum over pane and direction

            var qry = new StringBuilder()
                    .append(key.key.type.name())
                    .append('{');

            var data = key.key.data;

            qry.append("nodeId=\"").append(data.nodeId).append('"');

            for (var p: key.key.type.getParts()) {
                if (p == RefType.EXPORTER_PART) {
                } else if (p == RefType.INTERFACE_PART) {
                    qry.append(",ifIndex=\"").append(data.ifIndex).append('"');
                } else if (p == RefType.DSCP_PART) {
                    qry.append(",dscp=\"").append(data.dscp).append('"');
                } else if (p == RefType.APPLICATION_PART) {
                    qry.append(",application=\"").append(data.application).append('"');
                } else if (p == RefType.HOST_PART) {
                    qry.append(",host=\"").append(data.address).append('"');
                } else if (p == RefType.CONVERSATION_PART) {
                    qry.append(",location=\"").append(data.location).append('"');
                    qry.append(",protocol=\"").append(data.protocol).append('"');
                    qry.append(",host=\"").append(data.address).append('"');
                    qry.append(",host2=\"").append(data.largerAddress).append('"');
                    qry.append(",application=\"").append(data.application).append('"');
                } else {
                    throw new RuntimeException("unexpected part: " + p);
                }
            }

            qry.append("}");

            // query the instance vector at the given time and print the response
            // -> may help in case that the following assertion on the sum fails
            var response0 = with()
                    .param("query", qry.toString())
                    .param("time", Instant.ofEpochMilli(key.eventTimestamp).toString())
                    .get("/prometheus/api/v1/query");

            response0.body().prettyPrint();

            LOG.debug("key: {}; eventTimestamp: {}; query: {}; bytes: {}",
                    key.key, Instant.ofEpochMilli(key.eventTimestamp), qry, agg.getBytes());

            // make sure that in cortex.yaml "querier.lookback_delta" is smaller than the window size
            // -> otherwise older samples for panes that do not exist at the timestamp of the current entry
            //    are also considered
            var response = with()
                    .param("query", "sum(" + qry + ")")
                    .param("time", Instant.ofEpochMilli(key.eventTimestamp).toString())
                    .get("/prometheus/api/v1/query");

            response.body().prettyPrint();

            response
                    .then()
                    .assertThat()
                    .body("status", equalTo("success"))
                    .body("data.result[0].value[1]", equalTo(String.valueOf(agg.getBytes())));

        }

    }

    private static SerializableBiFunction<Long, FlowDocuments.FlowData, Instant> frozenLastSwitchedPolicy(FlowGenOptions options) {
        var minExporter = options.getMinExporter();
        var uniformLastSwitchedPolicy = FlowConfig.uniformInWindowLastSwitchedPolicy(options);
        var start = Instant.ofEpochMilli(options.getStartMs());
        return (idx, fd) -> fd.fd1.nodeId == minExporter ? start : uniformLastSwitchedPolicy.apply(idx, fd);
    }

    public static SerializableBiFunction<Long, FlowDocuments.FlowData, Instant> skewedLastSwitchedPolicy(FlowGenOptions options) {
        var minExporter = options.getMinExporter();
        var uniformLastSwitchedPolicy = FlowConfig.uniformInWindowLastSwitchedPolicy(options);
        return (idx, fd) -> {
            var ls = uniformLastSwitchedPolicy.apply(idx, fd);
            return fd.fd1.nodeId == minExporter ? ls.minus(10*60*1000) : ls;
        };
    }

}
