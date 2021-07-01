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

import static org.junit.Assert.assertTrue;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_TOS;
import static org.opennms.nephron.Pipeline.registerCoders;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.opennms.nephron.Aggregate;
import org.opennms.nephron.CompoundKey;
import org.opennms.nephron.CompoundKeyType;
import org.opennms.nephron.FlowSummaryData;
import org.opennms.nephron.MissingFieldsException;
import org.opennms.nephron.Pipeline;
import org.opennms.nephron.UnalignedFixedWindows;
import org.opennms.nephron.elastic.AggregationType;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TotalVolumeTest {

    private static Logger LOG = LoggerFactory.getLogger(TotalVolumeTest.class);

    @Test
    public void testTotalVolume() {

        // Uses a (deterministic) stream of flows and compares the total volume of traffic calculated in memory
        // and calculated by the pipeline.
        //
        // Only the "total" aggregations are checked because checking "topK" aggregations would require to replicate the
        // topK pruning logic.
        //
        // Flow summaries calculated by the pipeline are use to increment counter metrics that are named
        // by the window start and aggregation key. This allows to compare the in-memory result with the
        // result returned by the pipeline.

        FlowGenOptions options = PipelineOptionsFactory.fromArgs(
                "--runner=FlinkRunner",
//                "--flinkMaster=localhost:8081",
//                "--parallelism=2",
                "--playbackMode=true",
                "--numWindows=10",
                "--flowsPerWindow=10000",
                "--lastSwitchedSigmaMs=25000",
                "--numExporters=3",
                "--numInterfaces=3",
                "--numApplications=4",
                "--numHosts=4",
                "--numEcns=4",
                "--numDscps=6",
                "--flowsPerSecond=0"
        ).withValidation().as(FlowGenOptions.class);

        SourceConfig sourceConfig = SourceConfig.of(options, SyntheticFlowTimestampPolicyFactory.withLimitedDelay(options, Pipeline.ReadFromKafka::getTimestamp));

        Map<ResKey, Aggregate> expected = new HashMap<>();

        long windowSizeMs = options.getFixedWindowSizeMs();
        long maxFlowDurationMs = options.getMaxFlowDurationMs();

        // calculate the in-memory result

        FlowDocuments.stream(sourceConfig).forEach(flow -> {

            // logic copied from attachTimestamps
            long deltaSwitched = flow.getDeltaSwitched().getValue();
            long lastSwitched = flow.getLastSwitched().getValue();
            int nodeId = flow.getExporterNode().getNodeId();
            int itfIdx = UnalignedFixedWindows.getItfIdx(flow);

            long shift = UnalignedFixedWindows.perNodeShift(nodeId, itfIdx, windowSizeMs);
            if (deltaSwitched < shift) {
                return;
            }

            long firstWindow = UnalignedFixedWindows.windowNumber(nodeId, itfIdx, windowSizeMs, deltaSwitched); // the first window the flow falls into
            long lastWindow = UnalignedFixedWindows.windowNumber(nodeId, itfIdx, windowSizeMs, lastSwitched); // the last window the flow falls into (assuming lastSwitched is inclusive)
            long nbWindows = lastWindow - firstWindow + 1;

            long timestamp = deltaSwitched;
            for (long i = 0; i < nbWindows; i++) {
                if (timestamp > lastSwitched - maxFlowDurationMs) {

                    long windowStart = UnalignedFixedWindows.windowStartForTimestamp(
                            flow.getExporterNode().getNodeId(),
                            itfIdx,
                            options.getFixedWindowSizeMs(),
                            timestamp
                    );
                    IntervalWindow window = new IntervalWindow(Instant.ofEpochMilli(windowStart), new Duration(windowSizeMs));

                    // total aggregations are calculated for EXPORTER_INTERFACE and EXPORTER_INTERFACE_TOS only
                    for (CompoundKeyType compoundKeyType : Arrays.asList(EXPORTER_INTERFACE, EXPORTER_INTERFACE_TOS)) {
                        try {
                            CompoundKey key = compoundKeyType.create(flow);
                            ResKey resKey = new ResKey(windowStart, key);
                            Aggregate aggregate = Pipeline.aggregatize(window, flow, "", "");
                            Aggregate previous = expected.get(resKey);
                            Aggregate next = previous != null ? Aggregate.merge(previous, aggregate) : aggregate;
                            expected.put(resKey, next);
                        } catch (MissingFieldsException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }
                // ensure that the timestamp used for the last window is not larger than lastSwitched
                if (timestamp + windowSizeMs < lastSwitched) {
                    timestamp += windowSizeMs;
                } else {
                    timestamp = lastSwitched;
                }
            }

        });

        org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(p);

        PCollection<FlowSummaryData> flowSummaries = p
                .apply(SyntheticFlowSource.readFromSyntheticSource(sourceConfig))
                .apply(new Pipeline.CalculateFlowStatistics(options));

        flowSummaries.apply(countVolumes());

        PipelineResult mainResult = p.run();

        PipelineResult.State state = mainResult.waitUntilFinish(Duration.standardMinutes(3));

        LOG.debug("Pipeline result state: " + state);

        var metrics = mainResult.metrics();

        boolean check = true;

        for (Map.Entry<ResKey, Aggregate> me : expected.entrySet()) {
            var key = me.getKey();
            var agg = me.getValue();
            boolean c1 = check(metrics, true, key, agg);
            boolean c2 = check(metrics, false, key, agg);
            check &= c1 && c2;
        }

        assertTrue("unexpected results", check);

    }

    /**
     * Checks that the in-memory calculated aggregation result matches the value of the corresponding counter metric.
     */
    private static boolean check(MetricResults metricResults, boolean inNotOut, ResKey resKey, Aggregate agg) {
        String strKey = resKey.asString();
        var iter = metricResults.queryMetrics(MetricsFilter.builder()
                .addNameFilter(MetricNameFilter.named(strKey, inNotOut ? "in" : "out")).build()).getCounters().iterator();
        if (iter.hasNext()) {
            var metricResult = iter.next();
            var expected = inNotOut ? agg.getBytesIn() : agg.getBytesOut();
            if (metricResult.getAttempted() == expected) {
                return true;
            } else {
                LOG.error("mismatch - key: " + strKey + "; in: " + inNotOut + "; expected: " + expected + "; actual: " + metricResult.getCommitted());
                return false;
            }
        } else {
            LOG.error("missing metric - key: " + strKey + "; in: " + inNotOut);
            return false;
        }
    }

    /**
     * A transform the increments counters that are named by the window start and aggregation key.
     */
    public static ParDo.SingleOutput<FlowSummaryData, Void> countVolumes() {
        return ParDo.of(
                new DoFn<FlowSummaryData, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        FlowSummaryData fsd = c.element();
                        if (fsd.aggregationType == AggregationType.TOPK) return;
                        ResKey resKey = new ResKey(fsd.windowStart, fsd.key);
                        String strKey = resKey.asString();
                        Metrics.counter(strKey, "in").inc(fsd.aggregate.getBytesIn());
                        Metrics.counter(strKey, "out").inc(fsd.aggregate.getBytesOut());
                    }
                });
    }

    public static class ResKey {
        public final long windowStart;
        public final CompoundKey key;

        public ResKey(long windowStart, CompoundKey key) {
            this.windowStart = windowStart;
            this.key = key;
        }

        public String asString() {
            StringBuilder sb = new StringBuilder();
            sb.append(windowStart).append('-').append(key);
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResKey resKey = (ResKey) o;
            return windowStart == resKey.windowStart && key.equals(resKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(windowStart, key);
        }
    }

}
