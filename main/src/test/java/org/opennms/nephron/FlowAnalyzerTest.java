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

import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_APPLICATION;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_CONVERSATION;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_HOST;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_TOS;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_TOS_APPLICATION;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_TOS_CONVERSATION;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST;
import static org.joda.time.Instant.ofEpochMilli;
import static org.opennms.nephron.JacksonJsonCoder.TO_FLOW_SUMMARY;
import static org.opennms.nephron.Pipeline.ReadFromKafka.getTimestamp;
import static org.opennms.nephron.flowgen.FlowGenerator.GIGABYTE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.function.LongFunction;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.nephron.elastic.AggregationType;
import org.opennms.nephron.elastic.ExporterNode;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.flowgen.FlowGenerator;
import org.opennms.nephron.flowgen.SyntheticFlowBuilder;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class FlowAnalyzerTest {

    static final ExporterNode EXPORTER_NODE = new ExporterNode();

    static {
        EXPORTER_NODE.setForeignSource("SomeFs");
        EXPORTER_NODE.setForeignId("SomeFid");
        EXPORTER_NODE.setNodeId(99);
    }

    // calculate the (unaligned) windows used in tests
    static WindowingDef WINDOW_DEF = new WindowingDef(Duration.standardMinutes(1), EXPORTER_NODE.getNodeId());
    static Wnd WND = WINDOW_DEF.at(1_500_000_000_000l);

    @Rule
    public TestPipeline p = TestPipeline.fromOptions(PipelineOptionsFactory.as(NephronOptions.class));

    @Before
    public void setUp() {
        Pipeline.registerCoders(p);
        final CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(FlowSummary.class, new JacksonJsonCoder<>(FlowSummary.class));
    }

    private TimestampedValue timestampedValue(FlowDocument fd) {
        return TimestampedValue.of(fd, getTimestamp(fd));
    }


    @Test
    public void canComputeTotalBytesInWindow() {
        // Generate some flows

        // carefully set the number of ingress/egress bytes
        // -> otherwise the FlowGenerator introduces some rounding error
        long totalIngressBytes = 5 * GIGABYTE + 10;
        long totalEgressBytes = 2 * GIGABYTE + 2;

        final FlowGenerator flowGenerator = FlowGenerator.builder()
                .withNumConversations(2)
                .withNumFlowsPerConversation(5)
                .withConversationDuration(2, TimeUnit.MINUTES)
                .withStartTime(Instant.ofEpochMilli(WND.startMs))
                .withApplications("http", "https")
                .withTotalIngressBytes(totalIngressBytes)
                .withTotalEgressBytes(totalEgressBytes)
                .withApplicationTrafficWeights(0.2d, 0.8d)
                .build();

        // Build a stream from the given set of flows
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        for (FlowDocument flow : flowGenerator.streamFlows()) {
            flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(flow));
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(new Pipeline.WindowedFlows(WND.windowSize, Duration.standardMinutes(15), Duration.ZERO, Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", CompoundKeyType.EXPORTER_INTERFACE))
                .apply(TO_FLOW_SUMMARY);

        FlowSummary summary = new FlowSummary();
        summary.setGroupedByKey("SomeFs:SomeFid-98");
        summary.setTimestamp(WND.startPlusWindowSizeMs);
        summary.setRangeStartMs(WND.startMs);
        summary.setRangeEndMs(WND.startPlusWindowSizeMs);
        summary.setRanking(0);
        summary.setGroupedBy(EXPORTER_INTERFACE);
        summary.setAggregationType(AggregationType.TOTAL);
        // the flow spans two minutes, the window 1 minute -> divide by 2
        summary.setBytesIngress(totalIngressBytes / 2);
        summary.setBytesEgress(totalEgressBytes / 2);
        summary.setBytesTotal((totalIngressBytes + totalEgressBytes) / 2);
        summary.setCongestionEncountered(false);
        summary.setNonEcnCapableTransport(true);

        summary.setIfIndex(98);

        summary.setExporter(EXPORTER_NODE);

        PAssert.that(output)
                .inWindow(new IntervalWindow(ofEpochMilli(WND.startMs), ofEpochMilli(WND.startPlusWindowSizeMs)))
                .containsInAnyOrder(summary);

        p.run();
    }

    @Test
    public void canHandleLateData() {
        Duration fixedWindowSize = Duration.standardMinutes(1);
        // choose a `start` that is the beginning of an unaligned window
        long startMs = UnalignedFixedWindows.windowStartForTimestamp(99, fixedWindowSize.getMillis(), 1500000000000L);

        Instant start = Instant.ofEpochMilli(startMs);
        List<Long> flowTimestampOffsets =
                ImmutableList.of(
                        0L, // 100b
                        30_000L, // 30s - 103b
                        60_000L, // 1m - 106b
                        // ...
                        1200_000L, // 20m - 109b
                        0L,  // late data - 112b
                        -24 * 3600_000L // 24 hours ago - late - should be discarded
                );

        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        long numBytes = 100;
        org.joda.time.Instant lastWatermark = null;
        for (Long offset : flowTimestampOffsets) {
            final Instant firstSwitched = start.plusMillis(offset);
            final Instant lastSwitched = firstSwitched.plusSeconds(30);;

            final FlowDocument flow = new SyntheticFlowBuilder()
                    .withExporter("SomeFs", "SomeFid", 99)
                    .withSnmpInterfaceId(98)
                    .withApplication("SomeApplication")
                    .withFlow(firstSwitched, lastSwitched.minusMillis(1),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            numBytes)
                    .build().get(0);

            final org.joda.time.Instant flowTimestamp = getTimestamp(flow);
            flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(flow));

            // Advance the watermark to the max timestamp
            if (lastWatermark == null || flowTimestamp.isAfter(lastWatermark)) {
                flowStreamBuilder = flowStreamBuilder.advanceWatermarkTo(flowTimestamp);
                lastWatermark = flowTimestamp;
            }

            // Add some bytes to each flow to help make then *unique*
            numBytes += 3;
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(new Pipeline.WindowedFlows(fixedWindowSize, Duration.standardMinutes(15), Duration.standardMinutes(1), Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", CompoundKeyType.EXPORTER_INTERFACE))
                .apply(TO_FLOW_SUMMARY);

        FlowSummary summaryFromOnTimePane = new FlowSummary();
        summaryFromOnTimePane.setGroupedByKey("SomeFs:SomeFid-98");
        summaryFromOnTimePane.setTimestamp(startMs + 60_000L);
        summaryFromOnTimePane.setRangeStartMs(startMs);
        summaryFromOnTimePane.setRangeEndMs(startMs + 60_000L);
        summaryFromOnTimePane.setRanking(0);
        summaryFromOnTimePane.setGroupedBy(EXPORTER_INTERFACE);
        summaryFromOnTimePane.setAggregationType(AggregationType.TOTAL);
        summaryFromOnTimePane.setBytesIngress(203L);
        summaryFromOnTimePane.setBytesEgress(0L);
        summaryFromOnTimePane.setBytesTotal(203L);
        summaryFromOnTimePane.setCongestionEncountered(false);
        summaryFromOnTimePane.setNonEcnCapableTransport(true);

        summaryFromOnTimePane.setIfIndex(98);

        summaryFromOnTimePane.setExporter(EXPORTER_NODE);

        IntervalWindow windowWithLateArrival = new IntervalWindow(
                toJoda(start),
                toJoda(start.plus(1, ChronoUnit.MINUTES)));

        PAssert.that(output)
                .inOnTimePane(windowWithLateArrival)
                .containsInAnyOrder(summaryFromOnTimePane);

        // We expect the summary in the late pane to include data from the first
        // pane with additional flows
        FlowSummary summaryFromLatePane = clone(summaryFromOnTimePane);
        summaryFromLatePane.setBytesIngress(315L);
        summaryFromLatePane.setBytesTotal(315L);

        PAssert.that(output)
                .inFinalPane(windowWithLateArrival)
                .containsInAnyOrder(summaryFromLatePane);

        p.run();
    }

    @Test
    public void canAssociateHostnames() {
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                .withExporter("SomeFs", "SomeFid", 99)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(Instant.ofEpochMilli(WND.startMs), Instant.ofEpochMilli(WND.startMs + 100),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        42)
                .build().get(0)));

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                .withExporter("SomeFs", "SomeFid", 99)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("second.src.example.com", null)
                .withFlow(Instant.ofEpochMilli(WND.startMs + 1000), Instant.ofEpochMilli(WND.startMs + 1100),
                        "10.0.0.2", 88,
                        "10.0.0.3", 99,
                        23)
                .build().get(0)));

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                .withExporter("SomeFs", "SomeFid", 99)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames(null, "third.dst.example.com")
                .withFlow(Instant.ofEpochMilli(WND.startMs + 2000), Instant.ofEpochMilli(WND.startMs + 2100),
                        "10.0.0.1", 88,
                        "10.0.0.3", 99,
                        1337)
                .build().get(0)));

        final TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();
        final PCollection<FlowSummary> output = p.apply(flowStream)
                // disable early firings
                // -> early panes prevent on-time panes if no new data arrives
                // -> early panes seem to be somewhat indeterministic: aggregation is distributed over different nodes;
                //    all of them seem to trigger (partial) early panes;
                .apply(new Pipeline.CalculateFlowStatistics(10, WND.windowSize, Duration.standardMinutes(15), Duration.ZERO, Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(TO_FLOW_SUMMARY);

        final FlowSummary[] summaries = new FlowSummary[]{
                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(0);
                    this.setGroupedBy(EXPORTER_INTERFACE);
                    this.setAggregationType(AggregationType.TOTAL);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1337L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1337L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(42L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(42L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(23L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(23L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-SomeApplication");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_APPLICATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setApplication("SomeApplication");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.1");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1379L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1379L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.1");
                    this.setHostName("first.src.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.2");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(65L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(65L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.2");
                    this.setHostName("second.dst.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.3");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1360L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1360L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.3");
                    this.setHostName("third.dst.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(0);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS);
                    this.setAggregationType(AggregationType.TOTAL);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1337L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1337L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(42L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(42L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(23L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(23L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setConversationKey("[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-SomeApplication");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_APPLICATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setApplication("SomeApplication");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-10.0.0.1");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1379L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1379L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.1");
                    this.setHostName("first.src.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-10.0.0.2");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(65L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(65L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.2");
                    this.setHostName("second.dst.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-0-10.0.0.3");
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_TOS_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1360L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1360L);
                    this.setCongestionEncountered(false);
                    this.setNonEcnCapableTransport(true);
                    this.setDscp(0);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setHostAddress("10.0.0.3");
                    this.setHostName("third.dst.example.com");
                }}

        };

        PAssert.that(output)
               .containsInAnyOrder(summaries);

        p.run();
    }

    @Test
    public void testAttachedTimestamps() throws Exception {
        final int NODE_ID = 99;
        WindowingDef winDef = new WindowingDef(Duration.standardSeconds(10), NODE_ID);
        Wnd wnd = winDef.at(1_500_000_000_000l);

        final LongFunction<IntervalWindow> window = (n) -> new IntervalWindow(
                ofEpochMilli(wnd.startMs + wnd.windowSize.getMillis() * n),
                ofEpochMilli(wnd.startMs + wnd.windowSize.getMillis() * (n + 1))
        );

        final PTransform<PCollection<FlowDocument>, PCollection<FlowDocument>> windowed =
                new Pipeline.WindowedFlows(wnd.windowSize, Duration.standardMinutes(15), Duration.ZERO, Duration.standardMinutes(5), Duration.standardMinutes(5));

        // Does not align with window
        final FlowDocument flow1 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow1", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(wnd.start.plusSeconds(17), wnd.start.plusSeconds(32).minusMillis(1),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        (32 - 17) * 100)
                .build()
                .get(0);
        final CompoundKey key1 = EXPORTER_INTERFACE.create(flow1).get(0).value;

        // Start aligns with window
        final FlowDocument flow2 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow2", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(wnd.start.plusSeconds(10), wnd.start.plusSeconds(32).minusMillis(1),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        (32 - 10) * 200)
                .build()
                .get(0);
        final CompoundKey key2 = CompoundKeyType.EXPORTER_INTERFACE.create(flow2).get(0).value;

        // Start and end aligns with window
        final FlowDocument flow3 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow3", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(wnd.start.plusSeconds(10), wnd.start.plusSeconds(40).minusMillis(1),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        (40 - 10) * 300)
                .build()
                .get(0);
        final CompoundKey key3 = CompoundKeyType.EXPORTER_INTERFACE.create(flow3).get(0).value;

        // Does not align with window but spans one more bucket with wrong alignment
        final FlowDocument flow4 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow4", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(wnd.start.plusSeconds(12), wnd.start.plusSeconds(37).minusMillis(1),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        (37 - 12) * 400)
                .build()
                .get(0);
        final CompoundKey key4 = CompoundKeyType.EXPORTER_INTERFACE.create(flow4).get(0).value;

        final FlowDocument flow5 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow5", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(wnd.start.plusSeconds(23), wnd.start.plusSeconds(27).minusMillis(1),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        (27 - 23) * 500)
                .build()
                .get(0);
        final CompoundKey key5 = CompoundKeyType.EXPORTER_INTERFACE.create(flow5).get(0).value;

        final TestStream<FlowDocument> flows = TestStream.create(new FlowDocumentProtobufCoder())
                .addElements(
                        timestampedValue(flow1),
                        timestampedValue(flow2),
                        timestampedValue(flow3),
                        timestampedValue(flow4),
                        timestampedValue(flow5)
                )
                .advanceWatermarkToInfinity();

        final PCollection<FlowDocument> output = p.apply(flows)
                                                  .apply(windowed);

        PAssert.that("Bucket 0", output).inWindow(window.apply(0)).containsInAnyOrder();
        PAssert.that("Bucket 1", output).inWindow(window.apply(1)).containsInAnyOrder(flow1, flow2, flow3, flow4);
        PAssert.that("Bucket 2", output).inWindow(window.apply(2)).containsInAnyOrder(flow1, flow2, flow3, flow4, flow5);
        PAssert.that("Bucket 3", output).inWindow(window.apply(3)).containsInAnyOrder(flow1, flow2, flow3, flow4);
        PAssert.that("Bucket 4", output).inWindow(window.apply(4)).containsInAnyOrder();

        final PCollection<KV<CompoundKey, Aggregate>> aggregates = output.apply(ParDo.of(new Pipeline.KeyFlowBy(EXPORTER_INTERFACE)));

        PAssert.that("Bytes 0", aggregates).inWindow(window.apply(0)).containsInAnyOrder();
        PAssert.that("Bytes 1", aggregates).inWindow(window.apply(1)).containsInAnyOrder(
                KV.of(key1, new Aggregate(300, 0, null, 0)), // 100/s * 3s
                KV.of(key2, new Aggregate(2000, 0, null, 0)), // 200/s * 10s
                KV.of(key3, new Aggregate(3000, 0, null, 0)), // 300/s * 10s
                KV.of(key4, new Aggregate(3200, 0, null, 0))); // 400/s * 8s
        PAssert.that("Bytes 2", aggregates).inWindow(window.apply(2)).containsInAnyOrder(
                KV.of(key1, new Aggregate(1000, 0, null, 0)), // 100/s * 10s
                KV.of(key2, new Aggregate(2000, 0, null, 0)), // 200/s * 10s
                KV.of(key3, new Aggregate(3000, 0, null, 0)), // 300/s * 10s
                KV.of(key4, new Aggregate(4000, 0, null, 0)), // 400/s * 10s
                KV.of(key5, new Aggregate(2000, 0, null, 0))); // 500/s * 4s
        PAssert.that("Bytes 3", aggregates).inWindow(window.apply(3)).containsInAnyOrder(
                KV.of(key1, new Aggregate(200, 0, null, 0)), // 100/s * 2s
                KV.of(key2, new Aggregate(400, 0, null, 0)), // 200/s * 2s
                KV.of(key3, new Aggregate(3000, 0, null, 0)), // 300/s * 10s
                KV.of(key4, new Aggregate(2800, 0, null, 0))); // 400/s * 7s
        PAssert.that("Bytes 4", aggregates).inWindow(window.apply(4)).containsInAnyOrder();

        p.run();
    }

    private static TestStream<FlowDocument> testStream(int tos1, int tos2) {
        TestStream.Builder<FlowDocument> flowStreamBuilder = new SyntheticFlowBuilder()
                .withExporter(EXPORTER_NODE.getForeignSource(), EXPORTER_NODE.getForeignId(), EXPORTER_NODE.getNodeId())
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withTos(tos1)
                .withFlow(Instant.ofEpochMilli(1500000000000L), Instant.ofEpochMilli(1500000000100L),
                        "10.0.0.1", 66,
                        "10.0.0.2", 77,
                        11)
                .withTos(tos2) // ~ DSCP 3
                .withFlow(Instant.ofEpochMilli(1500000000100L), Instant.ofEpochMilli(1500000000150L),
                        "10.0.0.1", 88,
                        "10.0.0.2", 99,
                        17)

                .build()
                .stream()
                .map(fd -> TimestampedValue.of(fd, org.joda.time.Instant.ofEpochMilli(1500000000000L)))
                .reduce(
                        TestStream.create(new FlowDocumentProtobufCoder()),
                        (builder, tv) -> builder.addElements(tv),
                        (b1, b2) -> { throw new RuntimeException("stream not parallel -> no combiner needed"); }
                );

        return flowStreamBuilder.advanceWatermarkToInfinity();
    }

    static class Expected {
        public final CompoundKeyType groupedBy;
        public final int ranking;
        public final String groupedByKey;
        public final AggregationType aggregationType;
        public final int bytes;
        public final boolean congestionEncountered;
        public final Integer dscp;
        public final String application;
        public final String hostAddress;
        public final String conversationKey;

        public Expected(CompoundKeyType groupedBy, int ranking, String groupedByKey, AggregationType aggregationType, int bytes, boolean congestionEncountered, Integer dscp, String application, String hostAddress, String conversationKey) {
            this.groupedBy = groupedBy;
            this.ranking = ranking;
            this.groupedByKey = groupedByKey;
            this.aggregationType = aggregationType;
            this.bytes = bytes;
            this.congestionEncountered = congestionEncountered;
            this.dscp = dscp;
            this.application = application;
            this.hostAddress = hostAddress;
            this.conversationKey = conversationKey;
        }
    }

    @Test
    public void groupsByDscp() {
        final TestStream<FlowDocument> flowStream = testStream(0, 12);
        final PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(new Pipeline.CalculateFlowStatistics(10, Duration.standardMinutes(1), Duration.standardMinutes(15), Duration.ZERO, Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(TO_FLOW_SUMMARY);

        // expect 15 flow summaries:
        // 1 for exporter/interface
        // 1 for exporter/interface/application ("SomeApplication")
        // 1 for exporter/interface/conversation (10.0.0.1 <-> 10.0.0.2)
        // 2 for exporter/interface/host (10.0.0.1, 10.0.0.2)
        // 2 for exporter/interface/dscp (0, 3)
        // 2 * 1 for exporter/interface/dscp/application ("SomeApplication")
        // 2 * 1 for exporter/interface/dscp/conversation (10.0.0.1 <-> 10.0.0.2)
        // 2 * 2 for exporter/interface/dscp/host (10.0.0.1, 10.0.0.2)


        String conversationKey = "[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]";

        FlowSummary[] summaries = Arrays.stream(
                new Expected[]{
                        new Expected(EXPORTER_INTERFACE, 0, "SomeFs:SomeFid-98", AggregationType.TOTAL, 28, false, null, null, null, null),
                        new Expected(EXPORTER_INTERFACE_APPLICATION, 1, "SomeFs:SomeFid-98-SomeApplication", AggregationType.TOPK, 28, false, null, "SomeApplication", null, null),
                        new Expected(EXPORTER_INTERFACE_CONVERSATION, 1, "SomeFs:SomeFid-98-" + conversationKey, AggregationType.TOPK, 28, false, null, null, null, conversationKey),
                        new Expected(EXPORTER_INTERFACE_HOST, 1, "SomeFs:SomeFid-98-10.0.0.1", AggregationType.TOPK, 28, false, null, null, "10.0.0.1", null),
                        new Expected(EXPORTER_INTERFACE_HOST, 2, "SomeFs:SomeFid-98-10.0.0.2", AggregationType.TOPK, 28, false, null, null, "10.0.0.2", null),
                        new Expected(EXPORTER_INTERFACE_TOS, 0, "SomeFs:SomeFid-98-0", AggregationType.TOTAL, 11, false, 0, null, null, null),
                        new Expected(EXPORTER_INTERFACE_TOS, 0, "SomeFs:SomeFid-98-3", AggregationType.TOTAL, 17, false, 3, null, null, null),
                        new Expected(EXPORTER_INTERFACE_TOS_APPLICATION, 1, "SomeFs:SomeFid-98-0-SomeApplication", AggregationType.TOPK, 11, false, 0, "SomeApplication", null, null),
                        new Expected(EXPORTER_INTERFACE_TOS_APPLICATION, 1, "SomeFs:SomeFid-98-3-SomeApplication", AggregationType.TOPK, 17, false, 3, "SomeApplication", null, null),
                        new Expected(EXPORTER_INTERFACE_TOS_CONVERSATION, 1, "SomeFs:SomeFid-98-0-" + conversationKey, AggregationType.TOPK, 11, false, 0, null, null, conversationKey),
                        new Expected(EXPORTER_INTERFACE_TOS_CONVERSATION, 1, "SomeFs:SomeFid-98-3-" + conversationKey, AggregationType.TOPK, 17, false, 3, null, null, conversationKey),
                        new Expected(EXPORTER_INTERFACE_TOS_HOST, 1, "SomeFs:SomeFid-98-0-10.0.0.1", AggregationType.TOPK, 11, false, 0, null, "10.0.0.1", null),
                        new Expected(EXPORTER_INTERFACE_TOS_HOST, 2, "SomeFs:SomeFid-98-0-10.0.0.2", AggregationType.TOPK, 11, false, 0, null, "10.0.0.2", null),
                        new Expected(EXPORTER_INTERFACE_TOS_HOST, 1, "SomeFs:SomeFid-98-3-10.0.0.1", AggregationType.TOPK, 17, false, 3, null, "10.0.0.1", null),
                        new Expected(EXPORTER_INTERFACE_TOS_HOST, 2, "SomeFs:SomeFid-98-3-10.0.0.2", AggregationType.TOPK, 17, false, 3, null, "10.0.0.2", null),
                        }
                ).map(e -> new FlowSummary() {{
                    this.setGroupedByKey(e.groupedByKey);
                    this.setTimestamp(WND.startPlusWindowSizeMs);
                    this.setRangeStartMs(WND.startMs);
                    this.setRangeEndMs(WND.startPlusWindowSizeMs);
                    this.setRanking(e.ranking);
                    this.setGroupedBy(e.groupedBy);
                    this.setAggregationType(e.aggregationType);
                    this.setBytesIngress((long)e.bytes);
                    this.setBytesEgress(0L);
                    this.setBytesTotal((long)e.bytes);
                    this.setCongestionEncountered(e.congestionEncountered);
                    this.setNonEcnCapableTransport(true);
                    this.setIfIndex(98);
                    this.setExporter(EXPORTER_NODE);
                    this.setDscp(e.dscp);
                    this.setApplication(e.application);
                    this.setHostAddress(e.hostAddress);
                    this.setConversationKey(e.conversationKey);
        }}).toArray(l -> new FlowSummary[l]);

        PAssert.that(output).containsInAnyOrder(summaries);

        p.run();

    }

    private static org.joda.time.Instant toJoda(Instant instant) {
        return org.joda.time.Instant.ofEpochMilli(instant.toEpochMilli());
    }

    private static FlowSummary clone(FlowSummary summary) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(mapper.writeValueAsString(summary), FlowSummary.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Contains the information that is required to calculate unaligned windows for the given node id.
     */
    public static class WindowingDef {

        public final Duration windowSize;
        public final int nodeId;

        public WindowingDef(Duration windowSize, int nodeId) {
            this.windowSize = windowSize;
            this.nodeId = nodeId;
        }

        /**
         * Returns information about the window the given timestamp falss into.
         */
        public Wnd at(long timestamp) {
            long windowSizeMs = windowSize.getMillis();
            long windowNumber = UnalignedFixedWindows.windowNumber(nodeId, windowSizeMs, timestamp);
            long start = UnalignedFixedWindows.windowStartForWindowNumber(nodeId, windowSizeMs, windowNumber);
            return new Wnd(windowSize, nodeId, windowNumber, start);

        }

    }

    public static class Wnd {

        public final Duration windowSize;
        public final int nodeId;
        public final long windowNumber;
        public final long startMs;
        public final Instant start;
        public final long startPlusWindowSizeMs;

        public Wnd(Duration windowSize, int nodeId, long windowNumber, long startMs) {
            this.windowSize = windowSize;
            this.nodeId = nodeId;
            this.windowNumber = windowNumber;
            this.startMs = startMs;
            this.start = Instant.ofEpochMilli(startMs);
            this.startPlusWindowSizeMs = startMs + windowSize.getMillis();
        }

    }

}
