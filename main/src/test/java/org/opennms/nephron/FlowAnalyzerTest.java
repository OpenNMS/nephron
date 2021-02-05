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

import static org.opennms.nephron.JacksonJsonCoder.TO_FLOW_SUMMARY;
import static org.opennms.nephron.Pipeline.ReadFromKafka.getTimestamp;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE_APPLICATION;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE_CONVERSATION;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE_HOST;
import static org.opennms.nephron.flowgen.FlowGenerator.GIGABYTE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;

import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
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

    private PipelineOptions flinkOptions = TestFlinkRunner
            .fromOptions(PipelineOptionsFactory.as(NephronOptions.class)).getPipelineOptions();


    @Rule
//    public TestPipeline p = TestPipeline.create();
    public TestPipeline p = TestPipeline.fromOptions(flinkOptions);

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

        Duration fixedWindowSize = Duration.standardMinutes(1);
        long start = 1546318800000L - FlowWindows.windowOffsetForNode(99, fixedWindowSize.getMillis());
        long startPlusOneMinute = start + 60000;

        // Generate some flows
        long totalIngressBytes = 5 * GIGABYTE + 10;
        long totalEgressBytes = 2 * GIGABYTE + 2;
        final FlowGenerator flowGenerator = FlowGenerator.builder()
                .withNumConversations(2)
                .withNumFlowsPerConversation(5)
                .withConversationDuration(2, TimeUnit.MINUTES)
                .withStartTime(Instant.ofEpochMilli(start))
                .withApplications("http", "https")
                .withTotalIngressBytes(totalIngressBytes)
                .withTotalEgressBytes(totalEgressBytes)
                .withApplicationTrafficWeights(0.2d, 0.8d)
                .build();

        // Build a stream from the given set of flows
        long timestampOffsetMillis = TimeUnit.MINUTES.toMillis(1);
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        for (FlowDocument flow : flowGenerator.streamFlows()) {
            flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(flow));
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(Pipeline.toWindow(fixedWindowSize, Duration.ZERO, Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", new Groupings.KeyByExporterInterface()))
                .apply(TO_FLOW_SUMMARY);

        FlowSummary summary = new FlowSummary();
        summary.setGroupedByKey("SomeFs:SomeFid-98");
        summary.setTimestamp(startPlusOneMinute);
        summary.setRangeStartMs(start);
        summary.setRangeEndMs(startPlusOneMinute);
        summary.setRanking(0);
        summary.setGroupedBy(EXPORTER_INTERFACE);
        summary.setAggregationType(AggregationType.TOTAL);
        // the flow spans two minutes, the window 1 minute -> divide by 2
        summary.setBytesIngress(totalIngressBytes / 2);
        summary.setBytesEgress(totalEgressBytes / 2);
        summary.setBytesTotal((totalIngressBytes + totalEgressBytes) / 2);
        summary.setIfIndex(98);

        ExporterNode exporterNode = new ExporterNode();
        exporterNode.setForeignSource("SomeFs");
        exporterNode.setForeignId("SomeFid");
        exporterNode.setNodeId(99);
        summary.setExporter(exporterNode);

        PAssert.that(output)
                .inWindow(new IntervalWindow(org.joda.time.Instant.ofEpochMilli(start),
                        org.joda.time.Instant.ofEpochMilli(startPlusOneMinute)))
                .containsInAnyOrder(summary);

        p.run();
    }

    @Test
    public void canHandleLateData() {
        Duration fixedWindowSize = Duration.standardMinutes(1);
        long startMs = 1500000000000L - FlowWindows.windowOffsetForNode(99, fixedWindowSize.getMillis());
        Instant start = Instant.ofEpochMilli(startMs);

        List<Long> flowTimestampOffsets =
                ImmutableList.of(
                        -3600_000L, // 1 hour ago - 100b
                        -3570_000L, // 59m30s ago - 103b
                        -3540_000L, // 59m ago - 106b
                        // ...
                        -2400_000L, // 40m ago - 109b
                        -3600_000L,  // 1 hour ago - 112b - late data
                        -24 * 3600_000L // 24 hours ago - 115b - late - should be discarded
                        );

        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        long numBytes = 100;
        org.joda.time.Instant lastWatermark = null;
        for (Long offset : flowTimestampOffsets) {
            final Instant lastSwitched = start.plusMillis(offset);
            final Instant firstSwitched = lastSwitched.minusSeconds(30);

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
                .apply(Pipeline.toWindow(fixedWindowSize, Duration.standardMinutes(1), Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", new Groupings.KeyByExporterInterface()))
                .apply(TO_FLOW_SUMMARY);

        FlowSummary summaryFromOnTimePane = new FlowSummary();
        summaryFromOnTimePane.setGroupedByKey("SomeFs:SomeFid-98");
        summaryFromOnTimePane.setTimestamp(startMs - 3600000);
        summaryFromOnTimePane.setRangeStartMs(startMs - 3660000);
        summaryFromOnTimePane.setRangeEndMs(startMs - 3600000);
        summaryFromOnTimePane.setRanking(0);
        summaryFromOnTimePane.setGroupedBy(EXPORTER_INTERFACE);
        summaryFromOnTimePane.setAggregationType(AggregationType.TOTAL);
        summaryFromOnTimePane.setBytesIngress(100L);
        summaryFromOnTimePane.setBytesEgress(0L);
        summaryFromOnTimePane.setBytesTotal(100L);
        summaryFromOnTimePane.setIfIndex(98);

        ExporterNode exporterNode = new ExporterNode();
        exporterNode.setForeignSource("SomeFs");
        exporterNode.setForeignId("SomeFid");
        exporterNode.setNodeId(99);
        summaryFromOnTimePane.setExporter(exporterNode);

        IntervalWindow windowWithLateArrival = new IntervalWindow(
                toJoda(start.minus(1, ChronoUnit.HOURS).minus(1, ChronoUnit.MINUTES)),
                toJoda(start.minus(1, ChronoUnit.HOURS)));

        PAssert.that(output)
                .inOnTimePane(windowWithLateArrival)
                .containsInAnyOrder(summaryFromOnTimePane);

        // We expect the summary in the late pane to include data from the first
        // pane with additional flows
        FlowSummary summaryFromLatePane = clone(summaryFromOnTimePane);
        summaryFromLatePane.setBytesIngress(212L);
        summaryFromLatePane.setBytesTotal(212L);

        PAssert.that(output)
                .inFinalPane(windowWithLateArrival)
                .containsInAnyOrder(summaryFromLatePane);

        p.run();
    }

    @Test
    public void canAssociateHostnames() {
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());

        Duration fixedWindowSize = Duration.standardMinutes(1);
        long start = 1500000000000L - FlowWindows.windowOffsetForNode(99, fixedWindowSize.getMillis());
        long startPlusOneMinute = start + 60000;

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                                                                                      .withExporter("SomeFs", "SomeFid", 99)
                                                                                      .withSnmpInterfaceId(98)
                                                                                      .withApplication("SomeApplication")
                                                                                      .withHostnames("first.src.example.com", "second.dst.example.com")
                                                                                      .withFlow(Instant.ofEpochMilli(start), Instant.ofEpochMilli(start + 100),
                                                                                                "10.0.0.1", 88,
                                                                                                "10.0.0.2", 99,
                                                                                                42)
                                                                                      .build().get(0)));

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                                                                                      .withExporter("SomeFs", "SomeFid", 99)
                                                                                      .withSnmpInterfaceId(98)
                                                                                      .withApplication("SomeApplication")
                                                                                      .withHostnames("second.src.example.com", null)
                                                                                      .withFlow(Instant.ofEpochMilli(start + 1000), Instant.ofEpochMilli(start + 1100),
                                                                                                "10.0.0.2", 88,
                                                                                                "10.0.0.3", 99,
                                                                                                23)
                                                                                      .build().get(0)));

        flowStreamBuilder = flowStreamBuilder.addElements(timestampedValue(new SyntheticFlowBuilder()
                                                                                      .withExporter("SomeFs", "SomeFid", 99)
                                                                                      .withSnmpInterfaceId(98)
                                                                                      .withApplication("SomeApplication")
                                                                                      .withHostnames(null, "third.dst.example.com")
                                                                                      .withFlow(Instant.ofEpochMilli(start + 2000), Instant.ofEpochMilli(start + 2100),
                                                                                                "10.0.0.1", 88,
                                                                                                "10.0.0.3", 99,
                                                                                                1337)
                                                                                      .build().get(0)));

        final TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();
        final PCollection<FlowSummary> output = p.apply(flowStream)
                                                 .apply(new Pipeline.CalculateFlowStatistics(10, fixedWindowSize, Duration.standardMinutes(1), Duration.standardMinutes(2), Duration.standardHours(2)))
                                                 .apply(TO_FLOW_SUMMARY);

        final ExporterNode exporterNode = new ExporterNode();
        exporterNode.setForeignSource("SomeFs");
        exporterNode.setForeignId("SomeFid");
        exporterNode.setNodeId(99);

        final FlowSummary[] summaries = new FlowSummary[]{
                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(0);
                    this.setGroupedBy(EXPORTER_INTERFACE);
                    this.setAggregationType(AggregationType.TOTAL);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1337L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1337L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(42L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(42L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setConversationKey("[\"\",6,\"10.0.0.1\",\"10.0.0.2\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_CONVERSATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(23L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(23L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setConversationKey("[\"\",6,\"10.0.0.2\",\"10.0.0.3\",\"SomeApplication\"]");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-SomeApplication");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_APPLICATION);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1402L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1402L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setApplication("SomeApplication");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.1");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(1);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1379L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1379L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setHostAddress("10.0.0.1");
                    this.setHostName("first.src.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.2");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(3);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(65L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(65L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setHostAddress("10.0.0.2");
                    this.setHostName("second.dst.example.com");
                }},

                new FlowSummary() {{
                    this.setGroupedByKey("SomeFs:SomeFid-98-10.0.0.3");
                    this.setTimestamp(startPlusOneMinute);
                    this.setRangeStartMs(start);
                    this.setRangeEndMs(startPlusOneMinute);
                    this.setRanking(2);
                    this.setGroupedBy(EXPORTER_INTERFACE_HOST);
                    this.setAggregationType(AggregationType.TOPK);
                    this.setBytesIngress(1360L);
                    this.setBytesEgress(0L);
                    this.setBytesTotal(1360L);
                    this.setIfIndex(98);
                    this.setExporter(exporterNode);
                    this.setHostAddress("10.0.0.3");
                    this.setHostName("third.dst.example.com");
                }}
        };

//        PAssert.that(output).satisfies(iter -> {
//            for (Object fs: iter) {
//                System.out.println("res-fs: " + fs);
//            }
//            return null;
//        });

        PAssert.that(output)
               .containsInAnyOrder(summaries);

        p.run();
    }

    @Test
    public void testAttachedTimestamps() throws Exception {
        final int NODE_ID = 99;

        final Duration fixedWindowSize = Duration.standardSeconds(10);
        long fixedWindowSizeMillis = fixedWindowSize.getMillis();
        long startMs = 1500000000000L - FlowWindows.windowOffsetForNode(NODE_ID, fixedWindowSizeMillis);
        Instant start = Instant.ofEpochMilli(startMs);

        final LongFunction<IntervalWindow> window = (n) -> new IntervalWindow(
                org.joda.time.Instant.ofEpochMilli(startMs + fixedWindowSizeMillis * n),
                org.joda.time.Instant.ofEpochMilli(startMs + fixedWindowSizeMillis * (n + 1))
        );

        final Window<FlowDocument> windowed = Pipeline.toWindow(fixedWindowSize, Duration.standardMinutes(1), Duration.standardMinutes(5), Duration.standardMinutes(5));

        // Does not align with window
        final FlowDocument flow1 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow1", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(start.plusSeconds(17), start.plusSeconds(32).minusMillis(1),
                          "10.0.0.1", 88,
                          "10.0.0.2", 99,
                          (32 - 17) * 100)
                .build()
                .get(0);
        final Groupings.ExporterInterfaceKey key1 = Groupings.ExporterInterfaceKey.from(flow1);

        // Start aligns with window
        final FlowDocument flow2 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow2", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(start.plusSeconds(10), start.plusSeconds(32).minusMillis(1),
                          "10.0.0.1", 88,
                          "10.0.0.2", 99,
                          (32 - 10) * 200)
                .build()
                .get(0);
        final Groupings.ExporterInterfaceKey key2 = Groupings.ExporterInterfaceKey.from(flow2);

        // Start and end aligns with window
        final FlowDocument flow3 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow3", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(start.plusSeconds(10), start.plusSeconds(40).minusMillis(1),
                          "10.0.0.1", 88,
                          "10.0.0.2", 99,
                          (40 - 10) * 300)
                .build()
                .get(0);
        final Groupings.ExporterInterfaceKey key3 = Groupings.ExporterInterfaceKey.from(flow3);

        // Does not align with window but spans one more bucket with wrong alignment
        final FlowDocument flow4 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow4", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(start.plusSeconds(12), start.plusSeconds(37).minusMillis(1),
                          "10.0.0.1", 88,
                          "10.0.0.2", 99,
                          (37 - 12) * 400)
                .build()
                .get(0);
        final Groupings.ExporterInterfaceKey key4 = Groupings.ExporterInterfaceKey.from(flow4);

        final FlowDocument flow5 = new SyntheticFlowBuilder()
                .withExporter("TestFlows", "Flow5", NODE_ID)
                .withSnmpInterfaceId(98)
                .withApplication("SomeApplication")
                .withHostnames("first.src.example.com", "second.dst.example.com")
                .withFlow(start.plusSeconds(23), start.plusSeconds(27),
                          "10.0.0.1", 88,
                          "10.0.0.2", 99,
                          (27 - 23) * 500)
                .build()
                .get(0);
        final Groupings.ExporterInterfaceKey key5 = Groupings.ExporterInterfaceKey.from(flow5);

        final TestStream<FlowDocument> flows = TestStream.create(new FlowDocumentProtobufCoder())
                                                         .addElements(timestampedValue(flow1),
                                                                 timestampedValue(flow2),
                                                                 timestampedValue(flow3),
                                                                 timestampedValue(flow4),
                                                                 timestampedValue(flow5))
                                                         .advanceWatermarkToInfinity();

        final PCollection<FlowDocument> output = p.apply(flows)
                                                  .apply(windowed);

        PAssert.that("Bucket 0", output).inWindow(window.apply(0)).containsInAnyOrder();
        PAssert.that("Bucket 1", output).inWindow(window.apply(1)).containsInAnyOrder(flow1, flow2, flow3, flow4);
        PAssert.that("Bucket 2", output).inWindow(window.apply(2)).containsInAnyOrder(flow1, flow2, flow3, flow4, flow5);
        PAssert.that("Bucket 3", output).inWindow(window.apply(3)).containsInAnyOrder(flow1, flow2, flow3, flow4);
        PAssert.that("Bucket 4", output).inWindow(window.apply(4)).containsInAnyOrder();

        final PCollection<KV<Groupings.CompoundKey, Aggregate>> aggregates = output.apply(ParDo.of(new Groupings.KeyByExporterInterface()));

        PAssert.that("Bytes 0", aggregates).inWindow(window.apply(0)).containsInAnyOrder();
        PAssert.that("Bytes 1", aggregates).inWindow(window.apply(1)).containsInAnyOrder(
                KV.of(key1, new Aggregate(300, 0, null)), // 100/s * 3s
                KV.of(key2, new Aggregate(2000, 0, null)), // 200/s * 10s
                KV.of(key3, new Aggregate(3000, 0, null)), // 300/s * 10s
                KV.of(key4, new Aggregate(3200, 0, null))); // 400/s * 8s
        PAssert.that("Bytes 2", aggregates).inWindow(window.apply(2)).containsInAnyOrder(
                KV.of(key1, new Aggregate(1000, 0, null)), // 100/s * 10s
                KV.of(key2, new Aggregate(2000, 0, null)), // 200/s * 10s
                KV.of(key3, new Aggregate(3000, 0, null)), // 300/s * 10s
                KV.of(key4, new Aggregate(4000, 0, null)), // 400/s * 10s
                KV.of(key5, new Aggregate(2000, 0, null))); // 500/s * 4s
        PAssert.that("Bytes 3", aggregates).inWindow(window.apply(3)).containsInAnyOrder(
                KV.of(key1, new Aggregate(200, 0, null)), // 100/s * 2s
                KV.of(key2, new Aggregate(400, 0, null)), // 200/s * 2s
                KV.of(key3, new Aggregate(3000, 0, null)), // 300/s * 10s
                KV.of(key4, new Aggregate(2800, 0, null))); // 400/s * 7s
        PAssert.that("Bytes 4", aggregates).inWindow(window.apply(4)).containsInAnyOrder();

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
}
