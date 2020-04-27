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

import static org.opennms.nephron.Pipeline.ReadFromKafka.getTimestamp;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE;
import static org.opennms.nephron.flowgen.FlowGenerator.GIGABYTE;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
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

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        Pipeline.registerCoders(p);
    }

    @Test
    public void canComputeTotalBytesInWindow() {
        // Generate some flows
        final FlowGenerator flowGenerator = FlowGenerator.builder()
                .withNumConversations(2)
                .withNumFlowsPerConversation(5)
                .withConversationDuration(2, TimeUnit.MINUTES)
                .withStartTime(Instant.ofEpochMilli(1546318800000L))
                .withApplications("http", "https")
                .withTotalIngressBytes(5*GIGABYTE)
                .withTotalEgressBytes(2*GIGABYTE)
                .withApplicationTrafficWeights(0.2d, 0.8d)
                .build();

        // Build a stream from the given set of flows
        long timestampOffsetMillis = TimeUnit.MINUTES.toMillis(1);
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        for (FlowDocument flow : flowGenerator.streamFlows()) {
            flowStreamBuilder = flowStreamBuilder.addElements(TimestampedValue.of(flow,
                    org.joda.time.Instant.ofEpochMilli(flow.getLastSwitched().getValue() + timestampOffsetMillis)));
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(new Pipeline.WindowedFlows(Duration.standardMinutes(1), Duration.standardMinutes(15), Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", new Groupings.KeyByExporterInterface()));

        FlowSummary summary = new FlowSummary();
        summary.setGroupedByKey("SomeFs:SomeFid-98");
        summary.setTimestamp(1546318860000L);
        summary.setRangeStartMs(1546318800000L);
        summary.setRangeEndMs(1546318860000L);
        summary.setRanking(0);
        summary.setGroupedBy(EXPORTER_INTERFACE);
        summary.setAggregationType(AggregationType.TOTAL);
        summary.setBytesIngress(5368709118L);
        summary.setBytesEgress(2147483646L);
        summary.setBytesTotal(7516192764L);
        summary.setIfIndex(98);

        ExporterNode exporterNode = new ExporterNode();
        exporterNode.setForeignSource("SomeFs");
        exporterNode.setForeignId("SomeFid");
        summary.setExporter(exporterNode);

        PAssert.that(output)
                .inWindow(new IntervalWindow(org.joda.time.Instant.ofEpochMilli(1546318800000L),
                        org.joda.time.Instant.ofEpochMilli(1546318800000L + TimeUnit.MINUTES.toMillis(1))))
                .containsInAnyOrder(summary);

        p.run();
    }

    @Test
    public void canHandleLateData() throws JsonProcessingException {
        Instant start = Instant.ofEpochMilli(1500000000000L);
        List<Long> flowTimestampOffsets =
                ImmutableList.of(
                        -3600_000L, // 1 hour ago
                        -3570_000L, // 59m30s ago
                        -3540_000L, // 59m ago
                        // ...
                        -2400_000L, // 40m ago
                        -3600_000L  // 1 hour ago - late data
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
                    .withFlow(firstSwitched, lastSwitched,
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            numBytes)
                    .build().get(0);

            final org.joda.time.Instant flowTimestamp = getTimestamp(flow);
            flowStreamBuilder = flowStreamBuilder.addElements(TimestampedValue.of(flow, getTimestamp(flow)));

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
                .apply(new Pipeline.WindowedFlows(Duration.standardMinutes(1), Duration.standardMinutes(15), Duration.standardMinutes(2), Duration.standardHours(2)))
                .apply(new Pipeline.CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", new Groupings.KeyByExporterInterface()));

        FlowSummary summaryFromOnTimePane = new FlowSummary();
        summaryFromOnTimePane.setGroupedByKey("SomeFs:SomeFid-98");
        summaryFromOnTimePane.setTimestamp(1499996400000L);
        summaryFromOnTimePane.setRangeStartMs(1499996340000L);
        summaryFromOnTimePane.setRangeEndMs(1499996400000L);
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
