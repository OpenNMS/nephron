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

import static org.opennms.nephron.FlowGenerator.GIGABYTE;
import static org.opennms.nephron.elastic.GroupedBy.EXPORTER_INTERFACE;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
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
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public class FlowAnalyzerTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        Pipeline.registerCoders(p);
    }

    private static final List<FlowDocument> FLOWS = FlowGenerator.builder()
            .withNumConversations(2)
            .withNumFlowsPerConversation(5)
            .withConversationDuration(2, TimeUnit.MINUTES)
            .withStartTime(Instant.ofEpochMilli(1546318800000L))
            .withApplications("http", "https")
            .withTotalIngressBytes(5*GIGABYTE)
            .withTotalEgressBytes(2*GIGABYTE)
            .withApplicationTrafficWeights(0.2d, 0.8d)
            .allFlows();

    @Test
    public void canComputeTotalBytesInWindow() {
        // Build a stream from the given set of flows
        long timestampOffsetMillis = TimeUnit.MINUTES.toMillis(1);
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowDocumentProtobufCoder());
        for (FlowDocument flow : FLOWS) {
            flowStreamBuilder = flowStreamBuilder.addElements(TimestampedValue.of(flow,
                    org.joda.time.Instant.ofEpochMilli(flow.getLastSwitched().getValue() + timestampOffsetMillis)));
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        PCollection<FlowSummary> output = p.apply(flowStream)
                .apply(new Pipeline.WindowedFlows(Duration.standardMinutes(1)))
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
    public void flowWindowingTest() {
        FlowDocument flowDocument = FlowDocument.newBuilder()
                .build();
        PCollection<FlowDocument> output = p.apply(Create.of(flowDocument))
                .apply(Pipeline.attachTimestamps())
                .apply(Pipeline.toWindow(Duration.standardMinutes(1)));
        PAssert.that(output)
                .containsInAnyOrder(flowDocument);

        p.run();
    }

}
