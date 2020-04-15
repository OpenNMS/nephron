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

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class FlowAnalyzerTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        FlowAnalyzer.registerCoders(p);
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
    public void canCalculateTopKApplications() {
        // Given this set of flows
        PCollection<FlowDocument> input = p.apply(Create.of(FLOWS));
        NephronOptions options = PipelineOptionsFactory.as(NephronOptions.class);
        options.setFixedWindowSize("30s");
        PCollection<TopKFlows> output = FlowAnalyzer.doTopKFlows(input, options);

        // We expect the top application to be "http" with in=10 & out=100
        TopKFlows topKFlows = new TopKFlows();
        topKFlows.setWindowMinMs(1546318819999L);
        topKFlows.setWindowMaxMs(1546318829999L);
        topKFlows.setContext("apps");
        topKFlows.setFlows(ImmutableMap.<String, FlowBytes>builder()
                .put("https", new FlowBytes(12884901885L,5153960754L))
                .put("http", new FlowBytes(3221225469L,1288490184L))
            .build());
        PAssert.that(output).containsInAnyOrder(topKFlows);
        p.run();
    }

}
