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

import java.util.Date;
import java.util.List;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class FlowAnalyzerTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void setUp() {
        FlowAnalyzer.registerCoders(p);
    }

    private static final Date FLOW_START = new Date(0);
    private static final Date FLOW_END = new Date(1);
    private static final List<FlowDocument> FLOWS = new SyntheticFlowBuilder()
            .withExporter("SomeFs", "SomeFid", 99)
            .withSnmpInterfaceId(98)
            // 192.168.1.100:43444 <-> 10.1.1.11:80 (110 bytes in total)
            .withApplication("http")
            .withDirection(Direction.INGRESS)
            .withFlow(FLOW_START, FLOW_END, "192.168.1.100", 43444, "10.1.1.11", 80, 10)
            .withDirection(Direction.EGRESS)
            .withFlow(FLOW_START, FLOW_END, "10.1.1.11", 80, "192.168.1.100", 43444, 100)
            // 192.168.1.100:43446 <-> 10.1.1.12:443 (500 bytes in total)
            .withApplication("https")
            .withDirection(Direction.INGRESS)
            .withFlow(FLOW_START, FLOW_END, "192.168.1.100", 43446, "10.1.1.12", 443, 25)
            .withDirection(Direction.EGRESS)
            .withFlow(FLOW_START, FLOW_END, "10.1.1.12", 443, "192.168.1.100", 43446, 500)
            .build();

    @Test
    public void canCalculateTopKApplications() {
        // Given this set of flows
        PCollection<FlowDocument> input = p.apply(Create.of(FLOWS));
        NephronOptions options = PipelineOptionsFactory.as(NephronOptions.class);
        options.setTopK(1);
        PCollection<TopKFlows> output = FlowAnalyzer.doTopKFlows(input, options);

        // We expect the top application to be "http" with in=10 & out=100
        TopKFlows topKFlows = new TopKFlows();
        topKFlows.setWindowMinMs(-1);
        topKFlows.setWindowMaxMs(9999);
        topKFlows.setContext("apps");
        topKFlows.setFlows(ImmutableMap.<String, FlowBytes>builder()
                .put("https", new FlowBytes(25,500))
                .put("http", new FlowBytes(10,100))
            .build());
        PAssert.that(output).containsInAnyOrder(topKFlows);
        p.run();
    }

}
