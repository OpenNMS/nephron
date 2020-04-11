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

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public class FlowAnalyzerTest {

    @Rule
    public TestPipeline p = TestPipeline.create();


    @Test
    public void canCalculateTopKApplications() {


    }

    @Test
    public void canTestSomething() {
        Date now = new Date(0);
        Date then = new Date(1);
        final List<FlowDocument> flows = new SyntheticFlowBuilder()
                .withExporter("SomeFs", "SomeFid", 99)
                .withSnmpInterfaceId(98)
                // 192.168.1.100:43444 <-> 10.1.1.11:80 (110 bytes in total)
                .withApplication("http")
                .withDirection(Direction.INGRESS)
                .withFlow(then, now, "192.168.1.100", 43444, "10.1.1.11", 80, 10)
                .withDirection(Direction.EGRESS)
                .withFlow(then, now, "10.1.1.11", 80, "192.168.1.100", 43444, 100)
                .build();

        PCollection<FlowDocument> input = p.apply(Create.of(flows));
        PCollection<FlowDocument> output = input.apply(FlowAnalyzer.attachTimestamps());
        PAssert.that(output).containsInAnyOrder(flows);

        p.run();
    }
}
