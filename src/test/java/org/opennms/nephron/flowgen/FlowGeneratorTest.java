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

package org.opennms.nephron.flowgen;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public class FlowGeneratorTest {

    @Test
    public void canGeneratorFlows() {
        FlowGenerator flowGenerator = FlowGenerator.builder()
                .withNumConversations(10)
                .withNumFlowsPerConversation(5)
                .withApplications("http", "https", "ssh")
                .withConversationDuration(2, TimeUnit.MINUTES)
                .withStartTime(Instant.ofEpochMilli(1546318800000L))
                .withTotalIngressBytes(10*FlowGenerator.MEGABYTE)
                .withTotalEgressBytes(2*FlowGenerator.MEGABYTE)
                .build();

        List<FlowDocument> allFlows = flowGenerator.allFlows();
        assertThat(allFlows, hasSize(10 * 5)); // num convos * num flows per convo

        long totalIngressBytes = allFlows.stream()
                .filter(f -> Direction.INGRESS.equals(f.getDirection()))
                .mapToLong(f -> f.getNumBytes().getValue()).sum();
        assertThat((double)totalIngressBytes, closeTo(10d*FlowGenerator.MEGABYTE, 20d));

        long totalEgressBytes = allFlows.stream()
                .filter(f -> Direction.EGRESS.equals(f.getDirection()))
                .mapToLong(f -> f.getNumBytes().getValue()).sum();
        assertThat((double)totalEgressBytes, closeTo(2*FlowGenerator.MEGABYTE, 20d));
    }
}
