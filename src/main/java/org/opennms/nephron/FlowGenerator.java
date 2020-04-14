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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import avro.shaded.com.google.common.collect.Iterables;

public class FlowGenerator {
    public static final long MEGABYTE = 1024*1024;
    public static final long GIGABYTE = 1024*MEGABYTE;

    private final int numConversations;
    private final int numFlowsPerConversation;
    private final List<String> applications;
    private final long totalIngressBytes;
    private final long totalEgressBytes;
    private final List<Double> applicationTrafficWeights;
    private final long conversationDurationMillis;
    private final Instant startTime;

    public static final class Builder {
        private int numConversations = 10;
        private int numFlowsPerConversation = 2;
        private List<String> applications = Arrays.asList("http", "https", "opennms-amqp", "opennms-http");
        private List<Double> applicationTrafficWeights = null;
        private long conversationDurationMillis = TimeUnit.MINUTES.toMillis(1);
        private Instant startTime = Instant.ofEpochMilli(1546318800000L); // Jan 1st 2019
        private long totalIngressBytes = 10*MEGABYTE;
        private long totalEgressBytes = 2*MEGABYTE;

        public Builder withNumConversations(int numConversations) {
            if (numConversations < 1) {
                throw new IllegalArgumentException("numConversations must be strictly positive.");
            }
            this.numConversations = numConversations;
            return this;
        }

        public Builder withNumFlowsPerConversation(int numFlowsPerConversation) {
            if (numFlowsPerConversation < 1) {
                throw new IllegalArgumentException("numFlowsPerConversation must be strictly positive.");
            }
            this.numFlowsPerConversation = numFlowsPerConversation;
            return this;
        }

        public Builder withApplications(String... applications) {
            if (applications.length < 1) {
                throw new IllegalArgumentException("one or more application names are required");
            }
            this.applications = Arrays.asList(applications);
            return this;
        }

        public Builder withConversationDuration(int value, TimeUnit unit) {
            if (value < 0) {
                throw new IllegalArgumentException("value must be strictly positive");
            }
            conversationDurationMillis = unit.toMillis(value);
            return this;
        }

        public Builder withStartTime(Instant startTime) {
            this.startTime = Objects.requireNonNull(startTime);
            return this;
        }


        public Builder withTotalIngressBytes(long totalIngressBytes) {
            if (totalIngressBytes < 1) {
                throw new IllegalArgumentException("totalIngressBytes must be strictly positive");
            }
            this.totalIngressBytes = totalIngressBytes;
            return this;
        }

        public Builder withTotalEgressBytes(long totalEgressBytes) {
            if (totalEgressBytes < 1) {
                throw new IllegalArgumentException("totalEgressBytes must be strictly positive");
            }
            this.totalEgressBytes = totalEgressBytes;
            return this;
        }

        public Builder withApplicationTrafficWeights(Double... weights) {
            this.applicationTrafficWeights = Arrays.asList(weights);
            return this;
        }

        public FlowGenerator build() {
            if (applicationTrafficWeights != null && !applicationTrafficWeights.isEmpty()) {
                if (applications.size() != applicationTrafficWeights.size()) {
                    throw new IllegalArgumentException("number of weights must match number of applications");
                }
            } else {
                //
                applicationTrafficWeights = IntStream.range(0, applications.size())
                        .mapToDouble(i -> 1 / (double)applications.size())
                        .boxed()
                        .collect(Collectors.toList());
            }
            return new FlowGenerator(this);
        }

        public List<FlowDocument> allFlows() {
            return build().allFlows();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private FlowGenerator(Builder builder) {
        this.numConversations = builder.numConversations;
        this.numFlowsPerConversation = builder.numFlowsPerConversation;
        this.applications = builder.applications;
        this.conversationDurationMillis = builder.conversationDurationMillis;
        this.startTime = builder.startTime;
        this.totalIngressBytes = builder.totalIngressBytes;
        this.totalEgressBytes = builder.totalEgressBytes;

        if (builder.applicationTrafficWeights != null) {
            if (applications.size() != builder.applicationTrafficWeights.size()) {
                throw new IllegalArgumentException("number of weights must match number of applications");
            }
            this.applicationTrafficWeights = builder.applicationTrafficWeights;
        } else {
            // Distribute the traffic evenly across all apps
            applicationTrafficWeights = IntStream.range(0, applications.size())
                    .mapToDouble(i -> 1 / (double)applications.size())
                    .boxed()
                    .collect(Collectors.toList());
        }
    }

    private class FlowDocumentIterator implements Iterator<FlowDocument> {
        int conversationIndex = -1;
        int flowIndex = 0;
        String applicationForConversation;
        Instant startOfFlow = startTime;
        long flowDurationMillis = Math.floorDiv(conversationDurationMillis, (long)numFlowsPerConversation);

        long ingressBytesPerFlow;
        long egressBytesPerFlow;

        public FlowDocumentIterator() {
            startNewConversation();
        }

        @Override
        public boolean hasNext() {
            return conversationIndex < numConversations;
        }

        private void startNewConversation() {
            // Start a new conversation
            flowIndex = 0;
            conversationIndex++;

            int applicationIndex = conversationIndex % applications.size();
            applicationForConversation = applications.get(applicationIndex);

            double applicationTrafficWeightForConversation = applicationTrafficWeights.get(applicationIndex);

            // How many times are we going to visit this application?
            int numConversationsForApp = Math.floorDiv(numConversations, applications.size());
            if (applicationIndex < numConversations % applications.size()) {
                numConversationsForApp += 1;
            }

            int numIngressFlowsPerConversation = Math.floorDiv(numFlowsPerConversation, 2) + numFlowsPerConversation % 2;
            int numEgressFlowsPerConversation = Math.floorDiv(numFlowsPerConversation, 2);

            ingressBytesPerFlow = (long)Math.floor((totalIngressBytes * applicationTrafficWeightForConversation) / numConversationsForApp / numIngressFlowsPerConversation);
            egressBytesPerFlow = (long)Math.floor((totalEgressBytes * applicationTrafficWeightForConversation) / numConversationsForApp / numEgressFlowsPerConversation);
        }

        @Override
        public FlowDocument next() {
            String sourceIp = "192.168.1.100";
            int srcPort = 55555;
            String dstIp = "10.1.1.11";
            int dstPort = 80;

            final SyntheticFlowBuilder flowBuilder = new SyntheticFlowBuilder()
                    .withExporter("SomeFs", "SomeFid", 99)
                    .withSnmpInterfaceId(98)
                    .withApplication(applicationForConversation);
            if (flowIndex % 2 == 0) {
                flowBuilder.withDirection(Direction.INGRESS)
                        .withFlow(startOfFlow, startOfFlow.plusMillis(flowDurationMillis),
                                sourceIp, srcPort,
                                dstIp, dstPort,
                                ingressBytesPerFlow);
            } else {
                flowBuilder.withDirection(Direction.EGRESS)
                        .withFlow(startOfFlow, startOfFlow.plusMillis(flowDurationMillis),
                                dstIp, dstPort,
                                sourceIp, srcPort,
                                egressBytesPerFlow);
            }
            final FlowDocument flow = flowBuilder.build().get(0);

            // Prepare for next flow
            flowIndex++;
            if (flowIndex >= numFlowsPerConversation) {
               startNewConversation();
            }

            return flow;
        }
    }

    public Iterable<FlowDocument> streamFlows() {
        return FlowDocumentIterator::new;
    }

    public List<FlowDocument> allFlows() {
        final List<FlowDocument> flows = new LinkedList<>();
        Iterables.addAll(flows, streamFlows());
        return flows;
    }
}
