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

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

public class KafkaFlowGenerator implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaFlowGenerator.class);
    private final KafkaProducer<String,byte[]> producer;
    private Consumer<FlowDocument> callback;

    public KafkaFlowGenerator(KafkaProducer<String,byte[]> producer) {
        this.producer = Objects.requireNonNull(producer);
    }

    @Override
    public void run() {
        final FlowGenerator flowGenerator = FlowGenerator.builder()
                .withNumConversations(2)
                .withNumFlowsPerConversation(5)
                .withConversationDuration(2, TimeUnit.MINUTES)
                .withStartTime(Instant.now().minus(Duration.ofHours(1)))
                .withApplications("http", "https")
                .withTotalIngressBytes(5*GIGABYTE)
                .withTotalEgressBytes(2*GIGABYTE)
                .withApplicationTrafficWeights(0.2d, 0.8d)
                .build();

        // Limit to 10 flows per second
        final RateLimiter rateLimiter = RateLimiter.create(10);
        for (FlowDocument flow : flowGenerator.streamFlows()) {
            rateLimiter.acquire(1);
            producer.send(new ProducerRecord<>(NephronOptions.DEFAULT_FLOW_SOURCE_TOPIC, flow.toByteArray()), (metadata, exception) -> {
                // Issue the callback then the send was successful
                if (callback != null && exception == null) {
                    callback.accept(flow);
                }
            });
        }
    }

    public void setCallback(Consumer<FlowDocument> callback) {
        this.callback = callback;
    }
}
