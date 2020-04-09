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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowGenerator implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(FlowGenerator.class);
    private final KafkaProducer<String,byte[]> producer;

    public FlowGenerator(KafkaProducer<String,byte[]> producer) {
        this.producer = Objects.requireNonNull(producer);
    }

    @Override
    public void run() {
        long flowIntervalMs = TimeUnit.SECONDS.toMillis(5);
        Date lastFlow = new Date(System.currentTimeMillis() - flowIntervalMs);

        while (true) {
            Date now = new Date();
            Date then = lastFlow;

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

            for (FlowDocument flow : flows) {
                producer.send(new ProducerRecord<>("flows", flow.toByteArray()));
            }

            lastFlow = then;
            try {
                Thread.sleep(flowIntervalMs);
            } catch (InterruptedException e) {
                LOG.info("Interrupted while sleeping... Exiting thread.");
                return;
            }
        }
    }
}
