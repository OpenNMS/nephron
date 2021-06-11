/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
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

package org.opennms.nephron.generator;

import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowDocSender implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FlowDocSender.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;
    private final String flowTopic;

    public FlowDocSender(final String bootstrapServers,
                   final String flowTopic,
                   final File propertiesFile) {
        final Map<String, Object> producerProps = new HashMap<>();

        if (propertiesFile != null) {
            try {
                final Properties properties = new Properties();
                properties.load(new FileReader(propertiesFile));
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    producerProps.put(entry.getKey().toString(), entry.getValue());
                }
            } catch (IOException e) {
                LOG.error("Error reading properties file", e);
                throw new RuntimeException("Error reading properties file", e);
            }
        }

        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        this.kafkaProducer = new KafkaProducer<>(producerProps);
        this.flowTopic = flowTopic;
    }

    public void send(FlowDocument flowDocument) {
        kafkaProducer.send(new ProducerRecord<>(this.flowTopic, flowDocument.toByteArray()), (metadata, exception) -> {
            if (exception != null) {
                LOG.warn("Simulation: error sending flow document to Kafka topic", exception);
            }
        });
    }

    public void close() throws IOException {
        kafkaProducer.close();
    }
}
