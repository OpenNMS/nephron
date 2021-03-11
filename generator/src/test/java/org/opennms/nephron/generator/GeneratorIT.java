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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.eclipse.persistence.jaxb.MarshallerProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.FlowGenerator;
import org.opennms.nephron.catheter.Simulation;
import org.opennms.nephron.catheter.json.ExporterJson;
import org.opennms.nephron.catheter.json.FlowGeneratorJson;
import org.opennms.nephron.catheter.json.SimulationJson;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;

public class GeneratorIT {
    public static final String FLOW_TOPIC = "flows";

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Before
    public void before() {
        createTopics(FLOW_TOPIC);
    }

    private void createTopics(String... topics) {
        final List<NewTopic> newTopics =
                Arrays.stream(topics)
                      .map(topic -> new NewTopic(topic, 1, (short) 1))
                      .collect(Collectors.toList());
        try (final AdminClient admin = AdminClient.create(ImmutableMap.<String, Object>builder()
                                                                  .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                                                                  .build())) {
            admin.createTopics(newTopics);
        }
    }

    @Test
    public void testMainMethod() throws Exception {
        final SimulationJson simulationJson = new SimulationJson();
        simulationJson.setBootstrapServers(kafka.getBootstrapServers());
        simulationJson.setFlowTopic(FLOW_TOPIC);
        simulationJson.setRealtime(true);
        simulationJson.setStartTime(Instant.now());
        simulationJson.setTickMs(250);

        final FlowGeneratorJson flowGeneratorJson1 = new FlowGeneratorJson();
        flowGeneratorJson1.setActiveTimeoutMs(1000);
        flowGeneratorJson1.setBytesPerSecond(1000_000);
        flowGeneratorJson1.setMaxFlowCount(10);
        flowGeneratorJson1.setMinFlowDurationMs(1000);
        flowGeneratorJson1.setMaxFlowDurationMs(20000);

        final ExporterJson exporterJson1 = new ExporterJson();
        exporterJson1.setForeignSource("foreignSource1");
        exporterJson1.setForeignId("foreignId1");
        exporterJson1.setNodeId(1);
        exporterJson1.setClockOffsetMs(10);
        exporterJson1.setFlowGenerator(flowGeneratorJson1);
        exporterJson1.setLocation("Default");

        simulationJson.setExporters(Arrays.asList(exporterJson1));

        final Marshaller marshaller = JAXBContext.newInstance(SimulationJson.class).createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        marshaller.setProperty(MarshallerProperties.MEDIA_TYPE, "application/json");
        marshaller.setProperty(MarshallerProperties.JSON_INCLUDE_ROOT, true);

        final File tempFile = File.createTempFile("test-", ".json");
        tempFile.deleteOnExit();

        // write JSON file
        marshaller.marshal(simulationJson, tempFile);
        // run main with JSON file argument
        Generator.main(tempFile.getAbsolutePath());
        // setup consumer
        final KafkaConsumer<String, FlowDocument> kafkaConsumer = createConsumer();
        // check whether data arrive...
        await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofMinutes(1)).until(() -> kafkaConsumer.poll(250).count() > 0);
        // close the consumer
        kafkaConsumer.close();
    }

    @Test
    public void testJsonHandling() throws Exception {
        final Simulation simulation = Generator.fromFile(new File("src/test/resources/simulation.json"));

        final Simulation expected = Simulation.builder(simulation.getHandler())
                                              .withRealtime(true)
                                              .withStartTime(Instant.parse("2020-11-27T09:16:31.122Z"))
                                              .withTickMs(Duration.ofMillis(250))
                                              .withExporters(
                                                      Exporter.builder()
                                                              .withInputSnmp(98)
                                                              .withOutputSnmp(99)
                                                              .withNodeId(1)
                                                              .withForeignSource("foreignSource1")
                                                              .withForeignId("foreignId1")
                                                              .withClockOffset(Duration.ofSeconds(10))
                                                              .withLocation("Default")
                                                              .withGenerator(FlowGenerator.builder()
                                                                                          .withBytesPerSecond(1000_000L)
                                                                                          .withMaxFlowCount(10)
                                                                                          .withActiveTimeout(Duration.ofSeconds(1))
                                                                                          .withMinFlowDuration(Duration.ofSeconds(1))
                                                                                          .withMaxFlowDuration(Duration.ofSeconds(20))),
                                                      Exporter.builder()
                                                              .withInputSnmp(11)
                                                              .withOutputSnmp(12)
                                                              .withNodeId(2)
                                                              .withForeignSource("foreignSource2")
                                                              .withForeignId("foreignId2")
                                                              .withClockOffset(Duration.ofSeconds(-10))
                                                              .withLocation("Minion")
                                                              .withGenerator(FlowGenerator.builder()
                                                                                          .withBytesPerSecond(1000_000L)
                                                                                          .withMaxFlowCount(10)
                                                                                          .withActiveTimeout(Duration.ofSeconds(1))
                                                                                          .withMinFlowDuration(Duration.ofSeconds(2))
                                                                                          .withMaxFlowDuration(Duration.ofSeconds(15)))
                                                            )
                                              .withSeed(1606468048782L)
                                              .build();

        // check whether loaded file and expected simulation instance is equal
        assertThat(simulation, is(expected));
    }

    private KafkaConsumer<String, FlowDocument> createConsumer() {
        final Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaInputFlowDeserializer.class);
        final KafkaConsumer<String, FlowDocument> kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Collections.singletonList(FLOW_TOPIC));
        return kafkaConsumer;
    }

    public static class KafkaInputFlowDeserializer implements Deserializer<FlowDocument> {
        @Override
        public FlowDocument deserialize(String topic, byte[] data) {
            try {
                return FlowDocument.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
