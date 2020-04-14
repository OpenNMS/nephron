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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import com.google.gson.Gson;

/**
 * Complete end-to-end test - reading & writing to/from Kafka
 */
public class FlowAnalyzerIT {

    private Gson gson = new Gson();

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Test
    public void canStreamIt() throws InterruptedException {
        Executor executor = Executors.newCachedThreadPool();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);

        // Write some flows
        OldFlowGenerator flowGenerator = new OldFlowGenerator(producer);
        executor.execute(flowGenerator);

        List<TopKFlows> allRecords = new LinkedList<>();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("flows_processed"));
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (final ConsumerRecord<String, String> record : records) {
                        System.out.println("Got record: " + record);
                        allRecords.add(gson.fromJson(record.value(), TopKFlows.class));
                    }
                }
            }
        });

        NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + kafka.getBootstrapServers(),
                "--fixedWindowSize=5s")
                .as(NephronOptions.class);

        // Fire up the pipeline
        final Pipeline pipeline = FlowAnalyzer.create(options);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    pipeline.run();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof InterruptedException) {
                        return;
                    }
                    ex.printStackTrace();
                }
            }
        });
        t.start();

        // Wait for some records to be consumed
        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> allRecords, hasSize(greaterThanOrEqualTo(6)));

        t.interrupt();
        t.join();
    }
}
