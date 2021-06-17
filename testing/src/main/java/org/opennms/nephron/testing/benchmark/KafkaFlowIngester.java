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

package org.opennms.nephron.testing.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.testing.flowgen.FlowDocuments;
import org.opennms.nephron.testing.flowgen.FlowGenOptions;
import org.opennms.nephron.testing.flowgen.Limiter;
import org.opennms.nephron.testing.flowgen.SourceConfig;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility program that generates synthetic flows and ingests them into Kafka.
 *
 * The {@link FlowGenOptions#getPlaybackMode()} command line argument is of particular interest. It controls if
 * flow timestamps are calculated based on a configured start timestamp or if the current time is used.
 */
public class KafkaFlowIngester {

    private static Logger LOG = LoggerFactory.getLogger(KafkaFlowIngester.class);

    public static Producer<String, byte[]> createProducer(FlowGenOptions options) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new KafkaProducer<>(props);
    }

    public static long sendRecordsToKafka(FlowGenOptions options) {
        final Producer<String, byte[]> producer = createProducer(options);
        SourceConfig sourceConfig = SourceConfig.of(options, null);
        Stream<FlowDocument> stream = FlowDocuments.stream(sourceConfig);

        Duration logPeriod = Duration.standardSeconds(10);
        Instant start = Instant.now();

        AtomicLong total = new AtomicLong(0);

        stream.forEach(new Consumer<FlowDocument>() {
                           Instant nextLog = start.plus(logPeriod);
                           long lastTotal = 0;
                           Limiter limiter = Limiter.of(sourceConfig.flowsPerSecond);

                           @Override
                           public void accept(FlowDocument flowDocument) {
                               while (!limiter.check(1)) {
                                   try {
                                       Thread.sleep(10);
                                   } catch (InterruptedException e) {
                                       throw new RuntimeException(e);
                                   }
                               }
                               long lTotal = total.incrementAndGet();
                               producer.send(new ProducerRecord<>(options.getFlowSourceTopic(), flowDocument.toByteArray()));
                               if (Instant.now().isAfter(nextLog)) {
                                   LOG.info("duration: " + new Duration(start, nextLog) + "; total: " + lTotal + "; current rate (1/s): " + String.format("%.2f", (lTotal - lastTotal) / (double)logPeriod.getStandardSeconds()));
                                   nextLog = nextLog.plus(logPeriod);
                                   lastTotal = lTotal;
                               }
                           }
                       }
        );
        return total.get();
    }

    public static void main(String[] args) {
        FlowGenOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FlowGenOptions.class);
        var start = Instant.now();
        long total = sendRecordsToKafka(options);
        LOG.info("Output " + total + " flows in " + new Duration(start, Instant.now()));
    }
}
