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

import static org.opennms.nephron.testing.benchmark.KafkaFlowIngester.sendRecordsToKafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.Pipeline;
import org.opennms.nephron.testing.flowgen.FlowGenOptions;
import org.opennms.nephron.testing.flowgen.SourceConfig;
import org.opennms.nephron.testing.flowgen.SyntheticFlowSource;
import org.opennms.nephron.testing.flowgen.SyntheticFlowTimestampPolicyFactory;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public abstract class InputSetup {

    public enum Seletion {
        MEMORY {
            @Override
            public InputSetup createInputSetup(BenchmarkOptions options) {
                return new MemoryInputSetup(options);
            }
        },
        KAFKA {
            @Override
            public InputSetup createInputSetup(BenchmarkOptions options) {
                return new KafkaInputSetup(options);
            }
        };

        public abstract InputSetup createInputSetup(BenchmarkOptions options);

    }

    protected final BenchmarkOptions options;
    protected final SourceConfig sourceConfig;

    public InputSetup(BenchmarkOptions options) {
        this.options = options;
        this.sourceConfig = SourceConfig.of(options, SyntheticFlowTimestampPolicyFactory.withLimitedDelay(options, Pipeline.ReadFromKafka::getTimestamp));
    }

    abstract PTransform<PBegin, PCollection<FlowDocument>> source();

    abstract void generate() throws Exception;

    private static TimestampPolicyFactory<String, FlowDocument> createTimestampPolicyFactory(
            long maxIdx,
            Duration maxInputDelay,
            Duration maxRunDuration
    ) {
        return (tp, previousWatermark) -> new CustomTimestampPolicyWithLimitedDelay<String, FlowDocument>(
                Pipeline.ReadFromKafka::getTimestamp,
                maxInputDelay,
                previousWatermark
        ) {
            private long counter = 0;
            private Instant start = Instant.now();
            @Override
            public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<String, FlowDocument> record) {
                counter++;
                return super.getTimestampForRecord(ctx, record);
            }

            @Override
            public Instant getWatermark(PartitionContext ctx) {
                // does not work if the source is split
                if (counter >= maxIdx || new Duration(start, Instant.now()).isLongerThan(maxRunDuration)) {
                    return BoundedWindow.TIMESTAMP_MAX_VALUE;
                } else {
                    return super.getWatermark(ctx);
                }
            }
        };
    }

    public static class KafkaInputSetup extends InputSetup {

        public KafkaInputSetup(BenchmarkOptions options) {
            super(options);
        }

        @Override
        public PTransform<PBegin, PCollection<FlowDocument>> source() {
            Map<String, Object> kafkaConsumerConfig = new HashMap<>();
            kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
            // Auto-commit should be disabled when checkpointing is on:
            // the state in the checkpoints are used to derive the offsets instead
            kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, options.getAutoCommit());
            TimestampPolicyFactory<String, FlowDocument> tpf = InputSetup.createTimestampPolicyFactory(
                    sourceConfig.maxIdx,
                    Duration.millis(options.getDefaultMaxInputDelayMs()),
                    Duration.standardSeconds(options.getMaxRunSecs())
            );

            return new Pipeline.ReadFromKafka(
                    options.getBootstrapServers(),
                    options.getFlowSourceTopic(),
                    kafkaConsumerConfig,
                    tpf
            );
        }

        @Override
        public void generate() throws Exception {
            sendRecordsToKafka(options);
        }

    }

    public static class MemoryInputSetup extends InputSetup {
        public MemoryInputSetup(BenchmarkOptions options) {
            super(options);
        }

        @Override
        public PTransform<PBegin, PCollection<FlowDocument>> source() {
            return SyntheticFlowSource.readFromSyntheticSource(sourceConfig);
        }

        @Override
        public void generate() throws Exception {
        }

    }

}
