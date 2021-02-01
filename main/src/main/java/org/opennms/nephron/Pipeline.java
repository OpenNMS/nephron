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

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.nephron.coders.KafkaInputFlowDeserializer;
import org.opennms.nephron.elastic.AggregationType;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.elastic.IndexStrategy;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.swrve.ratelimitedlogger.RateLimitedLog;

public class Pipeline {

    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final RateLimitedLog RATE_LIMITED_LOG = RateLimitedLog
            .withRateLimit(LOG)
            .maxRate(5).every(java.time.Duration.ofSeconds(10))
            .build();

    /**
     * Creates a new pipeline from the given set of runtime options.
     *
     * @param options runtime options
     * @return a new pipeline
     */
    public static org.apache.beam.sdk.Pipeline create(NephronOptions options) {
        Objects.requireNonNull(options);
        org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(p);

        // Read from Kafka
        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        // Auto-commit should be disabled when checkpointing is on:
        // the state in the checkpoints are used to derive the offsets instead
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, options.getAutoCommit());
        PCollection<FlowDocument> streamOfFlows = p.apply(new ReadFromKafka(options.getBootstrapServers(),
                options.getFlowSourceTopic(), kafkaConsumerConfig));

        // Calculate the flow summary statistics
        PCollection<FlowSummaryData> flowSummaries = streamOfFlows.apply(new CalculateFlowStatistics(options));

        // Write the results out to Elasticsearch
        flowSummaries.apply(new WriteToElasticsearch(options));

        // Optionally write out to Kafka as well
        if (options.getFlowDestTopic() != null) {
            flowSummaries.apply(new WriteToKafka(options.getBootstrapServers(), options.getFlowDestTopic()));
        }

        return p;
    }

    public static void registerCoders(org.apache.beam.sdk.Pipeline p) {
        final CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(FlowDocument.class, new FlowDocumentProtobufCoder());
        coderRegistry.registerCoderForClass(FlowSummaryData.class, new FlowSummaryData.FlowSummaryDataCoder());
        coderRegistry.registerCoderForClass(CompoundKey.class, new CompoundKey.CompoundKeyCoder());
        coderRegistry.registerCoderForClass(Aggregate.class, new Aggregate.AggregateCoder());
    }

    public static class CalculateFlowStatistics extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummaryData>> {
        private final int topK;
        private final Duration fixedWindowSize;
        private final Duration earlyProcessingDelay;
        private final Duration lateProcessingDelay;
        private final Duration allowedLateness;

        public CalculateFlowStatistics(int topK, Duration fixedWindowSize, Duration earlyProcessingDelay, Duration lateProcessingDelay, Duration allowedLateness) {
            this.topK = topK;
            this.fixedWindowSize = Objects.requireNonNull(fixedWindowSize);
            this.earlyProcessingDelay = Objects.requireNonNull(earlyProcessingDelay);
            this.lateProcessingDelay = Objects.requireNonNull(lateProcessingDelay);
            this.allowedLateness = Objects.requireNonNull(allowedLateness);
        }

        public CalculateFlowStatistics(NephronOptions options) {
            this(options.getTopK(),
                 Duration.millis(options.getFixedWindowSizeMs()),
                 Duration.millis(options.getEarlyProcessingDelayMs()),
                 Duration.millis(options.getLateProcessingDelayMs()),
                 Duration.millis(options.getAllowedLatenessMs()));
        }

        @Override
        public PCollection<FlowSummaryData> expand(PCollection<FlowDocument> input) {
            PCollection<FlowDocument> windowedStreamOfFlows = input.apply("to_windows", toWindow(fixedWindowSize, earlyProcessingDelay, lateProcessingDelay, allowedLateness));

            // exporter/interface and exporter/interface/tos aggregations are used as "parents" when the
            // "include other" option is selected for topK-queries
            // -> they must not be limited to topK but contain all cases

            PCollection<FlowSummaryData> totalBytesByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTotalBytesByExporterAndInterface",
                    new CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", CompoundKeyType.EXPORTER_INTERFACE));

            PCollection<FlowSummaryData> totalBytesByExporterAndInterfaceAndTos = windowedStreamOfFlows.apply("CalculateTotalBytesByExporterAndInterfaceAndTos",
                    new CalculateTotalBytes("CalculateTotalBytesByExporterAndInterfaceAndTos_", CompoundKeyType.EXPORTER_INTERFACE_TOS));

            PCollection<FlowSummaryData> topKAppsByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTopAppsByExporterAndInterface",
                    new CalculateTopKGroups("CalculateTopAppsByExporterAndInterface_", topK, CompoundKeyType.EXPORTER_INTERFACE_APPLICATION));

            PCollection<FlowSummaryData> topKHostsByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTopHostsByExporterAndInterface",
                    new CalculateTopKGroups("CalculateTopHostsByExporterAndInterface_", topK, CompoundKeyType.EXPORTER_INTERFACE_HOST));

            PCollection<FlowSummaryData> topKConversationsByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTopConversationsByExporterAndInterface",
                    new CalculateTopKGroups("CalculateTopConversationsByExporterAndInterface_", topK, CompoundKeyType.EXPORTER_INTERFACE_CONVERSATION));

            PCollection<FlowSummaryData> topKAppsByExporterAndInterfaceAndTos = windowedStreamOfFlows.apply("CalculateTopAppsByExporterAndInterfaceAndTos",
                    new CalculateTopKGroups("CalculateTopAppsByExporterAndInterfaceAndTos_", topK, CompoundKeyType.EXPORTER_INTERFACE_TOS_APPLICATION));

            PCollection<FlowSummaryData> topKHostsByExporterAndInterfaceAndTos = windowedStreamOfFlows.apply("CalculateTopHostsByExporterAndInterfaceAndTos",
                    new CalculateTopKGroups("CalculateTopHostsByExporterAndInterfaceAndTos_", topK, CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST));

            PCollection<FlowSummaryData> topKConversationsByExporterAndInterfaceAndTos = windowedStreamOfFlows.apply("CalculateTopConversationsByExporterAndInterfaceAndTos",
                    new CalculateTopKGroups("CalculateTopConversationsByExporterAndInterfaceAndTos_", topK, CompoundKeyType.EXPORTER_INTERFACE_TOS_CONVERSATION));

            // Merge all the collections
            PCollectionList<FlowSummaryData> flowSummaries = PCollectionList.of(totalBytesByExporterAndInterface)
                    .and(totalBytesByExporterAndInterfaceAndTos)
                    .and(topKAppsByExporterAndInterface)
                    .and(topKHostsByExporterAndInterface)
                    .and(topKConversationsByExporterAndInterface)
                    .and(topKAppsByExporterAndInterfaceAndTos)
                    .and(topKHostsByExporterAndInterfaceAndTos)
                    .and(topKConversationsByExporterAndInterfaceAndTos)
                    ;
            return flowSummaries.apply(Flatten.pCollections());
        }
    }

    /**
     * Used to calculate the top K groups using the given grouping function.
     * Assumes that the input stream is windowed.
     */
    public static class CalculateTopKGroups extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummaryData>> {
        private final String transformPrefix;
        private final int topK;
        private final DoFn<FlowDocument, KV<CompoundKey, Aggregate>> groupingBy;

        public CalculateTopKGroups(String transformPrefix, int topK, CompoundKeyType type) {
            this.transformPrefix = Objects.requireNonNull(transformPrefix);
            this.topK = topK;
            this.groupingBy = new KeyFlowBy(Objects.requireNonNull(type));
        }

        @Override
        public PCollection<FlowSummaryData> expand(PCollection<FlowDocument> input) {
            return input.apply(transformPrefix + "group_by_key_in_window", ParDo.of(groupingBy))
                    .apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()))
                    .apply(transformPrefix + "group_by_outer_key", ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, KV<CompoundKey, Aggregate>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            KV<CompoundKey, Aggregate> el = c.element();
                            c.output(KV.of(el.getKey().getOuterKey(), el));
                        }
                    }))
                    .apply(transformPrefix + "top_k_per_key", Top.perKey(topK, new FlowBytesValueComparator()))
                    .apply(transformPrefix + "flatten", Values.create())
                    .apply(transformPrefix + "top_k_for_window", ParDo.of(new DoFn<List<KV<CompoundKey, Aggregate>>, FlowSummaryData>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, FlowWindows.FlowWindow window) {
                            int ranking = 1;
                            for (KV<CompoundKey, Aggregate> el : c.element()) {
                                FlowSummaryData flowSummary = toFlowSummaryData(AggregationType.TOPK, window, el, ranking++);
                                c.output(flowSummary);
                            }
                        }
                    }));
        }
    }

    /**
     * Used to calculate the total bytes derived from flows for the given grouping.
     * Assumes that the input stream is windowed.
     */
    public static class CalculateTotalBytes extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummaryData>> {
        private final String transformPrefix;
        private final DoFn<FlowDocument, KV<CompoundKey, Aggregate>> groupingBy;

        public CalculateTotalBytes(String transformPrefix, CompoundKeyType type) {
            this.transformPrefix = Objects.requireNonNull(transformPrefix);
            this.groupingBy = new KeyFlowBy(Objects.requireNonNull(type));
        }

        @Override
        public PCollection<FlowSummaryData> expand(PCollection<FlowDocument> input) {
            return input.apply(transformPrefix + "group_by_key_in_window", ParDo.of(groupingBy))
                    .apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()))
                    .apply(transformPrefix + "total_bytes_for_window", ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, FlowSummaryData>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, FlowWindows.FlowWindow window) {
                            c.output(toFlowSummaryData(AggregationType.TOTAL, window, c.element(), 0));
                        }
                    }));
        }
    }

    public static class WriteToElasticsearch extends PTransform<PCollection<FlowSummaryData>, PDone> {
        private final String elasticIndex;
        private final IndexStrategy indexStrategy;
        private final ElasticsearchIO.ConnectionConfiguration esConfig;

        private final Counter flowsToEs = Metrics.counter("flows", "to_es");
        private final Distribution flowsToEsDrift = Metrics.distribution("flows", "to_es_drift");

        private int elasticRetryCount;
        private long elasticRetryDuration;

        public WriteToElasticsearch(String elasticUrl, String elasticUser, String elasticPassword, String elasticIndex,
                                    IndexStrategy indexStrategy, int elasticConnectTimeout, int elasticSocketTimeout,
                                    int elasticRetryCount, long elasticRetryDuration) {
            Objects.requireNonNull(elasticUrl);
            this.elasticIndex = Objects.requireNonNull(elasticIndex);
            this.indexStrategy = Objects.requireNonNull(indexStrategy);

            ElasticsearchIO.ConnectionConfiguration thisEsConfig = ElasticsearchIO.ConnectionConfiguration.create(
                    new String[]{elasticUrl}, elasticIndex, "_doc");
            if (!Strings.isNullOrEmpty(elasticUser) && !Strings.isNullOrEmpty(elasticPassword)) {
                thisEsConfig = thisEsConfig.withUsername(elasticUser).withPassword(elasticPassword);
            }
            thisEsConfig = thisEsConfig.withConnectTimeout(elasticConnectTimeout)
                                       .withSocketTimeout(elasticSocketTimeout);
            this.esConfig = thisEsConfig;
            this.elasticRetryCount = elasticRetryCount;
            this.elasticRetryDuration = elasticRetryDuration;
        }

        public WriteToElasticsearch(NephronOptions options) {
            this(options.getElasticUrl(), options.getElasticUser(), options.getElasticPassword(),
                    options.getElasticFlowIndex(), options.getElasticIndexStrategy(),
                    options.getElasticConnectTimeout(), options.getElasticSocketTimeout(),
                    options.getElasticRetryCount(), options.getElasticRetryDuration());
        }

        @Override
        public PDone expand(PCollection<FlowSummaryData> input) {
            return input.apply("SerializeToJson", toJson())
                    .apply("WriteToElasticsearch", ElasticsearchIO.write().withConnectionConfiguration(esConfig)
                            .withRetryConfiguration(
                                    ElasticsearchIO.RetryConfiguration.create(this.elasticRetryCount,
                                            Duration.millis(this.elasticRetryDuration))
                            )
                            .withUsePartialUpdate(true)
                            .withIndexFn(new ElasticsearchIO.Write.FieldValueExtractFn() {
                                @Override
                                public String apply(JsonNode input) {
                                    // We need to derive the timestamp from the document in order to be able to calculate
                                    // the correct index.
                                    java.time.Instant flowTimestamp = java.time.Instant.ofEpochMilli(input.get("@timestamp").asLong());

                                    // Derive the index
                                    String indexName = indexStrategy.getIndex(elasticIndex, flowTimestamp);

                                    // Metrics
                                    flowsToEs.inc();
                                    flowsToEsDrift.update(System.currentTimeMillis() - flowTimestamp.toEpochMilli());

                                    return indexName;
                                }
                            })
                            .withIdFn(new ElasticsearchIO.Write.FieldValueExtractFn() {
                                @Override
                                public String apply(JsonNode input) {
                                    return input.get("@timestamp").asLong() + "_" +
                                            input.get("grouped_by").asText() + "_" +
                                            input.get("grouped_by_key").asText() + "_" +
                                            input.get("aggregation_type").asText() + "_" +
                                            input.get("ranking").asLong();
                                }
                            }));
        }
    }

    public static TimestampPolicyFactory<String, FlowDocument> getKafkaInputTimestampPolicyFactory(Duration maxDelay) {
        return (tp, previousWatermark) -> new FlowTimestampPolicy(maxDelay, previousWatermark);
    }

    public static class ReadFromKafka extends PTransform<PBegin, PCollection<FlowDocument>> {
        private final String bootstrapServers;
        private final String topic;
        private final Map<String, Object> kafkaConsumerConfig;

        private final Counter flowsFromKafka = Metrics.counter("flows", "from_kafka");
        private final Distribution flowsFromKafkaDrift = Metrics.distribution("flows", "from_kafka_drift");

        public ReadFromKafka(String bootstrapServers, String topic, Map<String, Object> kafkaConsumerConfig) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            this.topic = Objects.requireNonNull(topic);
            this.kafkaConsumerConfig = Objects.requireNonNull(kafkaConsumerConfig);
        }

        @Override
        public PCollection<FlowDocument> expand(PBegin input) {
            final NephronOptions options = input.getPipeline().getOptions().as(NephronOptions.class);
            return input.apply(KafkaIO.<String, FlowDocument>read()
                    .withBootstrapServers(bootstrapServers)
                    .withTopic(topic)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(KafkaInputFlowDeserializer.class)
                    .withConsumerConfigUpdates(kafkaConsumerConfig)
                    .withTimestampPolicyFactory(getKafkaInputTimestampPolicyFactory(Duration.millis(options.getDefaultMaxInputDelayMs())))
                    .withoutMetadata()
            ).apply(Values.create())
                    .apply("init", ParDo.of(new DoFn<FlowDocument, FlowDocument>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            // Add deltaSwitched if missing, was observed a few times
                            FlowDocument flow = c.element();
                            if (!flow.hasDeltaSwitched()) {
                                flow = FlowDocument.newBuilder(c.element())
                                        .setDeltaSwitched(flow.getFirstSwitched())
                                        .build();
                            }
                            c.output(flow);

                            // Metrics
                            flowsFromKafka.inc();
                            flowsFromKafkaDrift.update(System.currentTimeMillis() - flow.getTimestamp());
                        }
                    }));
        }

        public static Instant getTimestamp(FlowDocument doc) {
            return Instant.ofEpochMilli(doc.getDeltaSwitched().getValue());
        }
    }

    public static class WriteToKafka extends PTransform<PCollection<FlowSummaryData>, PDone> {
        private final String bootstrapServers;
        private final String topic;

        public WriteToKafka(String bootstrapServers, String topic) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            this.topic = Objects.requireNonNull(topic);
        }

        @Override
        public PDone expand(PCollection<FlowSummaryData> input) {
            return input.apply(toJson())
                    .apply(KafkaIO.<Void, String>write()
                            .withBootstrapServers(bootstrapServers)
                            .withTopic(topic)
                            .withValueSerializer(StringSerializer.class)
                            .values()
                    );
        }
    }

    private static ParDo.SingleOutput<FlowSummaryData, String> toJson() {
        return ParDo.of(new DoFn<FlowSummaryData, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws JsonProcessingException {
                FlowSummary flowSummary = toFlowSummary(c.element());
                c.output(MAPPER.writeValueAsString(flowSummary));
            }
        });
    }

    static class SumBytes extends Combine.BinaryCombineFn<Aggregate> {
        @Override
        public Aggregate apply(Aggregate left, Aggregate right) {
            return Aggregate.merge(left, right);
        }
    }

    static class FlowBytesValueComparator implements Comparator<KV<CompoundKey, Aggregate>>, Serializable {
        @Override
        public int compare(KV<CompoundKey, Aggregate> a, KV<CompoundKey, Aggregate> b) {
            int res = Long.compare(a.getValue().getBytes(), b.getValue().getBytes());
            if (res != 0) {
                return res;
            } else {
                // use the lexicographic order of groupedByKey as a second order criteria
                // -> makes the FlowSummary ranking deterministic (eases unit tests)
                // -> the first order criteria orders large number of bytes before lower number of bytes
                //    whereas the second order criteria orders "smaller" strings before "larger" ones
                return b.getKey().groupedByKey().compareTo(a.getKey().groupedByKey());
            }
        }
    }


    public static FlowSummaryData toFlowSummaryData(AggregationType aggregationType, FlowWindows.FlowWindow window, KV<CompoundKey, Aggregate> el, int ranking) {
        return new FlowSummaryData(aggregationType, el.getKey(), el.getValue(), window.start().getMillis(), window.end().getMillis(), ranking);
    }

    public static FlowSummary toFlowSummary(FlowSummaryData fsd) {
        FlowSummary flowSummary = new FlowSummary();
        fsd.key.populate(flowSummary);
        flowSummary.setAggregationType(fsd.aggregationType);

        flowSummary.setRangeStartMs(fsd.windowStart);
        flowSummary.setRangeEndMs(fsd.windowEnd);
        // Use the range end as the timestamp
        flowSummary.setTimestamp(flowSummary.getRangeEndMs());

        flowSummary.setBytesEgress(fsd.aggregate.getBytesOut());
        flowSummary.setBytesIngress(fsd.aggregate.getBytesIn());
        flowSummary.setBytesTotal(flowSummary.getBytesIngress() + flowSummary.getBytesEgress());
        flowSummary.setCongestionEncountered(fsd.aggregate.isCongestionEncountered());
        flowSummary.setNonEcnCapableTransport(fsd.aggregate.isNonEcnCapableTransport());

        flowSummary.setHostName(fsd.aggregate.getHostname());
        flowSummary.setRanking(fsd.ranking);

        return flowSummary;
    }

    public static Window<FlowDocument> toWindow(Duration fixedWindowSize, Duration earlyProcessingDelay,  Duration lateProcessingDelay, Duration allowedLateness) {
        AfterWatermark.AfterWatermarkEarlyAndLate trigger = AfterWatermark
                // On Beam’s estimate that all the data has arrived (the watermark passes the end of the window)
                .pastEndOfWindow()

                // Any time late data arrives, after a delay - (wait and see if more late data shows up before firing)
                .withLateFirings(AfterProcessingTime
                                         .pastFirstElementInPane()
                                         .plusDelayOf(lateProcessingDelay));

        if (earlyProcessingDelay != null && !earlyProcessingDelay.isEqual(Duration.ZERO)) {
            // During the window, get near real-time estimates
            trigger = trigger.withEarlyFirings(AfterProcessingTime
                            .pastFirstElementInPane()
                            .plusDelayOf(earlyProcessingDelay));
        }

        return Window.<FlowDocument>into(FlowWindows.of(fixedWindowSize))
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
                .triggering(trigger)

                // After 4 hours, we assume no more data of interest will arrive, and the trigger stops executing
                .withAllowedLateness(allowedLateness)
                .accumulatingFiredPanes();
    }

    /**
     * Maps flow documents into pairs of compound keys and aggregates.
     *
     * The {@link CompoundKey} is determined by the {@link CompoundKeyType} given in the constructor. {@link Aggregate}
     * values are determined for window based on the intersection of flows with their windows.
     */
    public static class KeyFlowBy extends DoFn<FlowDocument, KV<CompoundKey, Aggregate>> {

        private final CompoundKeyType type;

        public KeyFlowBy(CompoundKeyType type) {
            this.type = type;
        }

        private final Counter flowsWithMissingFields = Metrics.counter(Pipeline.class, "flowsWithMissingFields");
        private final Counter flowsInWindow = Metrics.counter("flows", "in_window");


        public static long bytesInWindow(
                long deltaSwitched,
                long lastSwitchedInclusive,
                double multipliedNumBytes,
                long windowStart,
                long windowEndInclusive
        ) {
            // The flow duration ranges [delta_switched, last_switched] (both bounds are inclusive)
            long flowDurationMs = lastSwitchedInclusive - deltaSwitched + 1;

            // the start (inclusive) of the flow in this window
            long overlapStart = Math.max(deltaSwitched, windowStart);
            // the end (inclusive) of the flow in this window
            long overlapEnd = Math.min(lastSwitchedInclusive, windowEndInclusive);

            // the end of the previous window (inclusive)
            long previousEnd = overlapStart - 1;

            long bytesAtPreviousEnd = (long) ((previousEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);
            long bytesAtEnd = (long) ((overlapEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);

            return bytesAtEnd - bytesAtPreviousEnd;
        }

        private Aggregate aggregatize(final FlowWindows.FlowWindow window, final FlowDocument flow, final String hostname) {
            double multiplier = 1;
            if (flow.hasSamplingInterval()) {
                double samplingInterval = flow.getSamplingInterval().getValue();
                if (samplingInterval > 0) {
                    multiplier = samplingInterval;
                }
            }
            long bytes = bytesInWindow(
                    flow.getDeltaSwitched().getValue(),
                    flow.getLastSwitched().getValue(),
                    flow.getNumBytes().getValue() * multiplier,
                    window.start().getMillis(),
                    window.maxTimestamp().getMillis()
            );
            // Track
            flowsInWindow.inc();
            return Direction.INGRESS.equals(flow.getDirection()) ?
                   new Aggregate(bytes, 0, hostname, flow.hasEcn() ? flow.getEcn().getValue() : null) :
                   new Aggregate(0, bytes, hostname, flow.hasEcn() ? flow.getEcn().getValue() : null);
        }

        @ProcessElement
        public void processElement(ProcessContext c, FlowWindows.FlowWindow window) {
            final FlowDocument flow = c.element();
            try {
                for (final WithHostname<? extends CompoundKey> key: key(flow)) {
                    Aggregate aggregate = aggregatize(window, flow, key.hostname);
                    c.output(KV.of(key.value, aggregate));
                }
            } catch (MissingFieldsException mfe) {
                flowsWithMissingFields.inc();
            }
        }

        public Collection<WithHostname<CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            return type.create(flow);
        }
    }
}
