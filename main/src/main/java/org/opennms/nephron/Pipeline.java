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

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

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
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
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

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        Map<String, Object> kafkaProducerConfig = new HashMap<>();

        if (!Strings.isNullOrEmpty(options.getKafkaClientProperties())) {
            final Properties properties = new Properties();
            try {
                properties.load(new FileReader(options.getKafkaClientProperties()));
            } catch (IOException e) {
                LOG.error("Error loading properties file", e);
                throw new RuntimeException("Error reading properties file", e);
            }
            for(Map.Entry<Object,Object> entry : properties.entrySet()) {
                kafkaConsumerConfig.put(entry.getKey().toString(), entry.getValue());
                kafkaProducerConfig.put(entry.getKey().toString(), entry.getValue());
            }
        }

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
            flowSummaries.apply(new WriteToKafka(options.getBootstrapServers(), options.getFlowDestTopic(), kafkaProducerConfig));
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
        private final Duration maxFlowDuration;
        private final Duration earlyProcessingDelay;
        private final Duration lateProcessingDelay;
        private final Duration allowedLateness;

        public CalculateFlowStatistics(int topK, Duration fixedWindowSize, Duration maxFlowDuration, Duration earlyProcessingDelay, Duration lateProcessingDelay, Duration allowedLateness) {
            this.topK = topK;
            this.fixedWindowSize = Objects.requireNonNull(fixedWindowSize);
            this.maxFlowDuration = Objects.requireNonNull(maxFlowDuration);
            this.earlyProcessingDelay = Objects.requireNonNull(earlyProcessingDelay);
            this.lateProcessingDelay = Objects.requireNonNull(lateProcessingDelay);
            this.allowedLateness = Objects.requireNonNull(allowedLateness);
        }

        public CalculateFlowStatistics(NephronOptions options) {
            this(options.getTopK(),
                 Duration.millis(options.getFixedWindowSizeMs()),
                 Duration.millis(options.getMaxFlowDurationMs()),
                 Duration.millis(options.getEarlyProcessingDelayMs()),
                 Duration.millis(options.getLateProcessingDelayMs()),
                 Duration.millis(options.getAllowedLatenessMs()));
        }

        @Override
        public PCollection<FlowSummaryData> expand(PCollection<FlowDocument> input) {
            PCollection<FlowDocument> windowedStreamOfFlows = input.apply("WindowedFlows",
                    new WindowedFlows(fixedWindowSize, maxFlowDuration, earlyProcessingDelay, lateProcessingDelay, allowedLateness));

            PCollection<KV<CompoundKey, Aggregate>> keyedByConvWithTos =
                    windowedStreamOfFlows.apply("key_by_conv", ParDo.of(new KeyByConvWithTos()));

            SumsAndTopKs conv = aggregateSumsAndTopKs("conv_", keyedByConvWithTos,
                    CompoundKeyType.EXPORTER_INTERFACE_CONVERSATION,
                    topK,
                    k -> ((Ref.Conversation)k.lastRef()).hasCompleteConversationKey()
            );

            PCollectionTuple projected =
                    conv.withTos.sum.apply("proj_conv", ParDo.of(new ProjConvWithTos()).withOutputTags(BY_APP, TupleTagList.of(BY_HOST)));

            PCollection<KV<CompoundKey, Aggregate>> keyedByAppWithTos = projected.get(BY_APP);
            PCollection<KV<CompoundKey, Aggregate>> keyedByHostWithTos = projected.get(BY_HOST);

            SumsAndTopKs app = aggregateSumsAndTopKs("app_", keyedByAppWithTos,
                    CompoundKeyType.EXPORTER_INTERFACE_APPLICATION,
                    topK,
                    k -> true
            );

            SumsAndTopKs host = aggregateSumsAndTopKs("host_", keyedByHostWithTos,
                    CompoundKeyType.EXPORTER_INTERFACE_HOST,
                    topK,
                    k -> true
            );

            // exporter/interface and exporter/interface/tos aggregations are used as "parents" when the
            // "include other" option is selected for topK-queries
            // -> they must not be limited to topK but contain all cases

            TotalAndSummary tos = aggregateParentTotal("tos_", app.withTos.sum);
            TotalAndSummary itf = aggregateParentTotal("itf_", tos.total);

            // Merge all the summary collections
            PCollectionList<FlowSummaryData> flowSummaries = PCollectionList.of(itf.summary)
                    .and(tos.summary)
                    .and(app.withTos.topK)
                    .and(app.withoutTos.topK)
                    .and(host.withTos.topK)
                    .and(host.withoutTos.topK)
                    .and(conv.withTos.topK)
                    .and(conv.withoutTos.topK);
            return flowSummaries.apply(Flatten.pCollections());
        }
    }

    private static TupleTag<KV<CompoundKey, Aggregate>> BY_HOST = new TupleTag<KV<CompoundKey, Aggregate>>(){};
    private static TupleTag<KV<CompoundKey, Aggregate>> BY_APP = new TupleTag<KV<CompoundKey, Aggregate>>(){};

    public static class WindowedFlows extends PTransform<PCollection<FlowDocument>, PCollection<FlowDocument>> {
        private final Duration fixedWindowSize;
        private final Duration maxFlowDuration;
        private final Duration earlyProcessingDelay;
        private final Duration lateProcessingDelay;
        private final Duration allowedLateness;

        public WindowedFlows(Duration fixedWindowSize, Duration maxFlowDuration, Duration earlyProcessingDelay, Duration lateProcessingDelay, Duration allowedLateness) {
            this.fixedWindowSize = Objects.requireNonNull(fixedWindowSize);
            this.maxFlowDuration = Objects.requireNonNull(maxFlowDuration);
            this.earlyProcessingDelay = Objects.requireNonNull(earlyProcessingDelay);
            this.lateProcessingDelay = Objects.requireNonNull(lateProcessingDelay);
            this.allowedLateness = Objects.requireNonNull(allowedLateness);
        }

        @Override
        public PCollection<FlowDocument> expand(PCollection<FlowDocument> input) {
            return input.apply("attach_timestamp", attachTimestamps(fixedWindowSize, maxFlowDuration))
                    .apply("to_windows", toWindow(fixedWindowSize, earlyProcessingDelay, lateProcessingDelay, allowedLateness));
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
                    .withTopic(topic)
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(KafkaInputFlowDeserializer.class)
                    .withConsumerConfigUpdates(kafkaConsumerConfig)
                    .withBootstrapServers(bootstrapServers) // Order matters: bootstrap server overwrite consumer properties
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

        public static long getTimestampMs(FlowDocument doc) {
            return doc.getLastSwitched().getValue();
        }

        public static Instant getTimestamp(FlowDocument doc) {
            return Instant.ofEpochMilli(getTimestampMs(doc));
        }
    }

    public static class WriteToKafka extends PTransform<PCollection<FlowSummaryData>, PDone> {
        private final String bootstrapServers;
        private final String topic;
        private final Map<String, Object> kafkaProducerConfig;

        public WriteToKafka(String bootstrapServers, String topic, Map<String, Object> kafkaProducerConfig) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            this.topic = Objects.requireNonNull(topic);
            this.kafkaProducerConfig = kafkaProducerConfig;
        }

        @Override
        public PDone expand(PCollection<FlowSummaryData> input) {
            return input.apply(toJson())
                    .apply(KafkaIO.<Void, String>write()
                            .withProducerConfigUpdates(kafkaProducerConfig)
                            .withBootstrapServers(bootstrapServers) // Order matters: bootstrap server overwrite producer properties
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

    /**
     * Dispatches a {@link FlowDocument} to all of the windows that overlap with the flow range.
     *
     * @return transform
     */
    public static ParDo.SingleOutput<FlowDocument, FlowDocument> attachTimestamps(Duration fixedWindowSize, Duration maxFlowDuration) {
        return ParDo.of(new DoFn<FlowDocument, FlowDocument>() {
            final long windowSizeMs = fixedWindowSize.getMillis();
            final long maxFlowDurationMs = maxFlowDuration.getMillis();
            @ProcessElement
            public void processElement(ProcessContext c) {

                // We want to dispatch the flow to all the windows it may be a part of
                // The flow ranges from [delta_switched, last_switched]
                final FlowDocument flow = c.element();

                long deltaSwitched = flow.getDeltaSwitched().getValue();
                long lastSwitched = flow.getLastSwitched().getValue();
                int nodeId = flow.getExporterNode().getNodeId();

                long firstWindow = UnalignedFixedWindows.windowNumber(nodeId, windowSizeMs, deltaSwitched); // the first window the flow falls into
                long lastWindow = UnalignedFixedWindows.windowNumber(nodeId, windowSizeMs, lastSwitched); // the last window the flow falls into (assuming lastSwitched is inclusive)
                long nbWindows = lastWindow - firstWindow + 1;

                long timestamp = deltaSwitched;
                for (long i = 0; i < nbWindows; i++) {
                    if (timestamp <= c.timestamp().getMillis() - maxFlowDurationMs) {
                        // Caused by: java.lang.IllegalArgumentException: Cannot output with timestamp 1970-01-01T00:00:00.000Z. Output timestamps must be no earlier than the timestamp of the current input (2020-
                        //                            04-14T15:33:11.302Z) minus the allowed skew (30 minutes). See the DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.
                        //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.checkTimestamp(SimpleDoFnRunner.java:607)
                        //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.outputWithTimestamp(SimpleDoFnRunner.java:573)
                        //                    at org.opennms.nephron.FlowAnalyzer$1.processElement(FlowAnalyzer.java:96)
                        RATE_LIMITED_LOG.warn("Skipping output for flow w/ start: {}, end: {}, target timestamp: {}, current input timestamp: {}. Full flow: {}",
                                Instant.ofEpochMilli(deltaSwitched), Instant.ofEpochMilli(lastSwitched), Instant.ofEpochMilli(timestamp), c.timestamp(),
                                flow);
                    } else {
                        c.outputWithTimestamp(flow, Instant.ofEpochMilli(timestamp));
                    }
                    // ensure that the timestamp used for the last window is not larger than lastSwitched
                    if (timestamp + windowSizeMs < lastSwitched) {
                        timestamp += windowSizeMs;
                    } else {
                        timestamp = lastSwitched;
                    }
                }
            }

            @Override
            public Duration getAllowedTimestampSkew() {
                return maxFlowDuration;
            }

        });
    }

    public static FlowSummaryData toFlowSummaryData(AggregationType aggregationType, IntervalWindow window, KV<CompoundKey, Aggregate> el, int ranking) {
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

        if (fsd.key.getType() == CompoundKeyType.EXPORTER_INTERFACE_HOST || fsd.key.getType() == CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST) {
            flowSummary.setHostName(Strings.emptyToNull(fsd.aggregate.getHostname()));
        }
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

        return Window.into(UnalignedFixedWindows.of(fixedWindowSize))
                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
                .triggering(trigger)
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                // After some time, we assume no more data of interest will arrive, and the trigger stops executing
                .withAllowedLateness(allowedLateness)
                // each pane is aggregated separately
                // -> aggregation is done by elastic search if multiple flow summary documents exist for a window
                .discardingFiredPanes();
    }

    /**
     * Maps flow documents into pairs of compound keys (of type EXPORTER_INTERFACE_TOS_CONVERSATION) and aggregates.
     * <p>
     * {@link Aggregate} values are determined for window based on the intersection of flows with their windows.
     */
    public static class KeyByConvWithTos extends DoFn<FlowDocument, KV<CompoundKey, Aggregate>> {

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

        private Aggregate aggregatize(final IntervalWindow window, final FlowDocument flow, final String hostname, String hostname2) {
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
                   new Aggregate(bytes, 0, hostname, hostname2, flow.hasEcn() ? flow.getEcn().getValue() : null) :
                   new Aggregate(0, bytes, hostname, hostname2, flow.hasEcn() ? flow.getEcn().getValue() : null);
        }

        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            final FlowDocument flow = c.element();
            try {
                CompoundKey key = CompoundKeyType.EXPORTER_INTERFACE_TOS_CONVERSATION.create(flow);
                String src = Strings.nullToEmpty(flow.getSrcAddress());
                String dst = Strings.nullToEmpty(flow.getDstAddress());
                String hostname, hostname2;
                if (src.compareTo(dst) < 0) {
                    hostname = flow.getSrcHostname();
                    hostname2 = flow.getDstHostname();
                } else {
                    hostname2 = flow.getSrcHostname();
                    hostname = flow.getDstHostname();
                }
                Aggregate aggregate = aggregatize(window, flow, hostname, hostname2);
                c.output(KV.of(key, aggregate));
            } catch (MissingFieldsException mfe) {
                flowsWithMissingFields.inc();
            }
        }

    }

    public static class ProjConvWithTos extends DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, Aggregate>> {

        @ProcessElement
        public void processElement(@Element KV<CompoundKey, Aggregate> kv, MultiOutputReceiver out) {
            Ref.Conversation rc = (Ref.Conversation)kv.getKey().lastRef();
            Aggregate a = kv.getValue();
            CompoundKey appKey = kv.getKey().changeLastRef(CompoundKeyType.EXPORTER_INTERFACE_TOS_APPLICATION, Ref.Application.of(rc.application));
            out.get(BY_APP).output(KV.of(appKey, a.withHostname(null)));
            CompoundKey hostKey1 = kv.getKey().changeLastRef(CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST, Ref.Host.of(rc.smallerAddress));
            out.get(BY_HOST).output(KV.of(hostKey1, a.withHostname(a.getHostname())));
            CompoundKey hostKey2 = kv.getKey().changeLastRef(CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST, Ref.Host.of(rc.largerAddress));
            out.get(BY_HOST).output(KV.of(hostKey2, a.withHostname(a.getHostname2())));
        }

    }

    public static TotalAndSummary aggregateParentTotal(
            String transformPrefix,
            PCollection<KV<CompoundKey, Aggregate>> child
    ) {
        PCollection<KV<CompoundKey, Aggregate>> parentTotal = child
                .apply(transformPrefix + "group_by_outer_key", ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, Aggregate>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<CompoundKey, Aggregate> el = c.element();
                        c.output(KV.of(el.getKey().getOuterKey(), el.getValue()));
                    }
                }))
                .apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()));

        PCollection<FlowSummaryData> summary = parentTotal.apply(transformPrefix + "total_summary", ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, FlowSummaryData>() {
            @ProcessElement
            public void processElement(ProcessContext c, IntervalWindow window) {
                c.output(toFlowSummaryData(AggregationType.TOTAL, window, c.element(), 0));
            }
        }));
        return new TotalAndSummary(parentTotal, summary);
    }

    public static SumsAndTopKs aggregateSumsAndTopKs(
            String transformPrefix,
            PCollection<KV<CompoundKey, Aggregate>> groupedByKeyWithTos,
            CompoundKeyType typeWithoutTos,
            int k,
            SerializableFunction<CompoundKey, Boolean> includeKeyInTopK
    ) {
        SumAndTopK withTos = aggregateSumAndTopK(transformPrefix + "with_tos_", groupedByKeyWithTos, k, includeKeyInTopK);

        PCollection<KV<CompoundKey, Aggregate>> groupedByKeyWithoutTos =
                withTos.sum.apply(
                        transformPrefix + "group_without_tos_",
                        ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, Aggregate>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<CompoundKey, Aggregate> el = c.element();
                                c.output(KV.of(el.getKey().project(typeWithoutTos), el.getValue()));
                            }
                        }));
        SumAndTopK withoutTos = aggregateSumAndTopK(transformPrefix + "without_tos_", groupedByKeyWithoutTos, k, includeKeyInTopK);

        return new SumsAndTopKs(withTos, withoutTos);
    }

    public static SumAndTopK aggregateSumAndTopK(
            String transformPrefix,
            PCollection<KV<CompoundKey, Aggregate>> groupedByKey,
            int k,
            SerializableFunction<CompoundKey, Boolean> includeKeyInTopK
    ) {
        PCollection<KV<CompoundKey, Aggregate>> sum =
                groupedByKey.apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()));

        PCollection<FlowSummaryData> topK = sum
                .apply(transformPrefix + "group_by_outer_key",
                        ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, KV<CompoundKey, Aggregate>>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<CompoundKey, Aggregate> el = c.element();
                                if (includeKeyInTopK.apply(el.getKey())) {
                                    c.output(KV.of(el.getKey().getOuterKey(), el));
                                }
                            }
                        }))
                .apply(transformPrefix + "top_k_per_key", Top.perKey(k, new FlowBytesValueComparator()))
                .apply(transformPrefix + "flatten", Values.create())
                .apply(transformPrefix + "top_k_summary", ParDo.of(new DoFn<List<KV<CompoundKey, Aggregate>>, FlowSummaryData>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, IntervalWindow window) {
                        int ranking = 1;
                        for (KV<CompoundKey, Aggregate> el : c.element()) {
                            FlowSummaryData flowSummary = toFlowSummaryData(AggregationType.TOPK, window, el, ranking++);
                            c.output(flowSummary);
                        }
                    }
                }));
        return new SumAndTopK(sum, topK);
    }

    public static class TotalAndSummary {
        public final PCollection<KV<CompoundKey, Aggregate>> total;
        public final PCollection<FlowSummaryData> summary;
        public TotalAndSummary(PCollection<KV<CompoundKey, Aggregate>> total, PCollection<FlowSummaryData> summary) {
            this.total = total;
            this.summary = summary;
        }
    }

    public static class SumAndTopK {
        public final PCollection<KV<CompoundKey, Aggregate>> sum;
        public final PCollection<FlowSummaryData> topK;

        public SumAndTopK(PCollection<KV<CompoundKey, Aggregate>> sum, PCollection<FlowSummaryData> topK) {
            this.sum = sum;
            this.topK = topK;
        }
    }

    public static class SumsAndTopKs {
        public final SumAndTopK withTos;
        public final SumAndTopK withoutTos;

        public SumsAndTopKs(SumAndTopK withTos, SumAndTopK withoutTos) {
            this.withTos = withTos;
            this.withoutTos = withoutTos;
        }
    }

}
