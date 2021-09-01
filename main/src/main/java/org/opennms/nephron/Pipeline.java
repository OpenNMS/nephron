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

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.nephron.coders.KafkaInputFlowDeserializer;
import org.opennms.nephron.cortex.CortexIo;
import org.opennms.nephron.cortex.TimeSeriesBuilder;
import org.opennms.nephron.elastic.AggregationType;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.elastic.IndexStrategy;
import org.opennms.nephron.network.IPAddress;
import org.opennms.nephron.network.IpValue;
import org.opennms.nephron.network.StringValue;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.net.InetAddresses;
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
        TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory =
                getKafkaInputTimestampPolicyFactory(Duration.millis(options.getDefaultMaxInputDelayMs()));
        return create(options, timestampPolicyFactory);
    }

    /**
     * Creates a new pipeline from the given set of runtime options using the given {@code TimestampPolicyFactory}.
     */
    public static org.apache.beam.sdk.Pipeline create(
            NephronOptions options,
            TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory
    ) {
        Objects.requireNonNull(options);
        org.apache.beam.sdk.Pipeline p = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(p);

        Map<String, Object> kafkaConsumerConfig = loadKafkaClientProperties(options);

        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        // Auto-commit should be disabled when checkpointing is on:
        // the state in the checkpoints are used to derive the offsets instead
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, options.getAutoCommit());
        PCollection<FlowDocument> streamOfFlows = p.apply(new ReadFromKafka(options.getBootstrapServers(),
                options.getFlowSourceTopic(), kafkaConsumerConfig, timestampPolicyFactory));

        // Calculate the flow summary statistics
        PCollection<FlowSummaryData> flowSummaries = streamOfFlows.apply(new CalculateFlowStatistics(options));

        // optionally attach different kinds of sinks
        attachWriteToElastic(options, flowSummaries);
        attachWriteToKafka(options, flowSummaries);
        attachWriteToCortex(options, flowSummaries);

        return p;
    }

    private static FlowSummaryData combineFlowSummaryData(
            FlowSummaryData d1,
            FlowSummaryData d2
    ) {
        return new FlowSummaryData(
                d1.key,
                Aggregate.merge(d1.aggregate, d2.aggregate)
        );
    }

    private static ParDo.SingleOutput<FlowSummaryData, KV<CompoundKey, FlowSummaryData>> KEY_SUMMARIES =
            ParDo.of(new DoFn<FlowSummaryData, KV<CompoundKey, FlowSummaryData>>() {
                @ProcessElement
                public void process(ProcessContext ctx) {
                    ctx.output(KV.of(ctx.element().key, ctx.element()));
                }
            });

    private static Map<String, Object> loadKafkaClientProperties(NephronOptions options) {
        Map<String, Object> kafkaClientProperties = new HashMap<>();

        if (!Strings.isNullOrEmpty(options.getKafkaClientProperties())) {
            final Properties properties = new Properties();
            try {
                properties.load(new FileReader(options.getKafkaClientProperties()));
            } catch (IOException e) {
                LOG.error("Error loading properties file", e);
                throw new RuntimeException("Error reading properties file", e);
            }
            for(Map.Entry<Object,Object> entry : properties.entrySet()) {
                kafkaClientProperties.put(entry.getKey().toString(), entry.getValue());
            }
        }
        return kafkaClientProperties;
    }

    public static void attachWriteToElastic(NephronOptions options, PCollection<FlowSummaryData> flowSummaries) {
        if (!Strings.isNullOrEmpty(options.getElasticUrl())) {
            flowSummaries.apply(new WriteToElasticsearch(options));
        }
    }

    public static void attachWriteToKafka(NephronOptions options, PCollection<FlowSummaryData> flowSummaries) {
        if (!Strings.isNullOrEmpty(options.getFlowDestTopic())) {
            var kafkaProducerConfig = loadKafkaClientProperties(options);
            flowSummaries.apply(new WriteToKafka(options.getBootstrapServers(), options.getFlowDestTopic(), kafkaProducerConfig));
        }
    }

    public static void attachWriteToCortex(NephronOptions options, PCollection<FlowSummaryData> flowSummaries) {
        if (cortexOutputEnabled(options)) {
            CortexIo.Write<CompoundKey, FlowSummaryData> cortexWrite;
            if (options.getCortexAccumulationDelayMs() != 0) {
                cortexWrite = CortexIo.of(options.getCortexWriteUrl(), Pipeline::cortexOutput,
                        new CompoundKey.CompoundKeyCoder(),
                        new FlowSummaryData.FlowSummaryDataCoder(),
                        Pipeline::combineFlowSummaryData,
                        Duration.millis(options.getCortexAccumulationDelayMs())
                );
            } else {
                cortexWrite = CortexIo.of(options.getCortexWriteUrl(), Pipeline::cortexOutput);
            }
            cortexWrite
                    .withMaxBatchSize(options.getCortexMaxBatchSize())
                    .withMaxBatchBytes(options.getCortexMaxBatchBytes())
            ;
            if (!Strings.isNullOrEmpty(options.getCortexOrgId())) {
                cortexWrite.withOrgId(options.getCortexOrgId());
            }
            flowSummaries
                    .apply(Filter.by(includeInCortexOutput(options)))
                    .apply(KEY_SUMMARIES)
                    .apply(cortexWrite);
        }
    }

    // copied and slightly adapted from org.opennms.netmgt.flows.classification.internal.validation.RuleValidator
    private static IpValue validateIpAddress(String ipAddressValue) throws IllegalArgumentException {
        final StringValue inputValue = new StringValue(ipAddressValue);
        var errorPrefix = "invalid cortexConsideredHosts argument - value: " + ipAddressValue;
        final List<StringValue> actualValues = inputValue.splitBy(",");
        for (StringValue eachValue : actualValues) {
            // In case it is ranged, verify the range
            if (eachValue.isRanged()) {
                final List<StringValue> rangedValues = eachValue.splitBy("-");
                // either a-, or a-b-c, etc.
                if (rangedValues.size() != 2) {
                    throw new IllegalArgumentException(errorPrefix + "; at range: " + eachValue.getValue());
                }
                // Ensure each range is an ip address
                for (StringValue rangedValue : rangedValues) {
                    if (rangedValue.contains("/")) {
                        throw new IllegalArgumentException(errorPrefix + "; CIDR notation not supported in address ranges: " + rangedValue.getValue());
                    }
                    if (!InetAddresses.isInetAddress(rangedValue.getValue())) {
                        throw new IllegalArgumentException(errorPrefix + "; not an ip address: " + rangedValue.getValue());
                    }
                }
                // Now verify the range itself
                final IPAddress begin = new IPAddress(rangedValues.get(0).getValue());
                final IPAddress end = new IPAddress(rangedValues.get(1).getValue());
                if (begin.isGreaterThan(end)) {
                    throw new IllegalArgumentException(errorPrefix + "; invalid address range: begin must not be after end - begin: " + begin + "; end: " + end);
                }
            } else {
                if (eachValue.contains("/")) {
                    try {
                        IpValue.parseCIDR(eachValue.getValue());
                    } catch (Exception e) {
                        throw new IllegalArgumentException(errorPrefix + "; not a valid CIDR value: " + eachValue.getValue());
                    }
                } else {
                    if (!InetAddresses.isInetAddress(eachValue.getValue())) {
                        throw new IllegalArgumentException(errorPrefix + "; not an ip address: " + eachValue.getValue());
                    }
                }
            }
        }
        return IpValue.of(inputValue);
    }


    private static SerializableFunction<FlowSummaryData, Boolean> includeInCortexOutput(NephronOptions options) {
        if (StringUtils.isNoneBlank(options.getCortexConsideredHosts())) {
            var ipValue = validateIpAddress(options.getCortexConsideredHosts());
            return fsd -> {
                switch (fsd.key.type) {
                    case EXPORTER_INTERFACE_HOST:
                    case EXPORTER_INTERFACE_TOS_HOST:
                        return ipValue.isInRange(fsd.key.data.address);
                    case EXPORTER_INTERFACE_CONVERSATION:
                    case EXPORTER_INTERFACE_TOS_CONVERSATION:
                        return false;
                    default:
                        return true;
                }
            };
        } else {
            return fsd -> {
                switch (fsd.key.type) {
                    case EXPORTER_INTERFACE_HOST:
                    case EXPORTER_INTERFACE_TOS_HOST:
                    case EXPORTER_INTERFACE_CONVERSATION:
                    case EXPORTER_INTERFACE_TOS_CONVERSATION:
                        return false;
                    default:
                        return true;
                }
            };
        }
    }

    private static boolean cortexOutputEnabled(NephronOptions options) {
        return !Strings.isNullOrEmpty(options.getCortexWriteUrl());
    }

    private static void cortexOutput(
            CompoundKey key,
            FlowSummaryData fsd,
            Instant eventTimestamp,
            int index,
            TimeSeriesBuilder builder
    ) {
        final var agg = fsd.aggregate;
        LOG.trace("cortex output - eventTimestamp: {}; keyType: {}; key: {}; index: {}; in: {}; out: {}; total: {}",
                eventTimestamp, fsd.key.type, fsd.key, index, agg.getBytesIn(), agg.getBytesOut(), agg.getBytesIn() + agg.getBytesOut());
        String pane = "pane-" + index;
        doCortexOutput(fsd, eventTimestamp, pane, "in", agg.getBytesIn(), builder);
        builder.nextSeries();
        doCortexOutput(fsd, eventTimestamp, pane, "out", agg.getBytesOut(), builder);
    }

    private static void doCortexOutput(
            FlowSummaryData fsd,
            Instant eventTimestamp,
            String paneId,
            String direction,
            long bytes,
            TimeSeriesBuilder builder
    ) {
        builder.addLabel("pane", paneId);
        builder.addLabel("direction", direction);
        builder.addSample(eventTimestamp.getMillis(), bytes);
        fsd.key.populate(builder);
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
        private final PTransform<PCollection<FlowDocument>, PCollection<FlowDocument>> windowing;

        public CalculateFlowStatistics(int topK, PTransform<PCollection<FlowDocument>, PCollection<FlowDocument>> windowing) {
            this.topK = topK;
            this.windowing = windowing;
        }

        public CalculateFlowStatistics(int topK, Duration fixedWindowSize, Duration maxFlowDuration, Duration earlyProcessingDelay, Duration lateProcessingDelay, Duration allowedLateness) {
            this(topK, new WindowedFlows(fixedWindowSize, maxFlowDuration, earlyProcessingDelay, lateProcessingDelay, allowedLateness));
        }

        public CalculateFlowStatistics(NephronOptions options) {
            this(options.getTopK(), new WindowedFlows(options));
        }

        @Override
        public PCollection<FlowSummaryData> expand(PCollection<FlowDocument> input) {
            PCollection<FlowDocument> windowedStreamOfFlows = input.apply("WindowedFlows", windowing);

            PCollection<KV<CompoundKey, Aggregate>> keyedByConvWithTos =
                    windowedStreamOfFlows.apply("key_by_conv", ParDo.of(new KeyByConvWithTos()));

            SumsAndTopKs conv = aggregateSumsAndTopKs("conv_", keyedByConvWithTos,
                    CompoundKeyType.EXPORTER_INTERFACE_CONVERSATION,
                    topK,
                    k -> k.isCompleteConversationKey()
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
            // -> all other persisted aggregations are topK aggregations

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

        public WindowedFlows(NephronOptions options) {
            this(
                    Duration.millis(options.getFixedWindowSizeMs()),
                    Duration.millis(options.getMaxFlowDurationMs()),
                    Duration.millis(options.getEarlyProcessingDelayMs()),
                    Duration.millis(options.getLateProcessingDelayMs()),
                    Duration.millis(options.getAllowedLatenessMs())
            );
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
        // a distribution would be more interesting for flowsToEsDrift
        // -> Unfortunately histograms are not supported Beam/Flink/Prometheus
        //    (cf. https://issues.apache.org/jira/browse/BEAM-10928)
        // -> use a gauge instead
        private final Gauge flowsToEsDrift = Metrics.gauge("flows", "to_es_drift");

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
            return input.apply("SerializeToJson", FLOW_SUMMARY_DATA_TO_JSON)
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
                                    flowsToEsDrift.set(System.currentTimeMillis() - flowTimestamp.toEpochMilli());

                                    return indexName;
                                }
                            }));
        }
    }

    public static TimestampPolicyFactory<byte[], FlowDocument> getKafkaInputTimestampPolicyFactory(Duration maxDelay) {
        return (tp, previousWatermark) ->
                new CustomTimestampPolicyWithLimitedDelay<>(ReadFromKafka::getTimestamp, maxDelay, previousWatermark);
    }

    public static class ReadFromKafka extends PTransform<PBegin, PCollection<FlowDocument>> {
        private final String bootstrapServers;
        private final String topic;
        private final Map<String, Object> kafkaConsumerConfig;

        private final Counter flowsFromKafka = Metrics.counter("flows", "from_kafka");
        // a distribution would be more interesting for from_kafka_drift
        // -> Unfortunately histograms are not supported Beam/Flink/Prometheus
        //    (cf. https://issues.apache.org/jira/browse/BEAM-10928)
        // -> use a gauge instead
        private final Gauge flowsFromKafkaDrift = Metrics.gauge("flows", "from_kafka_drift");

        private final TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory;

        public ReadFromKafka(
                String bootstrapServers,
                String topic,
                Map<String, Object> kafkaConsumerConfig,
                TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory
        ) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            this.topic = Objects.requireNonNull(topic);
            this.kafkaConsumerConfig = Objects.requireNonNull(kafkaConsumerConfig);
            this.timestampPolicyFactory = timestampPolicyFactory;
        }

        @Override
        public PCollection<FlowDocument> expand(PBegin input) {
            return input.apply(KafkaIO.<byte[], FlowDocument>read()
                    .withTopic(topic)
                    .withKeyDeserializer(ByteArrayDeserializer.class)
                    .withValueDeserializer(KafkaInputFlowDeserializer.class)
                    .withConsumerConfigUpdates(kafkaConsumerConfig)
                    .withBootstrapServers(bootstrapServers) // Order matters: bootstrap server overwrite consumer properties
                    .withTimestampPolicyFactory(timestampPolicyFactory)
                    .withoutMetadata()
            )
                    .apply("init", ParDo.of(new DoFn<KV<byte[], FlowDocument>, FlowDocument>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            // Add deltaSwitched if missing, was observed a few times
                            FlowDocument flow = c.element().getValue();
                            if (!flow.hasDeltaSwitched()) {
                                flow = FlowDocument.newBuilder(flow)
                                        .setDeltaSwitched(flow.getFirstSwitched())
                                        .build();
                            }
                            c.output(flow);

                            // Metrics
                            flowsFromKafka.inc();
                            flowsFromKafkaDrift.set(System.currentTimeMillis() - flow.getLastSwitched().getValue());
                        }
                    }));
        }

        public static long getTimestampMs(FlowDocument doc) {
            return doc.getLastSwitched().getValue();
        }

        public static Instant getTimestamp(KafkaRecord<byte[], FlowDocument> record) {
            return getTimestamp(record.getKV().getValue());
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
            return input.apply(FLOW_SUMMARY_DATA_TO_JSON)
                    .apply(KafkaIO.<Void, String>write()
                            .withProducerConfigUpdates(kafkaProducerConfig)
                            .withBootstrapServers(bootstrapServers) // Order matters: bootstrap server overwrite producer properties
                            .withTopic(topic)
                            .withValueSerializer(StringSerializer.class)
                            .values()
                    );
        }
    }

    private static ParDo.SingleOutput<FlowSummaryData, String> FLOW_SUMMARY_DATA_TO_JSON =
        ParDo.of(new DoFn<FlowSummaryData, String>() {
            @ProcessElement
            public void processElement(ProcessContext c, IntervalWindow window) throws JsonProcessingException {
                FlowSummary flowSummary = toFlowSummary(c.element(), window);
                c.output(MAPPER.writeValueAsString(flowSummary));
            }
        });

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

                long shift = UnalignedFixedWindows.perNodeShift(nodeId, windowSizeMs);
                if (deltaSwitched < shift) {
                    RATE_LIMITED_LOG.warn("Skipping output for flow whose start is too small w/ start: {}, end: {}, target timestamp: {}, current input timestamp: {}. Full flow: {}",
                            Instant.ofEpochMilli(deltaSwitched), Instant.ofEpochMilli(lastSwitched), Instant.ofEpochMilli(deltaSwitched), c.timestamp(),
                            flow);
                    return;
                }

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
                        RATE_LIMITED_LOG.warn("Skipping output for flow that reaches back too far w/ start: {}, end: {}, target timestamp: {}, current input timestamp: {}. Full flow: {}",
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

    public static FlowSummaryData toFlowSummaryData(KV<CompoundKey, Aggregate> el) {
        return new FlowSummaryData(el.getKey(), el.getValue());
    }

    public static FlowSummary toFlowSummary(FlowSummaryData fsd, IntervalWindow window) {
        FlowSummary flowSummary = new FlowSummary();
        fsd.key.populate(flowSummary);
        flowSummary.setAggregationType(fsd.key.type.isTotalNotTopK() ? AggregationType.TOTAL : AggregationType.TOPK);

        flowSummary.setRangeStartMs(window.start().getMillis());
        flowSummary.setRangeEndMs(window.end().getMillis());
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
                flowsInWindow.inc();
                c.output(KV.of(key, aggregate));
            } catch (MissingFieldsException mfe) {
                flowsWithMissingFields.inc();
            }
        }

    }

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

    public static Aggregate aggregatize(final IntervalWindow window, final FlowDocument flow, final String hostname, String hostname2) {
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
        return Direction.INGRESS.equals(flow.getDirection()) ?
               new Aggregate(bytes, 0, hostname, hostname2, flow.hasEcn() ? flow.getEcn().getValue() : null) :
               new Aggregate(0, bytes, hostname, hostname2, flow.hasEcn() ? flow.getEcn().getValue() : null);
    }

    public static class ProjConvWithTos extends DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, Aggregate>> {

        @ProcessElement
        public void processElement(@Element KV<CompoundKey, Aggregate> kv, MultiOutputReceiver out) {
            CompoundKey convKey = kv.getKey();
            Aggregate a = kv.getValue();
            // the CompoundKeyData of a conversation key of type EXPORTER_INTERFACE_TOS_CONVERSATION
            // contains all fields that are required for a key of type EXPORTER_INTERFACE_TOS_APPLICATION
            // -> the key can be cast
            CompoundKey appKey = convKey.cast(CompoundKeyType.EXPORTER_INTERFACE_TOS_APPLICATION);
            out.get(BY_APP).output(KV.of(appKey, a.withHostname(null)));
            // the CompoundKeyData of a conversation key of type EXPORTER_INTERFACE_TOS_CONVERSATION
            // contains all fields that are required for a key of type EXPORTER_INTERFACE_TOS_HOST
            // -> the key can be cast
            CompoundKey hostKey1 = convKey.cast(CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST);
            out.get(BY_HOST).output(KV.of(hostKey1, a.withHostname(a.getHostname())));
            // the CompoundKeyData of a conversation key of type EXPORTER_INTERFACE_TOS_CONVERSATION
            // contains all fields that are required for a key of type EXPORTER_INTERFACE_TOS_HOST
            // and a second host address, namely the larger address of the conversation
            // -> use the larget address and construct a corresponding key
            CompoundKey hostKey2 = new CompoundKey(
                    CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST,
                    new CompoundKeyData.Builder(convKey.data).withAddress(convKey.data.largerAddress).build()
            );
            out.get(BY_HOST).output(KV.of(hostKey2, a.withHostname(a.getHostname2())));
        }

    }

    /**
     * Aggregates over parent keys.
     * <p>
     * The result collection is "total" collection, i.e. it is not capped by a topK transform.
     *
     * @param child A total collection that is keyed by subkeys.
     */
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
            public void processElement(ProcessContext c) {
                c.output(toFlowSummaryData(c.element()));
            }
        }));
        return new TotalAndSummary(parentTotal, summary);
    }

    /**
     * Aggregates the sums and topKs for the input collection and a projection of the input collection where the tos
     * (i.e. dscp) key dimension is ignored.
     *
     * @param groupedByKeyWithTos a total collection that is a multimap (i.e. the collection may contain several
     *                           entries with the same CompoundKey but different values)
     * @param typeWithoutTos a type that considers the same dimension as the entries in the input collection but ignores
     *                       the dscp field
     * @param k count for the topK calculation
     * @param includeKeyInTopK filters the entries that are considered in topK calculations
     */
    public static SumsAndTopKs aggregateSumsAndTopKs(
            String transformPrefix,
            PCollection<KV<CompoundKey, Aggregate>> groupedByKeyWithTos,
            CompoundKeyType typeWithoutTos,
            int k,
            SerializableFunction<CompoundKey, Boolean> includeKeyInTopK
    ) {
        SumAndTopK withTos = aggregateSumAndTopK(transformPrefix + "with_tos_", groupedByKeyWithTos, k, includeKeyInTopK);

        // multimap
        PCollection<KV<CompoundKey, Aggregate>> groupedByKeyWithoutTos =
                withTos.sum.apply(
                        transformPrefix + "group_without_tos_",
                        ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, KV<CompoundKey, Aggregate>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<CompoundKey, Aggregate> el = c.element();
                                c.output(KV.of(el.getKey().cast(typeWithoutTos), el.getValue()));
                            }
                        }));
        SumAndTopK withoutTos = aggregateSumAndTopK(transformPrefix + "without_tos_", groupedByKeyWithoutTos, k, includeKeyInTopK);

        return new SumsAndTopKs(withTos, withoutTos);
    }

    /**
     * Reduces the input multimap collection into a collection with unique keys and the summed aggregates and
     * calculates the topK entries of these sums when selected over their parent keys.
     *
     * @param groupedByKey a multimap that may contain several entries with the same key but different values
     * @param k count for the topK calculation
     * @param includeKeyInTopK filters the entries that are considered in topK calculations
     */
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
                    public void processElement(ProcessContext c) {
                        for (KV<CompoundKey, Aggregate> el : c.element()) {
                            FlowSummaryData flowSummary = toFlowSummaryData(el);
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
        // - all keys in the sum collection are unique (i.e. this is not a multimap)
        // - the sum collection is a total collection (i.e. it is not capped by a topK transform)
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
