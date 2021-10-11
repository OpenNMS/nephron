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
import java.util.function.Consumer;

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
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.nephron.coders.KafkaInputFlowDeserializer;
import org.opennms.nephron.cortex.CortexIo;
import org.opennms.nephron.util.PaneAccumulator;
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

        var aggregatedAndHostnames = calculateFlowStatistics(streamOfFlows, options);

        // Calculate the flow summary statistics
        PCollection<KV<CompoundKey, Aggregate>> flowSummaries = aggregatedAndHostnames.getLeft();

        flowSummaries = accumulateSummariesIfNecessary(options, flowSummaries);

        // optionally attach different kinds of sinks
        attachWriteToElastic(options, flowSummaries, aggregatedAndHostnames.getRight());
        attachWriteToKafka(options, flowSummaries, aggregatedAndHostnames.getRight());
        attachWriteToCortex(options, flowSummaries);

        return p;
    }

    public static PCollection<KV<CompoundKey, Aggregate>> accumulateSummariesIfNecessary(NephronOptions options, PCollection<KV<CompoundKey, Aggregate>> flowSummaries) {
        if (options.getSummaryAccumulationDelayMs() != 0) {
            return accumulateFlowSummaries(flowSummaries, Duration.millis(options.getSummaryAccumulationDelayMs()));
        } else {
            return flowSummaries;
        }
    }

    public static PCollection<KV<CompoundKey, Aggregate>> accumulateFlowSummaries(
            PCollection<KV<CompoundKey, Aggregate>> input,
            Duration accumulationDelay
    ) {
        var paneAccumulator = new PaneAccumulator<>(
                Aggregate::merge,
                accumulationDelay,
                new CompoundKey.CompoundKeyCoder(),
                new Aggregate.AggregateCoder()
        );
        return input.apply(paneAccumulator);
    }

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

    public static void attachWriteToElastic(
            NephronOptions options,
            PCollection<KV<CompoundKey, Aggregate>> flowSummaries,
            PCollectionView<Map<String, String>> hostnamesView
    ) {
        if (!Strings.isNullOrEmpty(options.getElasticUrl())) {
            flowSummaries.apply(new WriteToElasticsearch(options, hostnamesView));
        }
    }

    public static void attachWriteToKafka(
            NephronOptions options,
            PCollection<KV<CompoundKey, Aggregate>> flowSummaries,
            PCollectionView<Map<String, String>> hostnamesView
    ) {
        if (!Strings.isNullOrEmpty(options.getFlowDestTopic())) {
            var kafkaProducerConfig = loadKafkaClientProperties(options);
            flowSummaries.apply(new WriteToKafka(options.getBootstrapServers(), options.getFlowDestTopic(), kafkaProducerConfig, hostnamesView));
        }
    }

    public static void attachWriteToCortex(NephronOptions options, PCollection<KV<CompoundKey, Aggregate>> flowSummaries) {
        attachWriteToCortex(options, flowSummaries, cw -> {});
    }

    /**
     * @param additionalConfig Allows for additional configuration of the Cortex writer; used by the benchmark
     *                         application for adding a label that differentiates benchmark runs.
     */
    public static void attachWriteToCortex(
            NephronOptions options,
            PCollection<KV<CompoundKey, Aggregate>> flowSummaries,
            Consumer<CortexIo.Write<CompoundKey, Aggregate>> additionalConfig
    ) {
        if (cortexOutputEnabled(options)) {
            CortexIo.Write<CompoundKey, Aggregate> cortexWrite;
            if (options.getCortexAccumulationDelayMs() != 0) {
                cortexWrite = CortexIo.of(
                        options.getCortexWriteUrl(),
                        Pipeline::cortexOutput,
                        new CompoundKey.CompoundKeyCoder(),
                        new Aggregate.AggregateCoder(),
                        Aggregate::merge,
                        Duration.millis(options.getCortexAccumulationDelayMs())
                );
            } else {
                cortexWrite = CortexIo.of(options.getCortexWriteUrl(), Pipeline::cortexOutput);
            }
            cortexWrite
                    .withMaxBatchSize(options.getCortexMaxBatchSize())
                    .withMaxBatchBytes(options.getCortexMaxBatchBytes());
            additionalConfig.accept(cortexWrite);
            if (!Strings.isNullOrEmpty(options.getCortexOrgId())) {
                cortexWrite.withOrgId(options.getCortexOrgId());
            }
            flowSummaries
                    .apply(Filter.by(includeInCortexOutput(options)))
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


    private static SerializableFunction<KV<CompoundKey, Aggregate>, Boolean> includeInCortexOutput(NephronOptions options) {
        if (StringUtils.isNoneBlank(options.getCortexConsideredHosts())) {
            var ipValue = validateIpAddress(options.getCortexConsideredHosts());
            return fsd -> {
                switch (fsd.getKey().type) {
                    case EXPORTER_INTERFACE_HOST:
                    case EXPORTER_INTERFACE_TOS_HOST:
                        return ipValue.isInRange(fsd.getKey().data.address);
                    case EXPORTER_INTERFACE_CONVERSATION:
                    case EXPORTER_INTERFACE_TOS_CONVERSATION:
                        return false;
                    default:
                        return true;
                }
            };
        } else {
            return fsd -> {
                switch (fsd.getKey().type) {
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
            Aggregate agg,
            Instant eventTimestamp,
            int index,
            TimeSeriesBuilder builder
    ) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("cortex output - eventTimestamp: {}; keyType: {}; key: {}; index: {}; in: {}; out: {}; total: {}",
                    eventTimestamp, key.type, key, index, agg.getBytesIn(), agg.getBytesOut(), agg.getBytesIn() + agg.getBytesOut());
        }
        doCortexOutput(key, eventTimestamp, index, "in", agg.getBytesIn(), builder);
        builder.nextSeries();
        doCortexOutput(key, eventTimestamp, index, "out", agg.getBytesOut(), builder);
    }

    private static void doCortexOutput(
            CompoundKey key,
            Instant eventTimestamp,
            int paneId,
            String direction,
            long bytes,
            TimeSeriesBuilder builder
    ) {
        builder.addLabel("pane", paneId);
        builder.addLabel("direction", direction);
        builder.addSample(eventTimestamp.getMillis(), bytes);
        key.populate(builder);
    }

    public static void registerCoders(org.apache.beam.sdk.Pipeline p) {
        final CoderRegistry coderRegistry = p.getCoderRegistry();
        coderRegistry.registerCoderForClass(FlowDocument.class, new FlowDocumentProtobufCoder());
        coderRegistry.registerCoderForClass(CompoundKey.class, new CompoundKey.CompoundKeyCoder());
        coderRegistry.registerCoderForClass(Aggregate.class, new Aggregate.AggregateCoder());
    }

    public static Pair<PCollection<KV<CompoundKey, Aggregate>>, PCollectionView<Map<String, String>>> calculateFlowStatistics(
            PCollection<FlowDocument> input,
            NephronOptions options
    ) {
        return calculateFlowStatistics(input, options.getTopK(), new WindowedFlows(options));
    }

    public static Pair<PCollection<KV<CompoundKey, Aggregate>>, PCollectionView<Map<String, String>>> calculateFlowStatistics(
            PCollection<FlowDocument> input,
            int topK,
            PTransform<PCollection<FlowDocument>, PCollection<FlowDocument>> windowing
    ) {
        PCollection<FlowDocument> windowedStreamOfFlows = input.apply("WindowedFlows", windowing);

        var byConvAndHostnamesView = keyByConfWithTos(windowedStreamOfFlows);

        PCollectionView<Map<String, String>> hostnamesView = byConvAndHostnamesView.getRight();

        PCollection<KV<CompoundKey, Aggregate>> keyedByConvWithTos = byConvAndHostnamesView.getLeft();

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
        PCollectionList<KV<CompoundKey, Aggregate>> flowSummaries = PCollectionList.of(itf.summary)
                .and(tos.summary)
                .and(app.withTos.topK)
                .and(app.withoutTos.topK)
                .and(host.withTos.topK)
                .and(host.withoutTos.topK)
                .and(conv.withTos.topK)
                .and(conv.withoutTos.topK);

        return Pair.of(flowSummaries.apply(Flatten.pCollections()), hostnamesView);
    }

    private static TupleTag<KV<String, String>> HOSTNAME = new TupleTag<>(){};
    private static TupleTag<KV<CompoundKey, Aggregate>> BY_CONV = new TupleTag<>(){};
    private static TupleTag<KV<CompoundKey, Aggregate>> BY_HOST = new TupleTag<>(){};
    private static TupleTag<KV<CompoundKey, Aggregate>> BY_APP = new TupleTag<>(){};

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

    public static class WriteToElasticsearch extends PTransform<PCollection<KV<CompoundKey, Aggregate>>, PDone> {
        private final String elasticIndex;
        private final IndexStrategy indexStrategy;
        private final ElasticsearchIO.ConnectionConfiguration esConfig;

        private final Counter flowsToEs = Metrics.counter("flows", "to_es");
        // a distribution would be more interesting for flowsToEsDrift
        // -> Unfortunately histograms are not supported Beam/Flink/Prometheus
        //    (cf. https://issues.apache.org/jira/browse/BEAM-10928)
        // -> use a gauge instead
        private final Gauge flowsToEsDrift = Metrics.gauge("flows", "to_es_drift");

        private final PCollectionView<Map<String, String>> hostnamesView;

        private int elasticRetryCount;
        private long elasticRetryDuration;

        public WriteToElasticsearch(String elasticUrl, String elasticUser, String elasticPassword, String elasticIndex,
                                    IndexStrategy indexStrategy, int elasticConnectTimeout, int elasticSocketTimeout,
                                    int elasticRetryCount, long elasticRetryDuration,
                                    PCollectionView<Map<String, String>> hostnamesView
                                    ) {
            Objects.requireNonNull(elasticUrl);
            this.elasticIndex = Objects.requireNonNull(elasticIndex);
            this.indexStrategy = Objects.requireNonNull(indexStrategy);
            this.hostnamesView = Objects.requireNonNull(hostnamesView);

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

        public WriteToElasticsearch(NephronOptions options, PCollectionView<Map<String, String>> hostnamesView) {
            this(options.getElasticUrl(), options.getElasticUser(), options.getElasticPassword(),
                    options.getElasticFlowIndex(), options.getElasticIndexStrategy(),
                    options.getElasticConnectTimeout(), options.getElasticSocketTimeout(),
                    options.getElasticRetryCount(), options.getElasticRetryDuration(),
                    hostnamesView);
        }

        @Override
        public PDone expand(PCollection<KV<CompoundKey, Aggregate>> input) {
            return input.apply("SerializeToJson", keyAndAggregateToJson(hostnamesView))
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

    public static class WriteToKafka extends PTransform<PCollection<KV<CompoundKey, Aggregate>>, PDone> {
        private final String bootstrapServers;
        private final String topic;
        private final Map<String, Object> kafkaProducerConfig;
        private final PCollectionView<Map<String, String>> hostnamesView;

        public WriteToKafka(
                String bootstrapServers,
                String topic,
                Map<String, Object> kafkaProducerConfig,
                PCollectionView<Map<String, String>> hostnamesView
        ) {
            this.bootstrapServers = Objects.requireNonNull(bootstrapServers);
            this.topic = Objects.requireNonNull(topic);
            this.kafkaProducerConfig = kafkaProducerConfig;
            this.hostnamesView = hostnamesView;
        }

        @Override
        public PDone expand(PCollection<KV<CompoundKey, Aggregate>> input) {
            return input.apply(keyAndAggregateToJson(hostnamesView))
                    .apply(KafkaIO.<Void, String>write()
                            .withProducerConfigUpdates(kafkaProducerConfig)
                            .withBootstrapServers(bootstrapServers) // Order matters: bootstrap server overwrite producer properties
                            .withTopic(topic)
                            .withValueSerializer(StringSerializer.class)
                            .values()
                    );
        }
    }

    private static ParDo.SingleOutput<KV<CompoundKey, Aggregate>, String> keyAndAggregateToJson(PCollectionView<Map<String, String>> hostnamesView) {
        return ParDo.of(new DoFn<KV<CompoundKey, Aggregate>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c, IntervalWindow window) throws JsonProcessingException {
                var hostnames = c.sideInput(hostnamesView);
                FlowSummary flowSummary = toFlowSummary(c.element(), window, hostnames);
                c.output(MAPPER.writeValueAsString(flowSummary));
            }
        }).withSideInputs(hostnamesView);
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

    public static FlowSummary toFlowSummary(KV<CompoundKey, Aggregate> fsd, IntervalWindow window, Map<String, String> hostnames) {
        FlowSummary flowSummary = new FlowSummary();
        fsd.getKey().populate(flowSummary);
        flowSummary.setAggregationType(fsd.getKey().type.isTotalNotTopK() ? AggregationType.TOTAL : AggregationType.TOPK);

        flowSummary.setRangeStartMs(window.start().getMillis());
        flowSummary.setRangeEndMs(window.end().getMillis());
        // Use the range end as the timestamp
        flowSummary.setTimestamp(flowSummary.getRangeEndMs());

        flowSummary.setBytesEgress(fsd.getValue().getBytesOut());
        flowSummary.setBytesIngress(fsd.getValue().getBytesIn());
        flowSummary.setBytesTotal(flowSummary.getBytesIngress() + flowSummary.getBytesEgress());
        flowSummary.setCongestionEncountered(fsd.getValue().isCongestionEncountered());
        flowSummary.setNonEcnCapableTransport(fsd.getValue().isNonEcnCapableTransport());

        if (fsd.getKey().getType() == CompoundKeyType.EXPORTER_INTERFACE_HOST || fsd.getKey().getType() == CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST) {
            flowSummary.setHostName(Strings.emptyToNull(hostnames.get(fsd.getKey().data.address)));
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

    private static final Counter flowsWithMissingFields = Metrics.counter(Pipeline.class, "flowsWithMissingFields");
    private static final Counter flowsInWindow = Metrics.counter("flows", "in_window");

    public static Pair<PCollection<KV<CompoundKey, Aggregate>>, PCollectionView<Map<String, String>>> keyByConfWithTos(
        PCollection<FlowDocument> input
    ) {
        var tuple = input
                .apply("key_by_conv", ParDo.of(new KeyByConvWithTos()).withOutputTags(BY_CONV, TupleTagList.of(HOSTNAME)));

        PCollectionView<Map<String, String>> hostnamesView = tuple.get(HOSTNAME)
                .apply(Combine.perKey((s1, s2) -> s1))
                .apply(View.asMap());

        return Pair.of(tuple.get(BY_CONV), hostnamesView);
    }

    /**
     * Maps flow documents into pairs of compound keys (of type EXPORTER_INTERFACE_TOS_CONVERSATION) and aggregates.
     * <p>
     * {@link Aggregate} values are determined for window based on the intersection of flows with their windows.
     */
    private static class KeyByConvWithTos extends DoFn<FlowDocument, KV<CompoundKey, Aggregate>> {

        @ProcessElement
        public void processElement(@Element FlowDocument flow, IntervalWindow window, MultiOutputReceiver out) {
            try {
                CompoundKey key = CompoundKeyType.EXPORTER_INTERFACE_TOS_CONVERSATION.create(flow);
                Aggregate aggregate = aggregatize(window, flow);
                flowsInWindow.inc();
                out.get(BY_CONV).output(KV.of(key, aggregate));
                if (!Strings.isNullOrEmpty(flow.getSrcHostname())) {
                    out.get(HOSTNAME).output(KV.of(flow.getSrcAddress(), flow.getSrcHostname()));
                }
                if (!Strings.isNullOrEmpty(flow.getDstHostname())) {
                    out.get(HOSTNAME).output(KV.of(flow.getDstAddress(), flow.getDstHostname()));
                }
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

    public static Aggregate aggregatize(final IntervalWindow window, final FlowDocument flow) {
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
               new Aggregate(bytes, 0, flow.hasEcn() ? flow.getEcn().getValue() : null) :
               new Aggregate(0, bytes, flow.hasEcn() ? flow.getEcn().getValue() : null);
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
            out.get(BY_APP).output(KV.of(appKey, a));
            // the CompoundKeyData of a conversation key of type EXPORTER_INTERFACE_TOS_CONVERSATION
            // contains all fields that are required for a key of type EXPORTER_INTERFACE_TOS_HOST
            // -> the key can be cast
            CompoundKey hostKey1 = convKey.cast(CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST);
            out.get(BY_HOST).output(KV.of(hostKey1, a));
            // the CompoundKeyData of a conversation key of type EXPORTER_INTERFACE_TOS_CONVERSATION
            // contains all fields that are required for a key of type EXPORTER_INTERFACE_TOS_HOST
            // and a second host address, namely the larger address of the conversation
            // -> use the larger address and construct a corresponding key
            CompoundKey hostKey2 = new CompoundKey(
                    CompoundKeyType.EXPORTER_INTERFACE_TOS_HOST,
                    new CompoundKeyData.Builder(convKey.data).withAddress(convKey.data.largerAddress).build()
            );
            out.get(BY_HOST).output(KV.of(hostKey2, a));
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

        return new TotalAndSummary(parentTotal, parentTotal);
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

        PCollection<KV<CompoundKey, Aggregate>> topK = sum
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
                .apply(transformPrefix + "top_k_summary", ParDo.of(new DoFn<List<KV<CompoundKey, Aggregate>>, KV<CompoundKey, Aggregate>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (KV<CompoundKey, Aggregate> el : c.element()) {
                            c.output(el);
                        }
                    }
                }));
        return new SumAndTopK(sum, topK);
    }

    public static class TotalAndSummary {
        public final PCollection<KV<CompoundKey, Aggregate>> total;
        public final PCollection<KV<CompoundKey, Aggregate>> summary;
        public TotalAndSummary(PCollection<KV<CompoundKey, Aggregate>> total, PCollection<KV<CompoundKey, Aggregate>> summary) {
            this.total = total;
            this.summary = summary;
        }
    }

    public static class SumAndTopK {
        // - all keys in the sum collection are unique (i.e. this is not a multimap)
        // - the sum collection is a total collection (i.e. it is not capped by a topK transform)
        public final PCollection<KV<CompoundKey, Aggregate>> sum;
        public final PCollection<KV<CompoundKey, Aggregate>> topK;

        public SumAndTopK(PCollection<KV<CompoundKey, Aggregate>> sum, PCollection<KV<CompoundKey, Aggregate>> topK) {
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
