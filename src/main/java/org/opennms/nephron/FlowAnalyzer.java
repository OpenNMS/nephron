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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.elastic.Context;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.query.NGFlowRepository;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.swrve.ratelimitedlogger.RateLimitedLog;

public class FlowAnalyzer {

    private static final Logger LOG = LoggerFactory.getLogger(FlowAnalyzer.class);

    private static final RateLimitedLog rateLimitedLog = RateLimitedLog
            .withRateLimit(LOG)
            .maxRate(5).every(java.time.Duration.ofSeconds(10))
            .build();

    /**
     * Dispatches a {@link FlowDocument} to all of the windows that overlap with the flow range.
     * @return transform
     */
    public static ParDo.SingleOutput<FlowDocument, FlowDocument> attachTimestamps() {
         return ParDo.of(new DoFn<FlowDocument, FlowDocument>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                final long windowSizeMs = DurationUtils.toMillis(c.getPipelineOptions().as(NephronOptions.class).getFixedWindowSize());

                final FlowDocument flow = c.element();
                // We want to dispatch the flow to all the windows it may be a part of
                // The flow ranges from [delta_switched, last_switched]

                long flowStart;
                if (flow.hasDeltaSwitched()) {
                    // FIXME: Delta-switch should always be populated, but it is not currently
                    flowStart = flow.getDeltaSwitched().getValue();
                } else {
                    flowStart = flow.getFirstSwitched().getValue();
                }
                long timestamp = flowStart - windowSizeMs;

                // If we're exactly on the window boundary, then don't go back
                if (timestamp % windowSizeMs == 0) {
                    timestamp += windowSizeMs;
                }
                while (timestamp <= flow.getLastSwitched().getValue()) {
                    if (timestamp <= c.timestamp().minus(Duration.standardMinutes(30)).getMillis()) {
                        // Caused by: java.lang.IllegalArgumentException: Cannot output with timestamp 1970-01-01T00:00:00.000Z. Output timestamps must be no earlier than the timestamp of the current input (2020-
                        //                            04-14T15:33:11.302Z) minus the allowed skew (30 minutes). See the DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.
                        //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.checkTimestamp(SimpleDoFnRunner.java:607)
                        //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.outputWithTimestamp(SimpleDoFnRunner.java:573)
                        //                    at org.opennms.nephron.FlowAnalyzer$1.processElement(FlowAnalyzer.java:96)
                        rateLimitedLog.warn("Skipping output for flow w/ start: {}, end: {}, target timestamp: {}, current input timestamp: {}. Full flow: {}",
                                Instant.ofEpochMilli(flowStart), Instant.ofEpochMilli(flow.getLastSwitched().getValue()), Instant.ofEpochMilli(timestamp), c.timestamp(),
                                flow);
                        timestamp += windowSizeMs;
                        continue;
                    }
                    c.outputWithTimestamp(flow, Instant.ofEpochMilli(timestamp));
                    timestamp += windowSizeMs;
                }
            }

            @Override
            public Duration getAllowedTimestampSkew() {
                // Max flow duration
                return Duration.standardMinutes(30);
            }
        });
    }

    public static Window<FlowDocument> toWindow(NephronOptions options) {
        return Window.<FlowDocument>into(FixedWindows.of(DurationUtils.toDuration(options.getFixedWindowSize())))
                // See https://beam.apache.org/documentation/programming-guide/#composite-triggers
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(10));
    }

    public static Window<FlowDocument> toWindow(String fixedWindowSize) {
        return Window.<FlowDocument>into(FixedWindows.of(DurationUtils.toDuration(fixedWindowSize)))
                // See https://beam.apache.org/documentation/programming-guide/#composite-triggers
                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(10));
    }

    public static void registerCoders(Pipeline p) {
        p.getCoderRegistry().registerCoderForClass(FlowDocument.class, new FlowDocumentProtobufCoder());
        p.getCoderRegistry().registerCoderForClass(TopKFlows.class, new JacksonJsonCoder<>(TopKFlows.class));
        p.getCoderRegistry().registerCoderForClass(FlowSummary.class, new JacksonJsonCoder<>(FlowSummary.class));
        p.getCoderRegistry().registerCoderForClass(Groupings.ExporterInterfaceApplicationKey.class, new Groupings.CompoundKeyCoder());
        p.getCoderRegistry().registerCoderForClass(Groupings.CompoundKey.class, new Groupings.CompoundKeyCoder());
    }

    public static Pipeline create(NephronOptions options) {
        Objects.requireNonNull(options);
        Pipeline p = Pipeline.create(options);
        registerCoders(p);

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // For testing only :)

        PCollection<FlowDocument> streamOfFlows = p.apply(KafkaIO.<String, FlowDocument>read()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic(options.getFlowSourceTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(FlowDocumentDeserializer.class)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withTimestampPolicyFactory((TimestampPolicyFactory<String, FlowDocument>) (tp, previousWatermark) -> {
                    // TODO: FIXME: Use delta-switched here instead?!
                    return new CustomTimestampPolicyWithLimitedDelay<>((rec) -> Instant.ofEpochMilli(rec.getKV().getValue().getFirstSwitched().getValue()),
                            Duration.standardMinutes(5), previousWatermark);
                })
                .withoutMetadata()
        )
                .apply(Values.create())
                .apply("add_missing_DeltaSwitched", ParDo.of(new DoFn<FlowDocument, FlowDocument>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                FlowDocument flow = c.element();
                                if (flow.hasDeltaSwitched()) {
                                    c.output(flow);
                                } else {
                                    c.output(FlowDocument.newBuilder(c.element())
                                            .setDeltaSwitched(flow.getFirstSwitched())
                                            .build());
                                }
                        }}));

        PCollection<FlowSummary> topKFlows = streamOfFlows.apply(new CalculateFlowStatistics(options.getFixedWindowSize(), options.getTopK()));
        topKFlows.apply(new FlowAnalyzer.WriteToElasticsearch(options.getElasticUrl(), options.getElasticIndex()));
        pushToKafka(options, topKFlows);
        return p;
    }

    public static class CalculateFlowStatistics extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummary>> {
        private final String fixedWindowSize;
        private final int topK;

        public CalculateFlowStatistics(String fixedWindowSize, int topK) {
            this.fixedWindowSize = Objects.requireNonNull(fixedWindowSize);
            this.topK = topK;
        }

        @Override
        public PCollection<FlowSummary> expand(PCollection<FlowDocument> input) {
            PCollection<FlowDocument> windowedStreamOfFlows = input.apply("attach_timestamp", attachTimestamps())
                    .apply("to_windows", toWindow(fixedWindowSize));

            PCollection<FlowSummary> totalBytesByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTotalBytesByExporterAndInterface",
                    new CalculateTotalBytes("CalculateTotalBytesByExporterAndInterface_", new Groupings.KeyByExporterInterface()));

            PCollection<FlowSummary> topKAppsByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTopAppsByExporterAndInterface",
                    new CalculateTopKGroups("CalculateTopAppsByExporterAndInterface_", topK, new Groupings.KeyByExporterInterfaceApplication()));

            PCollection<FlowSummary> topKHostsByExporterAndInterface = windowedStreamOfFlows.apply("CalculateTopHostsByExporterAndInterface",
                    new CalculateTopKGroups("CalculateTopHostsByExporterAndInterface_", topK, new Groupings.KeyByExporterInterfaceHost()));

            // Merge all the collections
            PCollectionList<FlowSummary> flowSummaries = PCollectionList.of(totalBytesByExporterAndInterface)
                    .and(topKAppsByExporterAndInterface)
                    .and(topKHostsByExporterAndInterface);
            return flowSummaries.apply(Flatten.pCollections());
        }
    }

    /**
     * Used to calculate the top K groups using the given grouping function.
     * Assumes that the input stream is windowed.
     */
    public static class CalculateTopKGroups extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummary>> {
        private final String transformPrefix;
        private final int topK;
        private final DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> groupingBy;

        public CalculateTopKGroups(String transformPrefix, int topK, DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> groupingBy) {
            this.transformPrefix = Objects.requireNonNull(transformPrefix);
            this.topK = topK;
            this.groupingBy = Objects.requireNonNull(groupingBy);
        }

        @Override
        public PCollection<FlowSummary> expand(PCollection<FlowDocument> input) {
            return input.apply(transformPrefix + "group_by_key", ParDo.of(groupingBy))
                    .apply(transformPrefix + "compute_bytes_in_window", ParDo.of(new ToWindowedBytes()))
                    .apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()))
                    .apply(transformPrefix + "group_by_outer_key", ParDo.of(new DoFn<KV<Groupings.CompoundKey, FlowBytes>, KV<Groupings.CompoundKey, KV<Groupings.CompoundKey, FlowBytes>>>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, IntervalWindow window) {
                            KV<Groupings.CompoundKey, FlowBytes> el = c.element();
                            c.output(KV.of(el.getKey().getOuterKey(), el));
                        }
                    }))
                    .apply(transformPrefix + "top_k_per_key", Top.perKey(topK, new ByteValueComparator()))
                    .apply(transformPrefix + "flatten", Values.create())
                    .apply(transformPrefix + "top_k_for_window", ParDo.of(new DoFn<List<KV<Groupings.CompoundKey, FlowBytes>>, FlowSummary>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, IntervalWindow window) {
                            int ranking = 1;
                            for (KV<Groupings.CompoundKey, FlowBytes> el : c.element()) {
                                FlowSummary flowSummary =  toFlowSummary(Context.TOPK, window, el);
                                flowSummary.setRanking(ranking++);
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
    public static class CalculateTotalBytes extends PTransform<PCollection<FlowDocument>, PCollection<FlowSummary>> {
        private final String transformPrefix;
        private final DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> groupingBy;

        public CalculateTotalBytes(String transformPrefix, DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> groupingBy) {
            this.transformPrefix = Objects.requireNonNull(transformPrefix);
            this.groupingBy = Objects.requireNonNull(groupingBy);
        }

        @Override
        public PCollection<FlowSummary> expand(PCollection<FlowDocument> input) {
            return input.apply(transformPrefix + "group_by_key", ParDo.of(groupingBy))
                    .apply(transformPrefix + "compute_bytes_in_window", ParDo.of(new ToWindowedBytes()))
                    .apply(transformPrefix + "sum_bytes_by_key", Combine.perKey(new SumBytes()))
                    .apply(transformPrefix + "total_bytes_for_window", ParDo.of(new DoFn<KV<Groupings.CompoundKey, FlowBytes>, FlowSummary>() {
                        @ProcessElement
                        public void processElement(ProcessContext c, IntervalWindow window) {
                            c.output(toFlowSummary(Context.TOTAL, window, c.element()));
                        }
                    }));
        }
    }

    public static class WriteToElasticsearch extends PTransform<PCollection<FlowSummary>, PDone> {
        private final String elasticUrl;
        private final String elasticIndex;

        public WriteToElasticsearch(String elasticUrl, String elasticIndex) {
            this.elasticUrl = Objects.requireNonNull(elasticUrl);
            this.elasticIndex = Objects.requireNonNull(elasticIndex);
        }

        @Override
        public PDone expand(PCollection<FlowSummary> input) {
            return input.apply("SerializeToJson", toTopKFlowJson())
                    .apply("WriteToElasticsearch", ElasticsearchIO.write().withConnectionConfiguration(
                            ElasticsearchIO.ConnectionConfiguration.create(
                                    new String[]{elasticUrl}, elasticIndex, "_doc"))
                            .withIndexFn(new ElasticsearchIO.Write.FieldValueExtractFn() {
                                @Override
                                public String apply(JsonNode input) {
                                    return NGFlowRepository.NETFLOW_AGG_INDEX_PREFIX + "2020";
                                }
                            }));
        }
    }

    private static ParDo.SingleOutput<FlowSummary, String> toTopKFlowJson() {
        return ParDo.of(new DoFn<FlowSummary, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws JsonProcessingException {
                final ObjectMapper mapper = new ObjectMapper();
                c.output(mapper.writeValueAsString(c.element()));
            }
        });
    }

    private static ParDo.SingleOutput<FlowSummary, String> toJson() {
        return ParDo.of(new DoFn<FlowSummary, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws JsonProcessingException {
                final ObjectMapper mapper = new ObjectMapper();
                c.output(mapper.writeValueAsString(c.element()));
            }
        });
    }

    private static void pushToKafka(NephronOptions options, PCollection<FlowSummary> topKFlowStream) {
        topKFlowStream
                .apply(toJson())
                .apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic("flows_processed")
                .withValueSerializer(StringSerializer.class)
                .values()
        );
    }

    private static Map<? extends Groupings.CompoundKey, FlowBytes> toMap(List<KV<? extends Groupings.CompoundKey, FlowBytes>> topKFlows) {
        return topKFlows
                .stream()
                .collect(
                        LinkedHashMap::new,
                        (map, item) -> map.put(item.getKey(), item.getValue()),
                        Map::putAll);
    }


    static class SumBytes extends Combine.BinaryCombineFn<FlowBytes> {
        @Override
        public FlowBytes apply(FlowBytes left, FlowBytes right) {
            return FlowBytes.sum(left, right);
        }
    }

    static class ExtractApplicationName extends DoFn<FlowDocument, KV<String, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            String applicationName = flow.getApplication();
            if (Strings.isNullOrEmpty(applicationName)) {
                applicationName = FlowSummary.UNKNOWN_APPLICATION_NAME_KEY;
            }
            c.output(KV.of(applicationName, flow));
        }
    }

    static class ExtractSrcAddr extends DoFn<FlowDocument, KV<String, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            c.output(KV.of(flow.getSrcAddress(), flow));
        }
    }


    static class ToWindowedBytesDead extends DoFn<KV<String, FlowDocument>, KV<String, FlowBytes>> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            final KV<String, FlowDocument> keyedFlow = c.element();
            final FlowDocument flow = keyedFlow.getValue();

            // The flow duration ranges [delta_switched, last_switched]
            long flowDurationMs = flow.getLastSwitched().getValue() - flow.getDeltaSwitched().getValue();
            if (flowDurationMs <= 0) {
                // 0 or negative duration, pass
                return;
            }

            // Determine the avg. rate of bytes per ms for the duration of the flow
            double bytesPerMs = flow.getNumBytes().getValue() / (double) flowDurationMs;

            // Now determine how many milliseconds the flow overlaps with the window bounds
            long flowWindowOverlapMs = Math.min(flow.getLastSwitched().getValue(), window.end().getMillis()) - Math.max(flow.getDeltaSwitched().getValue(), window.start().getMillis());
            if (flowWindowOverlapMs < 0) {
                // flow should not be in this windows! pass
                return;
            }

            double multiplier = flowWindowOverlapMs / (double) flowDurationMs;
            c.output(KV.of(keyedFlow.getKey(), new FlowBytes(keyedFlow.getValue(), multiplier)));
        }
    }

    static class ToWindowedBytes extends DoFn<KV<Groupings.CompoundKey, FlowDocument>, KV<Groupings.CompoundKey, FlowBytes>> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            final KV<? extends Groupings.CompoundKey, FlowDocument> keyedFlow = c.element();
            final FlowDocument flow = keyedFlow.getValue();

            // The flow duration ranges [delta_switched, last_switched]
            long flowDurationMs = flow.getLastSwitched().getValue() - flow.getDeltaSwitched().getValue();
            if (flowDurationMs <= 0) {
                // 0 or negative duration, pass
                return;
            }

            // Determine the avg. rate of bytes per ms for the duration of the flow
            double bytesPerMs = flow.getNumBytes().getValue() / (double) flowDurationMs;

            // Now determine how many milliseconds the flow overlaps with the window bounds
            long flowWindowOverlapMs = Math.min(flow.getLastSwitched().getValue(), window.end().getMillis()) - Math.max(flow.getDeltaSwitched().getValue(), window.start().getMillis());
            if (flowWindowOverlapMs < 0) {
                // flow should not be in this windows! pass
                return;
            }

            double multiplier = flowWindowOverlapMs / (double) flowDurationMs;
            c.output(KV.of(keyedFlow.getKey(), new FlowBytes(keyedFlow.getValue(), multiplier)));
        }
    }

    static class ByteValueComparator implements Comparator<KV<Groupings.CompoundKey, FlowBytes>>, Serializable {
        @Override
        public int compare(KV<Groupings.CompoundKey, FlowBytes> a, KV<Groupings.CompoundKey, FlowBytes> b) {
            return a.getValue().compareTo(b.getValue());
        }
    }

    public static class FlowDocumentProtobufCoder extends Coder<FlowDocument> {
        private final ByteArrayCoder delegate = ByteArrayCoder.of();

        @Override
        public void encode(FlowDocument value, OutputStream outStream) throws IOException {
            delegate.encode(value.toByteArray(), outStream);
        }

        @Override
        public FlowDocument decode(InputStream inStream) throws IOException {
            return FlowDocument.parseFrom(delegate.decode(inStream));
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
            // pass
        }
    }

    public static class JacksonJsonCoder<T> extends Coder<T> {

        private static final ObjectMapper mapper = new ObjectMapper();
        private static final StringUtf8Coder delegate = StringUtf8Coder.of();
        private final Class<T> clazz;

        public JacksonJsonCoder(Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public void encode(T value, OutputStream outStream) throws IOException {
            final String json = mapper.writeValueAsString(value);
            delegate.encode(json, outStream);
        }

        @Override
        public T decode(InputStream inStream) throws IOException {
            String json = delegate.decode(inStream);
            return mapper.readValue(json, clazz);
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() {
            // pass
        }
    }

    public static FlowSummary toFlowSummary(Context ctx, IntervalWindow window, KV<Groupings.CompoundKey, FlowBytes> el) {
        FlowSummary flowSummary = new FlowSummary();
        el.getKey().visit(new Groupings.FlowPopulatingVisitor(flowSummary));
        flowSummary.setContext(ctx);

        flowSummary.setRangeStartMs(window.start().getMillis());
        flowSummary.setRangeEndMs(window.end().getMillis());
        // Use the range end as the timestamp
        flowSummary.setTimestamp(flowSummary.getRangeEndMs());

        flowSummary.setBytesEgress(el.getValue().bytesOut);
        flowSummary.setBytesIngress(el.getValue().bytesIn);
        flowSummary.setBytesTotal(flowSummary.getBytesIngress() + flowSummary.getBytesEgress());
        return flowSummary;
    }
}
