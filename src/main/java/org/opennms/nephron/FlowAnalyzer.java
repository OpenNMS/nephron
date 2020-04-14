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
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.protobuf.InvalidProtocolBufferException;

public class FlowAnalyzer {
    private static final Gson gson = new GsonBuilder()
            .setLenient()
            .create();

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
                long timestamp = flow.getDeltaSwitched().getValue() - windowSizeMs;

                // If we're exactly on the window boundary, then don't go back
                if (timestamp % windowSizeMs == 0) {
                    timestamp += windowSizeMs;
                }
                while (timestamp <= flow.getLastSwitched().getValue()) {
                    c.outputWithTimestamp(flow, Instant.ofEpochMilli(timestamp));
                    timestamp += windowSizeMs;
                }
            }

            @Override
            public Duration getAllowedTimestampSkew() {
                // TODO: Make configurable
                return Duration.standardHours(4);
            }
        });
    }

    public static Window<FlowDocument> toWindow(NephronOptions options) {
        return Window.<FlowDocument>into(FixedWindows.of(DurationUtils.toDuration(options.getFixedWindowSize())))
                // See https://beam.apache.org/documentation/programming-guide/#composite-triggers
                .triggering(AfterWatermark
                        .pastEndOfWindow()
                        .withLateFirings(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardMinutes(10))))
                .discardingFiredPanes() // FIXME: Not sure what this means :)
                .withAllowedLateness(Duration.standardHours(4)); // FIXME: Make this configurable
    }

    public static PCollection<TopKFlows> doTopKFlows(PCollection<FlowDocument> input, NephronOptions options) {
        PCollection<FlowDocument> windowedStreamOfFlows = input.apply("attach_timestamp", attachTimestamps())
                .apply("to_windows", toWindow(options));

        PCollection<TopKFlows> topKAppsFlows = windowedStreamOfFlows.apply("key_by_app", ParDo.of(new ExtractApplicationName()))
                .apply("compute_bytes_in_window", ParDo.of(new ToWindowedBytes()))
                .apply("sum_bytes_by_key", Combine.perKey(new SumBytes()))
                .apply("top_k_per_key", Top.of(options.getTopK(), new ByteValueComparator()).withoutDefaults())
                .apply("top_k_summary_for_window", ParDo.of(new DoFn<List<KV<String, FlowBytes>>, TopKFlows>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow boundedWindow) {
                        final long windowSizeMs = DurationUtils.toMillis(c.getPipelineOptions().as(NephronOptions.class).getFixedWindowSize());

                        final TopKFlows topK = new TopKFlows();
                        topK.setContext("apps");
                        topK.setWindowMaxMs(boundedWindow.maxTimestamp().getMillis());
                        topK.setWindowMinMs(topK.getWindowMaxMs() - windowSizeMs);
                        topK.setFlows(toMap(c.element()));
                        c.output(topK);
                    }
                }));

        return topKAppsFlows;
    }

    public static void registerCoders(Pipeline p) {
        p.getCoderRegistry().registerCoderForClass(FlowDocument.class, new FlowDocumentProtobufCoder());
        p.getCoderRegistry().registerCoderForClass(TopKFlows.class, new TopKFlowsGsonCoder());
    }

    public static Pipeline create(NephronOptions options) {
        Objects.requireNonNull(options);
        Pipeline p = Pipeline.create(options);
        registerCoders(p);

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // For testing only :)

        PCollection<FlowDocument> windowedStreamOfFlows = p.apply(KafkaIO.<String, byte[]>read()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic(options.getFlowSourceTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withoutMetadata()
        )
                .apply(Values.create())
                .apply(ParDo.of(new DoFn<byte[], FlowDocument>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        try {
                            c.output(FlowDocument.parseFrom(c.element()));
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }))
                .apply(attachTimestamps())
                .apply(toWindow(options));

        PCollection<TopKFlows> topKAppsFlows = windowedStreamOfFlows.apply(ParDo.of(new ExtractApplicationName()))
                .apply(ParDo.of(new ToWindowedBytes()))
                .apply(Combine.perKey(new SumBytes()))
                .apply(Top.of(options.getTopK(), new ByteValueComparator()).withoutDefaults())
                .apply(ParDo.of(new DoFn<List<KV<String, FlowBytes>>, TopKFlows>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow boundedWindow) {
                        final long windowSizeMs = DurationUtils.toMillis(c.getPipelineOptions().as(NephronOptions.class).getFixedWindowSize());

                        final TopKFlows topK = new TopKFlows();
                        topK.setContext("apps");
                        topK.setWindowMaxMs(boundedWindow.maxTimestamp().getMillis());
                        topK.setWindowMinMs(topK.getWindowMaxMs() - windowSizeMs);
                        topK.setFlows(toMap(c.element()));
                        c.output(topK);
                    }
                }));
        pushToKafka(options, topKAppsFlows);

        PCollection<TopKFlows> topKSrcFlows = windowedStreamOfFlows.apply(ParDo.of(new ExtractSrcAddr()))
                .apply(ParDo.of(new ToWindowedBytes()))
                .apply(Combine.perKey(new SumBytes()))
                .apply(Top.of(options.getTopK(), new ByteValueComparator()).withoutDefaults())
                .apply(ParDo.of(new DoFn<List<KV<String, FlowBytes>>, TopKFlows>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow boundedWindow) {
                        final long windowSizeMs = DurationUtils.toMillis(c.getPipelineOptions().as(NephronOptions.class).getFixedWindowSize());

                        final TopKFlows topK = new TopKFlows();
                        topK.setContext("src-addr");
                        topK.setWindowMaxMs(boundedWindow.maxTimestamp().getMillis());
                        topK.setWindowMinMs(topK.getWindowMaxMs() - windowSizeMs);
                        topK.setFlows(toMap(c.element()));
                        c.output(topK);
                    }
                }));
        pushToKafka(options, topKSrcFlows);

        return p;
    }

    private static void pushToKafka(NephronOptions options, PCollection<TopKFlows> topKFlowStream) {
        topKFlowStream
                .apply(ParDo.of(new DoFn<TopKFlows, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(gson.toJson(c.element()));
                    }
                }))
                .apply(KafkaIO.<Void, String>write()
                .withBootstrapServers(options.getBootstrapServers())
                .withTopic("flows_processed")
                .withValueSerializer(StringSerializer.class)
                .values()
        );
    }

    private static Map<String, FlowBytes> toMap(List<KV<String, FlowBytes>> topKFlows) {
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
            c.output(KV.of(flow.getApplication(), flow));
        }
    }

    static class ExtractSrcAddr extends DoFn<FlowDocument, KV<String, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            c.output(KV.of(flow.getSrcAddress(), flow));
        }
    }


    static class ToWindowedBytes extends DoFn<KV<String, FlowDocument>, KV<String, FlowBytes>> {
        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            final long windowSizeMs = DurationUtils.toMillis(c.getPipelineOptions().as(NephronOptions.class).getFixedWindowSize());
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
            long endOfWindowMs = window.maxTimestamp().getMillis();
            long startOfWindowMs = endOfWindowMs - windowSizeMs;
            long flowWindowOverlapMs = Math.min(flow.getLastSwitched().getValue(), endOfWindowMs) - Math.max(flow.getDeltaSwitched().getValue(), startOfWindowMs);
            if (flowWindowOverlapMs < 0) {
                // flow should not be in this windows! pass
                return;
            }

            double multiplier = flowWindowOverlapMs / (double) flowDurationMs;
            c.output(KV.of(keyedFlow.getKey(), new FlowBytes(keyedFlow.getValue(), multiplier)));
        }
    }

    static class ByteValueComparator implements Comparator<KV<String, FlowBytes>>, Serializable {
        @Override
        public int compare(KV<String, FlowBytes> a, KV<String, FlowBytes> b) {
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

    public static class TopKFlowsGsonCoder extends Coder<TopKFlows> {

        private final StringUtf8Coder delegate = StringUtf8Coder.of();

        @Override
        public void encode(TopKFlows value, OutputStream outStream) throws IOException {
            final String json = gson.toJson(value);
            delegate.encode(json, outStream);
        }

        @Override
        public TopKFlows decode(InputStream inStream) throws IOException {
            String json = delegate.decode(inStream);
            try {
                return gson.fromJson(json, TopKFlows.class);
            } catch (JsonSyntaxException e) {
                throw new RuntimeException("Invalid JSON: " + json, e);
            }
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

}

