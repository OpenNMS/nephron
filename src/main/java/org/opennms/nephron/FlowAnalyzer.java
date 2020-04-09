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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.flows.model.Direction;
import org.opennms.flows.model.FlowDocument;

import com.google.gson.Gson;

public class FlowAnalyzer {
    private static final Gson gson = new Gson();

    public static void main(String[] args) {
        String bootstrapServers = args[0];
        Pipeline pipeline = getPipeline(PipeOptions.builder()
                .withBootstrapServers(bootstrapServers)
                .build());
        pipeline.run().waitUntilFinish();
    }

    public static Pipeline getPipeline(PipeOptions pipeOptions) {
        Objects.requireNonNull(pipeOptions);
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        p.getCoderRegistry().registerCoderForClass(FlowDocument.class, new FlowDocumentGsonCoder());

        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "myapp");
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // For testing only :)

        // This example reads a public data set consisting of the complete works of Shakespeare.
        PCollection<FlowDocument> windowedStreamOfFlows = p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(pipeOptions.getBootstrapServers())
                .withTopic("flows")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withConsumerConfigUpdates(kafkaConsumerConfig)
                .withoutMetadata()
        )
                .apply(Values.create())
                .apply(ParDo.of(new DoFn<String, FlowDocument>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String json = c.element();
                        FlowDocument flow = gson.fromJson(json, FlowDocument.class);
                        // We want to dispatch the flow to all the windows it may be a part of
                        // The flow ranges from [delta_switched, last_switched]
                        final long windowSize = pipeOptions.getFixedWindowSize().getMillis();
                        long timestamp = flow.getDeltaSwitched() - windowSize;
                        while (timestamp <= flow.getLastSwitched()) {
                            c.outputWithTimestamp(flow, Instant.ofEpochMilli(timestamp));
                            timestamp += windowSize;
                        }
                    }

                    @Override
                    public Duration getAllowedTimestampSkew() {
                        return Duration.standardHours(4);
                    }
                }))
                .apply(Window.<FlowDocument>into(FixedWindows.of(pipeOptions.getFixedWindowSize()))
                        // See https://beam.apache.org/documentation/programming-guide/#composite-triggers
                        .triggering(AfterWatermark
                                .pastEndOfWindow()
                                .withLateFirings(AfterProcessingTime
                                        .pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardMinutes(10))))
                        .discardingFiredPanes() // FIXME: Not sure what this means :)
                        .withAllowedLateness(Duration.standardHours(4)));


        windowedStreamOfFlows.apply(ParDo.of(new ExtractApplicationName()))
                .apply(ParDo.of(new ToWindowedBytes(pipeOptions.getFixedWindowSize())))
                .apply(Combine.perKey(new SumBytes()))
                .apply(Top.of(pipeOptions.getTopN(), new ByteValueComparator()).withoutDefaults())
                .apply(ParDo.of(new DoFn<List<KV<String, FlowBytes>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c, BoundedWindow boundedWindow) {
                        c.output(gson.toJson(c.element()));
                    }
                }))
                .apply(KafkaIO.<Void, String>write()
                        .withBootstrapServers(pipeOptions.getBootstrapServers())
                        .withTopic("flows_processed")
                        .withValueSerializer(StringSerializer.class)
                        .values()
                );

        windowedStreamOfFlows.apply(ParDo.of(new ExtractSrcAddr()))
                .apply(ParDo.of(new ToWindowedBytes(pipeOptions.getFixedWindowSize())))
                .apply(Combine.perKey(new SumBytes()))
                .apply(Top.of(pipeOptions.getTopN(), new ByteValueComparator()).withoutDefaults())
                .apply(ParDo.of(new DoFn<List<KV<String, FlowBytes>>, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(gson.toJson(c.element()));
                    }
                }))
                .apply(KafkaIO.<Void, String>write()
                        .withBootstrapServers(pipeOptions.getBootstrapServers())
                        .withTopic("flows_processed")
                        .withValueSerializer(StringSerializer.class)
                        .values()
                );
        return p;
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
            c.output(KV.of(flow.getSrcAddr(), flow));
        }
    }

    private static class FlowBytes implements Serializable, Comparable<FlowBytes> {
        final long bytesIn;
        final long bytesOut;

        public FlowBytes(long bytesIn, long bytesOut) {
            this.bytesIn = bytesIn;
            this.bytesOut = bytesOut;
        }

        public FlowBytes(FlowDocument flow, double multiplier) {
            if (Direction.INGRESS.equals(flow.getDirection())) {
                bytesIn =  (long)(flow.getBytes() * multiplier);
                bytesOut = 0;
            } else {
                bytesIn = 0;
                bytesOut = (long)(flow.getBytes() * multiplier);
            }
        }

        public static FlowBytes sum(FlowBytes a, FlowBytes b) {
            return new FlowBytes(a.bytesIn + b.bytesIn, a.bytesOut + b.bytesOut);
        }

        @Override
        public int compareTo(FlowAnalyzer.FlowBytes other) {
            return Long.compare(bytesIn + bytesOut, other.bytesIn + other.bytesOut);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FlowBytes)) return false;
            FlowBytes flowBytes = (FlowBytes) o;
            return bytesIn == flowBytes.bytesIn &&
                    bytesOut == flowBytes.bytesOut;
        }

        @Override
        public int hashCode() {
            return Objects.hash(bytesIn, bytesOut);
        }
    }

    static class ToWindowedBytes extends DoFn<KV<String, FlowDocument>, KV<String, FlowBytes>> {
        private final long windowSizeMs;

        public ToWindowedBytes(Duration fixedWindowSize) {
            this.windowSizeMs = fixedWindowSize.getMillis();
        }

        @ProcessElement
        public void processElement(ProcessContext c, BoundedWindow window) {
            final KV<String, FlowDocument> keyedFlow = c.element();
            final FlowDocument flow = keyedFlow.getValue();

            // The flow duration ranges [delta_switched, last_switched]
            long flowDurationMs = flow.getLastSwitched() - flow.getDeltaSwitched();
            if (flowDurationMs <= 0) {
                // 0 or negative duration, pass
                return;
            }

            // Determine the avg. rate of bytes per ms for the duration of the flow
            double bytesPerMs = flow.getBytes() / (double)flowDurationMs;

            // Now determine how many milliseconds the flow overlaps with the window bounds
            long endOfWindowMs = window.maxTimestamp().getMillis();
            long startOfWindowMs = endOfWindowMs - windowSizeMs;
            long flowWindowOverlapMs = Math.min(flow.getLastSwitched(), endOfWindowMs) - Math.max(flow.getDeltaSwitched(), startOfWindowMs);
            if (flowWindowOverlapMs < 0) {
                // flow should not be in this windows! pass
                return;
            }

            double multiplier = flowWindowOverlapMs / (double)flowDurationMs;
            c.output(KV.of(keyedFlow.getKey(), new FlowBytes(keyedFlow.getValue(), multiplier)));
        }
    }

    static class ByteValueComparator implements Comparator<KV<String, FlowBytes>>, Serializable {
        @Override
        public int compare(KV<String, FlowBytes> a, KV<String, FlowBytes> b) {
            return a.getValue().compareTo(b.getValue());
        }
    }

    public static class FlowDocumentGsonCoder extends Coder<FlowDocument> {
        @Override
        public void encode(FlowDocument value, OutputStream outStream) throws IOException {
            final String json =  gson.toJson(value);
            outStream.write(json.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public FlowDocument decode(InputStream inStream) throws IOException {
            String json = CharStreams.toString(new InputStreamReader(
                    inStream, StandardCharsets.UTF_8));
            return gson.fromJson(json, FlowDocument.class);
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


