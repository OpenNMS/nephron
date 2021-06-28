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

package org.opennms.nephron.cortex;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import prometheus.PrometheusRemote;
import prometheus.PrometheusTypes;

@Experimental(Experimental.Kind.SOURCE_SINK)
public class CortexIo {

    // Label name indicating the metric name of a timeseries.
    public static final String METRIC_NAME_LABEL = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(CortexIo.class);

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    private static final String X_SCOPE_ORG_ID_HEADER = "X-Scope-OrgID";

    public static <T> Write<T> write(
            String writeUrl,
            SerializableFunction<Write, WriteFn<T>> createWriteFn
    ) {
        return new Write<T>(writeUrl, createWriteFn);
    }

    public static class Write<T> extends PTransform<PCollection<T>, PDone> {

        private final String writeUrl;
        private final SerializableFunction<Write, WriteFn<T>> createWriteFn;

        private String orgId;

        // maximum number of time series samples in a single batch
        private long maxBatchSize = 10000;
        // threshold for the sum of the number of bytes of time series samples in a single batch
        // -> the complete message will be some bytes larger
        private long maxBatchBytes = 512 * 1024;

        private int maxConcurrentHttpConnections = 5;

        private long readTimeoutMs = 10000;
        private long writeTimeoutMs = 10000;

        private Map<String, String> fixedLabels = new HashMap<>();

        public Write(
                String writeUrl,
                SerializableFunction<Write, WriteFn<T>> createWriteFn
        ) {
            super("CortexWrite");
            this.writeUrl = writeUrl;
            this.createWriteFn = createWriteFn;
        }

        public Write withOrgId(String value) {
            this.orgId = orgId;
            return this;
        }

        public Write withMaxBatchSize(long value) {
            this.maxBatchSize = value;
            return this;
        }

        public Write withMaxBatchBytes(long value) {
            this.maxBatchBytes = value;
            return this;
        }

        public Write withMaxConcurrentHttpConnections(int value) {
            this.maxConcurrentHttpConnections = value;
            return this;
        }

        public Write withReadTimeoutMs(long value) {
            this.readTimeoutMs = value;
            return this;
        }

        public Write withWriteTimeoutMs(long value) {
            this.writeTimeoutMs = value;
            return this;
        }

        public Write withFixedLabel(String name, String value) {
            sanitize(name, value, fixedLabels::put);
            return this;
        }

        public Write withMetricName(String value) {
            return withFixedLabel(METRIC_NAME_LABEL, value);
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply(ParDo.of(createWriteFn.apply(this)));
            return PDone.in(input.getPipeline());
        }

    }

    /**
     * Base function class for writing to Cortex.
     * <p>
     * Subclasses have to implement a method that is annotated by {@link org.apache.beam.sdk.transforms.DoFn.ProcessElement}
     * and that calls the {@link #outputTimeSeries(Consumer)} method.
     * <p>
     * Depending on the information required to transform input of type {@code T} into a Cortex protobuf time series
     * different subclasses are used.
     */
    public abstract static class WriteFn<T> extends DoFn<T, Void> {

        private final Write<T> spec;

        private transient OkHttpClient okHttpClient;

        private transient PrometheusRemote.WriteRequest.Builder writeRequestBuilder;
        private transient long batchSize;
        private transient long batchBytes;

        public WriteFn(Write<T> spec) {
            this.spec = spec;
        }

        @Setup
        public void setup() {
            LOG.debug("setup - instance: {}", this);
            var connectionPool = new ConnectionPool(spec.maxConcurrentHttpConnections, 5, TimeUnit.MINUTES);
            var dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(spec.maxConcurrentHttpConnections);
            dispatcher.setMaxRequestsPerHost(spec.maxConcurrentHttpConnections);
            okHttpClient = new OkHttpClient.Builder()
                    .readTimeout(spec.readTimeoutMs, TimeUnit.MILLISECONDS)
                    .writeTimeout(spec.writeTimeoutMs, TimeUnit.MILLISECONDS)
                    .dispatcher(dispatcher)
                    .connectionPool(connectionPool)
                    .build();
        }

        @StartBundle
        public void startBundle(StartBundleContext context) {
            LOG.debug("startBundle - instance: {}", this);
            startBatch();
        }

        /**
         * Called by subclasses for adding a time series to the output.
         */
        protected void outputTimeSeries(Consumer<TimeSeriesBuilder> consumer) throws IOException {
            var builder = PrometheusTypes.TimeSeries.newBuilder();

            for (var entry : spec.fixedLabels.entrySet()) {
                builder.addLabels(
                        PrometheusTypes.Label.newBuilder()
                                .setName(entry.getKey())
                                .setValue(entry.getValue())
                );
            }

            consumer.accept(new TimeSeriesBuilder() {
                @Override
                public TimeSeriesBuilder addLabel(String name, String value) {
                    sanitize(name, value,
                            (n, v) -> builder.addLabels(PrometheusTypes.Label.newBuilder()
                                    .setName(n)
                                    .setValue(v)
                            )
                    );
                    return this;
                }

                @Override
                public TimeSeriesBuilder addSample(long epocheMillis, double value) {
                    builder.addSamples(
                            PrometheusTypes.Sample.newBuilder()
                                    .setTimestamp(epocheMillis)
                                    .setValue(value)
                    );
                    return this;
                }
            });

            var timeSeries = builder.build();
            int serializedSize = timeSeries.getSerializedSize();
            // check if the time series does fit into the buffer and flush the buffer if necessary
            if (batchSize + 1 > spec.maxBatchSize || batchBytes + serializedSize > spec.maxBatchBytes) {
                flushBatch();
                startBatch();
            }
            batchSize++;
            batchBytes += serializedSize;
            writeRequestBuilder.addTimeseries(timeSeries);
        }

        @FinishBundle
        public void finishBundle(FinishBundleContext context)
                throws IOException, InterruptedException {
            LOG.debug("finishBundle - instance: {}", this);
            flushBatch();
        }

        @Teardown
        public void closeClient() throws IOException {
            LOG.debug("teardown - instance: {}", this);
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
        }

        @Override
        public TypeDescriptor<T> getInputTypeDescriptor() {
            return super.getInputTypeDescriptor();
        }

        @Override
        public TypeDescriptor<Void> getOutputTypeDescriptor() {
            return super.getOutputTypeDescriptor();
        }

        public void startBatch() {
            writeRequestBuilder = PrometheusRemote.WriteRequest.newBuilder();
            batchSize = 0;
            batchBytes = 0;
        }

        private void flushBatch() throws IOException {
            LOG.trace("flushBatch - instance: {}; batchSize: {}", this, batchSize);
            if (batchSize == 0) {
                return;
            }
            // Compress the write request using Snappy
            PrometheusRemote.WriteRequest writeRequest = writeRequestBuilder.build();
            final byte[] writeRequestCompressed = Snappy.compress(writeRequest.toByteArray());

            // Build the HTTP request
            final RequestBody body = RequestBody.create(PROTOBUF_MEDIA_TYPE, writeRequestCompressed);
            final Request.Builder builder = new Request.Builder()
                    .url(spec.writeUrl)
                    .addHeader("X-Prometheus-Remote-Write-Version", "0.1.0")
                    .addHeader("Content-Encoding", "snappy")
                    .addHeader("User-Agent", CortexIo.class.getCanonicalName())
                    .post(body);
            // Add the OrgId header if set
            if (spec.orgId != null) {
                builder.addHeader(X_SCOPE_ORG_ID_HEADER, spec.orgId);
            }
            final Request request = builder.build();

            LOG.trace("Writing: {}", writeRequest);

            // TODO: bulkheading, retries, ...

            okHttpClient.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(Call call, IOException e) {
                    LOG.error("Write to Cortex failed", e);
                }

                @Override
                public void onResponse(Call call, Response response) {
                    try (ResponseBody body = response.body()) {
                        if (!response.isSuccessful()) {
                            String bodyAsString;
                            if (body != null) {
                                try {
                                    bodyAsString = body.string();
                                } catch (IOException e) {
                                    bodyAsString = "(error reading body)";
                                }
                            } else {
                                bodyAsString = "(null)";
                            }
                            LOG.error("Writing to Cortex failed - code: " + response.code() + "; message: " + response.message() + "; body: " + bodyAsString);
                        }
                    }
                }
            });

        }

    }

    interface TimeSeriesBuilder {
        TimeSeriesBuilder addLabel(String name, String value);

        TimeSeriesBuilder addSample(long epochMillis, double value);
    }

    @FunctionalInterface
    interface BuildFromProcessContext<T> extends Serializable {
        void accept(DoFn<T, Void>.ProcessContext processContext, TimeSeriesBuilder builder);
    }

    public static <T> SerializableFunction<Write, WriteFn<T>> writeFn(
            BuildFromProcessContext<T> build
    ) {
        // an anonymous inner class did not work with Beam's type wizardry -> use a local class
        class LocalWriteFn extends WriteFn<T> {
            public LocalWriteFn(Write<T> spec) {
                super(spec);
            }
            @ProcessElement
            public void processElement(ProcessContext processContext) throws Exception {
                outputTimeSeries(builder -> build.accept(processContext, builder));
            }
        }
        return LocalWriteFn::new;
    }

    @FunctionalInterface
    interface BuildFromProcessContextAndWindow<T> extends Serializable {
        void accept(DoFn<T, Void>.ProcessContext processContext, BoundedWindow window, TimeSeriesBuilder builder);
    }

    public static <T> SerializableFunction<Write, WriteFn<T>> writeFn(
            BuildFromProcessContextAndWindow<T> build
    ) {
        // an anonymous inner class did not work with Beam's type wizardry -> use a local class
        class LocalWriteFn extends WriteFn<T> {
            public LocalWriteFn(Write<T> spec) {
                super(spec);
            }
            @ProcessElement
            public void processElement(ProcessContext processContext, BoundedWindow window) throws Exception {
                outputTimeSeries(builder -> build.accept(processContext, window, builder));
            }
        }
        return LocalWriteFn::new;
    }

    @FunctionalInterface
    interface BuildFromElementAndTimestamp<T> extends Serializable {
        void accept(T t, Instant timestamp, TimeSeriesBuilder builder);
    }

    public static <T> SerializableFunction<Write, WriteFn<T>> writeFn(
            BuildFromElementAndTimestamp<T> build
    ) {
        // an anonymous inner class did not work with Beam's type wizardry -> use a local class
        class LocalWriteFn extends WriteFn<T> {
            public LocalWriteFn(Write<T> spec) {
                super(spec);
            }

            @ProcessElement
            public void processElement(@Element T element, @Timestamp Instant timestamp) throws Exception {
                outputTimeSeries(builder -> build.accept(element, timestamp, builder));
            }
        }
        return LocalWriteFn::new;
    }

    public static void sanitize(String name, String value, BiConsumer<String, String> consumer) {
        String n;
        String v;
        if (METRIC_NAME_LABEL.equals(name)) {
            n = name;
            v = sanitizeMetricName(value);
        } else {
            n = sanitizeLabelName(name);
            v = value;
        }
        consumer.accept(n, v);
    }

    public static String sanitizeMetricName(String metricName) {
        // Hard-coded implementation optimized for speed - see
        // See https://github.com/prometheus/common/blob/v0.22.0/model/metric.go#L92
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < metricName.length(); i++) {
            char b = metricName.charAt(i);
            if (!((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == ':' || (b >= '0' && b <= '9' && i > 0))) {
                sb.append("_");
            } else {
                sb.append(b);
            }
        }
        return sb.toString();
    }

    public static String sanitizeLabelName(String labelName) {
        // Hard-coded implementation optimized for speed - see
        // See https://github.com/prometheus/common/blob/v0.22.0/model/labels.go#L95
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < labelName.length(); i++) {
            char b = labelName.charAt(i);
            if (!((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || (b >= '0' && b <= '9' && i > 0))) {
                sb.append("_");
            } else {
                sb.append(b);
            }
        }
        return sb.toString();
    }

}
