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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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

import com.google.common.annotations.VisibleForTesting;

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

/**
 * Provides a write transformation for writing to Cortex.
 * <p>
 * The write transformation is configured by the {@link Write} class and is implemented by the {@link WriteFn} class.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CortexIo {

    // Label name indicating the metric name of a time series.
    private static final String METRIC_NAME_LABEL = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(CortexIo.class);

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    private static final String X_SCOPE_ORG_ID_HEADER = "X-Scope-OrgID";

    public static final String CORTEX_METRIC_NAMESPACE = "cortex";
    public static final String CORTEX_WRITE_FAILURE_METRIC_NAME = "cortex_write_failure";
    public static final String CORTEX_RESPONSE_FAILURE_METRIC_NAME = "cortex_response_failure";

    private static Counter WRITE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_WRITE_FAILURE_METRIC_NAME);
    private static Counter RESPONSE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_RESPONSE_FAILURE_METRIC_NAME);

    @FunctionalInterface
    interface CreateWriteFn<T> extends SerializableFunction<Write<T>, CortexIo.WriteFn<T>> {}

    public static <T> Write<T> write(
            String writeUrl,
            CreateWriteFn<T> createWriteFn
    ) {
        return new Write<>(writeUrl, createWriteFn);
    }

    /**
     * Stores the configuration of the transformation.
     * <p>
     * Expands to a {@link ParDo} transformation that is specified by  a subclass of the {@link WriteFn} class.
     */
    public static class Write<T> extends PTransform<PCollection<T>, PDone> {

        private final String writeUrl;
        private final CreateWriteFn<T> createWriteFn;

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
                CreateWriteFn<T> createWriteFn
        ) {
            super("CortexWrite");
            this.writeUrl = writeUrl;
            this.createWriteFn = createWriteFn;
        }

        public Write<T> withOrgId(String value) {
            this.orgId = orgId;
            return this;
        }

        public Write<T> withMaxBatchSize(long value) {
            this.maxBatchSize = value;
            return this;
        }

        public Write<T> withMaxBatchBytes(long value) {
            this.maxBatchBytes = value;
            return this;
        }

        public Write<T> withMaxConcurrentHttpConnections(int value) {
            this.maxConcurrentHttpConnections = value;
            return this;
        }

        public Write<T> withReadTimeoutMs(long value) {
            this.readTimeoutMs = value;
            return this;
        }

        public Write<T> withWriteTimeoutMs(long value) {
            this.writeTimeoutMs = value;
            return this;
        }

        public Write<T> withFixedLabel(String name, String value) {
            sanitize(name, value, fixedLabels::put);
            return this;
        }

        public Write<T> withMetricName(String value) {
            return withFixedLabel(METRIC_NAME_LABEL, value);
        }

        @VisibleForTesting
        WriteFn<T> createWriteFn() {
            return createWriteFn.apply(this);
        }

        @Override
        public PDone expand(PCollection<T> input) {
            input.apply(ParDo.of(createWriteFn()));
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
     * different subclasses are used. The various {@link CortexIo#writeFn} methods instantiate these subclasses.
     */
    public abstract static class WriteFn<T> extends DoFn<T, Void> {

        private final Write<T> spec;

        private transient OkHttpClient okHttpClient;

        private transient PrometheusRemote.WriteRequest.Builder writeRequestBuilder;
        private transient long batchSize;
        private transient long batchBytes;

        // metrics can not be updated in http response callbacks
        // -> use AtomicLongs for intermediary storage and update the metrics when the bundle is finished
        private transient AtomicLong writeFailures;
        private transient AtomicLong responseFailures;

        // synchronizes on-going http requests and bundle finalization
        private transient Phaser phaser;

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
            phaser = new Phaser(1);
            writeFailures = new AtomicLong();
            responseFailures = new AtomicLong();
        }

        private static void recordFailures(AtomicLong al, Counter counter) {
            var cnt = al.getAndSet(0);
            if (cnt != 0) {
                LOG.debug("record failure - count: " + cnt + "; metric: " + counter.getName());
                counter.inc(cnt);
            }
        }

        @StartBundle
        public void startBundle() {
            LOG.debug("startBundle - instance: {}", this);
            startBatch();
        }

        @FinishBundle
        public void finishBundle() throws IOException, InterruptedException {
            LOG.debug("finishBundle - instance: {}", this);
            flushBatch();
            // wait for all write requests to finish
            // -> ensures that writeFailures/responseFailures are current
            // -> blocks until the IO for the current bundle has completed
            // -> decreases the chance that windows are processed out of order
            phaser.arriveAndAwaitAdvance();
            recordFailures(writeFailures, WRITE_FAILURE);
            recordFailures(responseFailures, RESPONSE_FAILURE);
        }

        @Teardown
        public void closeClient() throws IOException {
            LOG.debug("teardown - instance: {}", this);
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
        }

        /**
         * Called by subclasses for adding time series to the output.
         */
        protected void outputTimeSeries(Consumer<TimeSeriesBuilder> consumer) throws IOException {
            var builders = new ArrayList<PrometheusTypes.TimeSeries.Builder>();

            consumer.accept(new TimeSeriesBuilder() {

                private PrometheusTypes.TimeSeries.Builder builder;

                private PrometheusTypes.TimeSeries.Builder builder() {
                    if (builder == null) {
                        builder = PrometheusTypes.TimeSeries.newBuilder();
                        for (var entry : spec.fixedLabels.entrySet()) {
                            builder.addLabels(
                                    PrometheusTypes.Label.newBuilder()
                                            .setName(entry.getKey())
                                            .setValue(entry.getValue())
                            );
                        }
                        builders.add(builder);
                    }
                    return builder;
                }

                @Override
                public TimeSeriesBuilder setMetricName(String name) {
                    return addLabel(METRIC_NAME_LABEL, name);
                }

                @Override
                public TimeSeriesBuilder addLabel(String name, String value) {
                    sanitize(name, value,
                            (n, v) -> builder().addLabels(PrometheusTypes.Label.newBuilder()
                                    .setName(n)
                                    .setValue(v)
                            )
                    );
                    return this;
                }

                @Override
                public TimeSeriesBuilder addSample(long epocheMillis, double value) {
                    builder().addSamples(
                            PrometheusTypes.Sample.newBuilder()
                                    .setTimestamp(epocheMillis)
                                    .setValue(value)
                    );
                    return this;
                }

                @Override
                public TimeSeriesBuilder nextSeries() {
                    // the next builder is created on demand
                    builder = null;
                    return this;
                }
            });

            for (var builder: builders) {
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

            // register an ongoing http request
            // -> onCallback the request is unregistered
            phaser.register();

            okHttpClient.newCall(request).enqueue(
                    new Callback() {
                        @Override
                        public void onFailure(Call call, IOException e) {
                            try {
                                LOG.error("Write to Cortex failed", e);
                                writeFailures.incrementAndGet();
                            } finally {
                                phaser.arriveAndDeregister();
                            }
                            doOnFailure(call, e);
                        }

                        @Override
                        public void onResponse(Call call, Response response) {
                            try {
                                LOG.trace("got response - code: {}, writeRequest: {}", response.code(), writeRequest);
                                try (ResponseBody body = response.body()) {
                                    if (!response.isSuccessful()) {
                                        responseFailures.incrementAndGet();
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
                            } finally {
                                phaser.arriveAndDeregister();
                            }
                            doOnResponse(call, response);
                        }
                    }
            );

        }

        // hook method for subclasses, e.g. in tests
        protected void doOnFailure(Call call, IOException e) {}
        protected void doOnResponse(Call call, Response response) {}

    }

    /**
     * Allows to add labels and samples to an underlying protobuf builder.
     * <p>
     * The metric name is just a special kind of label. Therefore {@link #setMetricName(String)} should be called
     * only once.
     * <p>
     * Label names and metric names are sanitized according to Cortex requirements.
     */
    public interface TimeSeriesBuilder {
        TimeSeriesBuilder setMetricName(String name);

        TimeSeriesBuilder addLabel(String name, String value);

        TimeSeriesBuilder addSample(long epochMillis, double value);

        TimeSeriesBuilder nextSeries();

        default TimeSeriesBuilder addLabel(String name, int value) {
            return addLabel(name, String.valueOf(value));
        }

        default TimeSeriesBuilder addLabel(String name, Integer value) {
            return addLabel(name, String.valueOf(value));
        }

    }

    @FunctionalInterface
    public interface BuildFromProcessContext<T> extends Serializable {
        void accept(DoFn<T, Void>.ProcessContext processContext, TimeSeriesBuilder builder);
    }

    public static <T> CreateWriteFn<T> writeFn(
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
    public interface BuildFromProcessContextAndWindow<T> extends Serializable {
        void accept(DoFn<T, Void>.ProcessContext processContext, BoundedWindow window, TimeSeriesBuilder builder);
    }

    public static <T> CreateWriteFn<T> writeFn(
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
    public interface BuildFromElementAndTimestamp<T> extends Serializable {
        void accept(T t, Instant timestamp, TimeSeriesBuilder builder);
    }

    public static <T> CreateWriteFn<T> writeFn(
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
