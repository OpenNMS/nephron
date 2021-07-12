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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
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
 * <p>
 * The write transformation provides two counter metrics in the "cortex" namespace:
 * <dl>
 *     <dt>write_failure</dt>
 *     <dd>Counts the number of failed write requests (requests that resulted in an exception)</dd>
 *     <dt>response_failure</dt>
 *     <dd>Counts the number of Cortex responses with a non-success result code</dd>
 * </dl>
 * <p>
 * Cortex requires that samples of a specific metric are written with increasing timestamps. This class assigns
 * index numbers to output values that can be used as an additional metric label that make time series strictly
 * increasing even in case of multiple panes for the same window and panes of later windows completing before
 * panes of earlier windows.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CortexNio {

    // Label name indicating the metric name of a time series.
    private static final String METRIC_NAME_LABEL = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(CortexNio.class);

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    private static final String X_SCOPE_ORG_ID_HEADER = "X-Scope-OrgID";

    public static final String CORTEX_METRIC_NAMESPACE = "cortex";
    public static final String CORTEX_WRITE_METRIC_NAME = "write";
    public static final String CORTEX_WRITE_FAILURE_METRIC_NAME = "write_failure";
    public static final String CORTEX_RESPONSE_FAILURE_METRIC_NAME = "response_failure";

    private static Counter WRITE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_WRITE_FAILURE_METRIC_NAME);
    private static Counter RESPONSE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_RESPONSE_FAILURE_METRIC_NAME);
    private static Counter WRITE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_WRITE_METRIC_NAME);

    @FunctionalInterface
    public interface BuildTimeSeries<K, V> extends Serializable {
        void accept(K key, V value, Instant eventTimestamp, int index, CortexIo.TimeSeriesBuilder builder);
    }

    public static <K, V> Write<K, V> of(
            String writeUrl,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            Duration outputDelay,
            SerializableBiFunction<V, V, V> combiner,
            BuildTimeSeries<K, V> build
    ) {
        return new Write<>(writeUrl, keyCoder, valueCoder, outputDelay, combiner, build);
    }

    /**
     * Stores the configuration of the transformation.
     * <p>
     * Expands to a {@link ParDo} transformation that is specified by the {@link WriteFn} class.
     */
    public static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {

        private final String writeUrl;
        private final Coder<K> keyCoder;
        private final Coder<V> valueCoder;
        private final Duration outputDelay;
        private final SerializableBiFunction<V, V, V> combiner;
        private final BuildTimeSeries<K, V> build;

        private String orgId;

        // maximum number of time series samples in a single batch
        private long maxBatchSize = 10000;
        // threshold for the sum of the number of bytes of time series samples in a single batch
        // -> the complete message will be some bytes larger
        private long maxBatchBytes = 512 * 1024;

        private long readTimeoutMs = 10000;
        private long writeTimeoutMs = 10000;

        private Map<String, String> fixedLabels = new HashMap<>();

        public Write(
                String writeUrl,
                Coder<K> keyCoder,
                Coder<V> valueCoder,
                Duration outputDelay,
                SerializableBiFunction<V, V, V> combiner,
                BuildTimeSeries<K, V> build
        ) {
            super("CortexWrite");
            this.writeUrl = writeUrl;
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.outputDelay = outputDelay;
            this.combiner = combiner;
            this.build = build;
        }

        public Write<K, V> withOrgId(String value) {
            this.orgId = orgId;
            return this;
        }

        public Write<K, V> withMaxBatchSize(long value) {
            this.maxBatchSize = value;
            return this;
        }

        public Write<K, V> withMaxBatchBytes(long value) {
            this.maxBatchBytes = value;
            return this;
        }

        public Write<K, V> withReadTimeoutMs(long value) {
            this.readTimeoutMs = value;
            return this;
        }

        public Write<K, V> withWriteTimeoutMs(long value) {
            this.writeTimeoutMs = value;
            return this;
        }

        public Write<K, V> withFixedLabel(String name, String value) {
            sanitize(name, value, fixedLabels::put);
            return this;
        }

        public Write<K, V> withMetricName(String value) {
            return withFixedLabel(METRIC_NAME_LABEL, value);
        }

        @VisibleForTesting
        WriteFn<K, V> createWriteFn() {
            return new WriteFn<>(this);
        }

        @Override
        public PDone expand(PCollection<KV<K, V>> input) {
            // switch to the global window
            // -> the state of the stateful WriteFn function is shared by the values of all timestamps
            input
                    .apply(Window.into(new GlobalWindows()))
                    .apply(ParDo.of(createWriteFn()));
            return PDone.in(input.getPipeline());
        }
    }

    /**
     * Function class for writing to Cortex.
     */
    public static class WriteFn<K, V> extends DoFn<KV<K, V>, Void> {

        private final Write<K, V> spec;

        private transient OkHttpClient okHttpClient;

        private transient PrometheusRemote.WriteRequest.Builder writeRequestBuilder;
        private transient long batchSize;
        private transient long batchBytes;

        // metrics can not be updated in http response callbacks
        // -> use AtomicLongs for intermediary storage and update the metrics when the bundle is finished
        //    (the counter for writes would not require such an intermediary storage; it is done for uniformness reasons)
        private transient AtomicLong writes;
        private transient AtomicLong writeFailures;
        private transient AtomicLong responseFailures;

        // synchronizes on-going http requests and bundle finalization
        private transient Phaser phaser;

        @StateId("key")
        private final StateSpec<ValueState<K>> keyStateSpec;

        @StateId("heap")
        private final StateSpec<ValueState<Heap.HeapImpl<V>>> heapStateSpec;

        @TimerId("output")
        private final TimerSpec outputTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        public WriteFn(Write<K, V> spec) {
            this.spec = spec;
            keyStateSpec = StateSpecs.value(spec.keyCoder);
            heapStateSpec = StateSpecs.value(new Heap.HeapImpl.HeapImplCoder<>(spec.valueCoder));
        }

        @Setup
        public void setup() {
            LOG.debug("setup - instance: {}", this);
            // request to Cortex must not be sent in parallel
            // -> request contain samples from different timestamps that must be ingested in sequence
            var connectionPool = new ConnectionPool(1, 5, TimeUnit.MINUTES);
            var dispatcher = new Dispatcher();
            dispatcher.setMaxRequests(1);
            dispatcher.setMaxRequestsPerHost(1);
            okHttpClient = new OkHttpClient.Builder()
                    .readTimeout(spec.readTimeoutMs, TimeUnit.MILLISECONDS)
                    .writeTimeout(spec.writeTimeoutMs, TimeUnit.MILLISECONDS)
                    .dispatcher(dispatcher)
                    .connectionPool(connectionPool)
                    .build();
            phaser = new Phaser(1);
            writes = new AtomicLong();
            writeFailures = new AtomicLong();
            responseFailures = new AtomicLong();
        }

        private static void incrementCounter(AtomicLong al, Counter counter) {
            var cnt = al.getAndSet(0);
            if (cnt != 0) {
                LOG.debug("increment counter - name: {};  count: {}", counter.getName(), cnt);
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
            incrementCounter(writes, WRITE);
            incrementCounter(writeFailures, WRITE_FAILURE);
            incrementCounter(responseFailures, RESPONSE_FAILURE);
        }

        @Teardown
        public void closeClient() throws IOException {
            LOG.debug("teardown - instance: {}", this);
            okHttpClient.dispatcher().executorService().shutdown();
            okHttpClient.connectionPool().evictAll();
        }

        @ProcessElement
        public void processElement(
                ProcessContext ctx,
                @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("heap") ValueState<Heap.HeapImpl<V>> heapState,
                @TimerId("output") Timer timer
        ) throws Exception {
            var heap = heapState.read();
            if (heap == null) {
                LOG.debug("create heap - key: ", ctx.element().getKey());
                heap = new Heap.HeapImpl<>(new ArrayList<>(), new HashMap<>());
                // also set the keyState for remembering the key
                keyState.write(ctx.element().getKey());
            }
            if (heap.isEmpty()) {
                // the first value is added to the heap
                // -> that value has to be flushed after the given output delay
                //    (if the value gets not accumulated in the meantime)
                timer.offset(spec.outputDelay).setRelative();
            }
            LOG.trace("add value to heap - ctx.timestamp: {}; key: {}; value: {}", ctx.timestamp(), ctx.element().getKey(), ctx.element().getValue());
            heap.add(ctx.element().getValue(), ctx.timestamp(), Instant.now(), spec.combiner);
            heapState.write(heap);
        }

        @OnTimer("output")
        public void onOutput(
                OnTimerContext ctx,
                @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("heap") ValueState<Heap.HeapImpl<V>> heapState,
                @TimerId("output") Timer timer
        ) throws IOException {
            var heap = heapState.read();
            Instant now = Instant.now();
            var fs = heap.flush(spec.outputDelay, now);
            if (!fs.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("flush values from heap - ctx.timestamp: {}, key: {}; size: {}", ctx.timestamp(), keyState.read(), fs.size());
                }
                // some values where flushed from the heap
                // -> the heap has changed and must be written back
                heapState.write(heap);
                var key = keyState.read();
                for (var f : fs) {
                    outputTimeSeries(builder -> spec.build.accept(key, f.value, f.eventTimestamp, f.index, builder));
                }
            }
            // schedule the output timer if the heap contains more values
            var oldestTimestamp = heap.oldestValueProcessingTimestamp();
            if (oldestTimestamp.isPresent()) {
                var d = new Duration(oldestTimestamp.get().plus(spec.outputDelay), now);
                LOG.trace("schedule next heap check - duration: {}", d);
                timer.offset(d).setRelative();
            }
        }

        @OnWindowExpiration
        public void onExpiration(
                @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("heap") ValueState<Heap.HeapImpl<V>> heapState
        ) throws IOException {
            var heap = heapState.read();
            if (heap != null) {
                // flush all values (they are all older than 'now')
                var fs = heap.flush(Duration.ZERO, Instant.now());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("flush values from heap on window expiration - key: {}; size: {}", keyState.read(), fs.size());
                }
                if (!fs.isEmpty()) {
                    var key = keyState.read();
                    for (var f : fs) {
                        outputTimeSeries(builder -> spec.build.accept(key, f.value, f.eventTimestamp, f.index, builder));
                    }
                }
            }
        }

        private void outputTimeSeries(Consumer<CortexIo.TimeSeriesBuilder> consumer) throws IOException {
            var builders = new ArrayList<PrometheusTypes.TimeSeries.Builder>();

            consumer.accept(new CortexIo.TimeSeriesBuilder() {

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
                public CortexIo.TimeSeriesBuilder setMetricName(String name) {
                    return addLabel(METRIC_NAME_LABEL, name);
                }

                @Override
                public CortexIo.TimeSeriesBuilder addLabel(String name, String value) {
                    sanitize(name, value,
                            (n, v) -> builder().addLabels(PrometheusTypes.Label.newBuilder()
                                    .setName(n)
                                    .setValue(v)
                            )
                    );
                    return this;
                }

                @Override
                public CortexIo.TimeSeriesBuilder addSample(long epocheMillis, double value) {
                    builder().addSamples(
                            PrometheusTypes.Sample.newBuilder()
                                    .setTimestamp(epocheMillis)
                                    .setValue(value)
                    );
                    return this;
                }

                @Override
                public CortexIo.TimeSeriesBuilder nextSeries() {
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
                    .addHeader("User-Agent", CortexNio.class.getCanonicalName())
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

            writes.incrementAndGet();
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
                                        LOG.error("Writing to Cortex failed - code: " + response.code() + "; message: " + response.message() + "; body: " + bodyAsString.trim() + "; request: " + writeRequest);
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
