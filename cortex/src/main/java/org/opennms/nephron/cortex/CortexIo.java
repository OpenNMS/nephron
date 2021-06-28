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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
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
import com.google.protobuf.util.JsonFormat;
import com.swrve.ratelimitedlogger.RateLimitedLog;

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
 * Provides write transforms (sinks) for writing to Cortex.
 * <p>
 * Cortex requires that samples of a specific time series are written with increasing timestamps. The write transforms assign
 * index numbers to output values that can be used as an additional metric label that make time series strictly
 * increasing even in case of multiple panes for the same window and panes of later windows completing before
 * panes of earlier windows.
 * <p>
 * This class can provide two different kinds of sinks:
 * <dl>
 *     <dt>Non-accumulating sink</dt>
 *     <dd>A sink that assigns unique index numbers to output values.</dd>
 *     <dt>Accumulating sink</dt>
 *     <dd>A sink that assigns unique index numbers to output values and additionally accumulates values
 *     of different panes before they are output.</dd>
 * </dl>
 * Both transforms use Beam's state mechanism to track written event timestamps. By doing so, index numbers can be
 * derived that allow to disambiguate samples for the same event timestamps. The accumulating sink
 * additionally uses Beam's timer mechanism to hold back values for accumulation and flushes values after a configured
 * output delay.
 * <p>
 * Sinks are configured by the {@link Write} class and are implemented by subclasses of the {@link WriteFn} class.
 * Sinks provide the following counter metrics in the "cortex" namespace:
 * <dl>
 *     <dt>sample</dt>
 *     <dd>Counts the number of samples</dd>
 *     <dt>write</dt>
 *     <dd>Counts the number of sent write requests</dd>
 *     <dt>write_failure</dt>
 *     <dd>Counts the number of failed write requests (requests that resulted in an exception)</dd>
 *     <dt>response_failure</dt>
 *     <dd>Counts the number of Cortex responses with a non-success result code</dd>
 * </dl>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CortexIo {

    // Label name indicating the metric name of a time series.
    private static final String METRIC_NAME_LABEL = "__name__";

    private static final Logger LOG = LoggerFactory.getLogger(CortexIo.class);

    public static final RateLimitedLog RATE_LIMITED_LOG = RateLimitedLog
            .withRateLimit(LOG)
            .maxRate(5).every(java.time.Duration.ofSeconds(10))
            .build();

    private static final Logger LOG_WRITE = LoggerFactory.getLogger(CortexIo.class.getName() + ".write");

    private static final MediaType PROTOBUF_MEDIA_TYPE = MediaType.parse("application/x-protobuf");

    private static final String X_SCOPE_ORG_ID_HEADER = "X-Scope-OrgID";

    public static final String CORTEX_METRIC_NAMESPACE = "cortex";
    public static final String CORTEX_WRITE_METRIC_NAME = "write";
    public static final String CORTEX_SAMPLE_METRIC_NAME = "sample";
    public static final String CORTEX_WRITE_FAILURE_METRIC_NAME = "write_failure";
    public static final String CORTEX_RESPONSE_FAILURE_METRIC_NAME = "response_failure";

    private static Counter WRITE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_WRITE_FAILURE_METRIC_NAME);
    private static Counter RESPONSE_FAILURE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_RESPONSE_FAILURE_METRIC_NAME);
    private static Counter WRITE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_WRITE_METRIC_NAME);
    private static Counter SAMPLE = Metrics.counter(CORTEX_METRIC_NAMESPACE, CORTEX_SAMPLE_METRIC_NAME);

    @FunctionalInterface
    public interface BuildTimeSeries<K, V> extends Serializable {
        void accept(K key, V value, Instant eventTimestamp, int index, TimeSeriesBuilder builder);
    }

    /**
     * Creates a Cortex sink.
     */
    public static <K, V> Write<K, V> of(
            String writeUrl,
            BuildTimeSeries<K, V> build
    ) {
        return new WriteWithoutAccumulation<>(writeUrl, build);
    }

    public static <K, V> Write<K, V> of(
            String writeUrl,
            BuildTimeSeries<K, V> build,
            Coder<K> keyCoder,
            Coder<V> valueCoder,
            SerializableBiFunction<V, V, V> combiner,
            Duration accumulationDelay
    ) {
        return new WriteWithAccumulation<>(writeUrl, build, keyCoder, valueCoder, combiner, accumulationDelay);
    }

    /**
     * Stores the configuration of the transforms.
     * <p>
     * Expands to a {@link ParDo} transform that is specified by subclasses of the {@link WriteFn} class.
     */
    public abstract static class Write<K, V> extends PTransform<PCollection<KV<K, V>>, PDone> {

        protected final String writeUrl;
        protected final BuildTimeSeries<K, V> build;

        protected String orgId;

        // maximum number of time series samples in a single batch
        protected long maxBatchSize = 10000;
        // threshold for the sum of the number of bytes of time series data in a single batch
        // -> the complete message will be some bytes larger
        protected long maxBatchBytes = 512 * 1024;

        protected long readTimeoutMs = 10000;
        protected long writeTimeoutMs = 10000;

        protected final Map<String, String> fixedLabels = new HashMap<>();

        protected final List<BiConsumer<Call, Response>> responseHandlers = new ArrayList<>();
        protected final List<BiConsumer<Call, Exception>> failureHandlers = new ArrayList<>();

        public Write(
                String writeUrl,
                BuildTimeSeries<K, V> build
        ) {
            super("CortexWrite");
            this.writeUrl = writeUrl;
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

        public Write<K, V> withResponseHandler(BiConsumer<Call, Response> handler) {
            responseHandlers.add(handler);
            return this;
        }

        public Write<K, V> withFailureHandler(BiConsumer<Call, Exception> handler) {
            failureHandlers.add(handler);
            return this;
        }

        @VisibleForTesting
        abstract WriteFn<K, V, ?> createWriteFn(Duration allowedLateness);

        @Override
        public PDone expand(PCollection<KV<K, V>> input) {
            // switch to the global window (or - in test mode - at least to a very long lasting window)
            // -> the state of the stateful WriteFn function is shared by the values in that window
            // -> this can be used to have cross window control of the output

            input
                    .apply(Window.into(new GlobalWindows()))
                    .apply(ParDo.of(createWriteFn(input.getWindowingStrategy().getAllowedLateness())));
            return PDone.in(input.getPipeline());
        }
    }

    private static class WriteWithAccumulation<K, V> extends Write<K, V> {
        private final Coder<K> keyCoder;
        private final Coder<V> valueCoder;
        private final SerializableBiFunction<V, V, V> combiner;
        private final Duration accumulationDelay;
        public WriteWithAccumulation(String writeUrl, BuildTimeSeries<K, V> build, Coder<K> keyCoder, Coder<V> valueCoder, SerializableBiFunction<V, V, V> combiner, Duration accumulationDelay) {
            super(writeUrl, build);
            this.keyCoder = keyCoder;
            this.valueCoder = valueCoder;
            this.combiner = combiner;
            this.accumulationDelay = accumulationDelay;
        }

        WriteFn<K, V, ?> createWriteFn(Duration allowedLateness) {
            return new WriteFnWithAccumulation<>(this, allowedLateness);
        }

    }

    private static class WriteWithoutAccumulation<K, V> extends Write<K, V> {
        public WriteWithoutAccumulation(String writeUrl, BuildTimeSeries<K, V> build) {
            super(writeUrl, build);
        }

        WriteFn<K, V, ?> createWriteFn(Duration allowedLateness) {
            return new WriteFnWithoutAccumulation<>(this, allowedLateness);
        }
    }

    /**
     * Function class for writing to Cortex.
     */
    public static abstract class WriteFn<K, V, W extends Write<K, V>> extends DoFn<KV<K, V>, Void> {

        protected final W spec;
        protected final Duration allowedLateness;

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

        public WriteFn(W spec, Duration allowedLateness) {
            this.spec = spec;
            this.allowedLateness = allowedLateness;
            LOG.debug("allowed lateness: {}", allowedLateness);
        }

        protected void setGcTimer(Timer gcTimer, Instant newestEventTimestamp) {
            // add an additional millisecond to make sure that the gc timer is triggered after data that arrives with
            // the maximum allowed lateness
            var gcAt =  newestEventTimestamp.plus(allowedLateness).plus(1);
            gcTimer.set(gcAt);
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

        /**
         * Transfer the value accumulated in an atomic long counter into a Beam metrics counter.
         * <p>
         * Atomic longs are used to record results in HTTP callbacks because Beam's metrics can not be accessed
         * in the callbacks.
         */
        private static void incrementCounter(AtomicLong al, Counter counter) {
            var cnt = al.getAndSet(0);
            if (cnt != 0) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("increment counter - name: {};  count: {}", counter.getName(), cnt);
                }
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

        protected void outputTimeSeries(Consumer<TimeSeriesBuilder> consumer) throws IOException {
            final TimeSeriesBuilderImpl timeSeriesBuilder = new TimeSeriesBuilderImpl(spec.fixedLabels.entrySet());
            consumer.accept(timeSeriesBuilder);

            for (var builder: timeSeriesBuilder.builders) {
                var timeSeries = builder.build();
                int serializedSize = timeSeries.getSerializedSize();
                // check if the time series does fit into the buffer and flush the buffer if necessary
                if (batchSize >= spec.maxBatchSize || batchBytes + serializedSize > spec.maxBatchBytes) {
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

            if (LOG_WRITE.isTraceEnabled()) {
                // convert the writeRequest into a message string that can be used as the body of an ElasticSearch
                // bulk indexing request.
                // -> if the LOG_WRITE logger is configured accordingly Cortex samples are also stored in ES.
                //    (cf. the `log4j2-flink.xml` file for further explanation.)
                final var str = PROTOBUF_JSON_PRINTER.print(writeRequest);
                var indexAction = "{\"index\":{}}";
                var pattern = Pattern.compile(",\\{\"labels\"");
                var bulkBody = pattern.splitAsStream(str.substring(str.indexOf('[') + 1, str.length() - 2))
                        .filter(StringUtils::isNoneBlank)
                        .collect(
                                Collectors.joining(
                                        "\n" + indexAction + "\n{\"labels\"",
                                        indexAction + "\n",
                                        ""
                                )
                        );
                LOG_WRITE.trace(bulkBody);
            }

            // register an ongoing http request
            // -> onCallback the request is unregistered
            phaser.register();

            writes.incrementAndGet();

            // rely on OkHttp's built-in retry mechanism
            okHttpClient.newCall(request).enqueue(
                    new Callback() {
                        @Override
                        public void onFailure(Call call, IOException e) {
                            try {
                                RATE_LIMITED_LOG.error("Write to Cortex failed", e);
                                writeFailures.incrementAndGet();
                            } finally {
                                phaser.arriveAndDeregister();
                            }
                            spec.failureHandlers.forEach(handler -> handler.accept(call, e));
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
                                        RATE_LIMITED_LOG.error("Write to Cortex failed - code: " + response.code() + "; message: " + response.message() + "; body: " + bodyAsString.trim() + "; request: " + writeRequest);
                                    }
                                }
                            } finally {
                                phaser.arriveAndDeregister();
                            }
                            spec.responseHandlers.forEach(handler -> handler.accept(call, response));
                        }
                    }
            );

        }

    }

    private static class TimeSeriesBuilderImpl implements TimeSeriesBuilder {

        // the "consumer" of the time series builder may need to output several time series
        // -> accumulate the required builders until the consumer has finished its work
        private final ArrayList<PrometheusTypes.TimeSeries.Builder> builders = new ArrayList<>();

        private Set<Map.Entry<String, String>> fixedLabels;

        private PrometheusTypes.TimeSeries.Builder builder;

        private TimeSeriesBuilderImpl(Set<Map.Entry<String, String>> fixedLabels) {
            this.fixedLabels = fixedLabels;
        }

        private PrometheusTypes.TimeSeries.Builder builder() {
            if (builder == null) {
                builder = PrometheusTypes.TimeSeries.newBuilder();
                for (var entry : fixedLabels) {
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
            SAMPLE.inc();
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
    }

    private final static JsonFormat.Printer PROTOBUF_JSON_PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();
    /**
     * A Cortex sink that assigns unique index numbers.
     */
    public static class WriteFnWithoutAccumulation<K, V> extends WriteFn<K, V, WriteWithoutAccumulation<K, V>> {

        private static final String GC_TIMER_NAME = "gc";
        private static final String FLUSHED_STATE_NAME = "flushed";

        @StateId(FLUSHED_STATE_NAME)
        private final StateSpec<ValueState<EventTimestampIndexer>> flushedStateSpec;

        @TimerId(GC_TIMER_NAME)
        private final TimerSpec gcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        public WriteFnWithoutAccumulation(WriteWithoutAccumulation<K, V> spec, Duration allowedLateness) {
            super(spec, allowedLateness);
            flushedStateSpec = StateSpecs.value(EventTimestampIndexer.EventTimestampIndexerCoder.of());
        }

        @ProcessElement
        public void processElement(
                ProcessContext ctx,
                @AlwaysFetched @StateId(FLUSHED_STATE_NAME) ValueState<EventTimestampIndexer> flushedState,
                @TimerId(GC_TIMER_NAME) Timer gcTimer
        ) throws Exception {
            var flushedEventTimestamps = flushedState.read();
            if (flushedEventTimestamps == null) {
                flushedEventTimestamps = new EventTimestampIndexer();
            }
            var index = flushedEventTimestamps.findIndex(ctx.timestamp());
            flushedState.write(flushedEventTimestamps);
            LOG.trace("add value to heap - ctx.timestamp: {}; key: {}; value: {}", ctx.timestamp(), ctx.element().getKey(), ctx.element().getValue());
            outputTimeSeries(builder -> spec.build.accept(ctx.element().getKey(), ctx.element().getValue(), ctx.timestamp(), index, builder));
            var newestEventTimestamp = flushedEventTimestamps.newestEventTimestamp();
            setGcTimer(gcTimer, newestEventTimestamp);
        }

        @OnTimer(GC_TIMER_NAME)
        public void onGc(
                @StateId(FLUSHED_STATE_NAME) ValueState<EventTimestampIndexer> flushedState
        ) throws IOException {
            flushedState.clear();
        }

    }

    /**
     * A Cortex sink that assigns unique index numbers and accumulates values.
     */
    public static class WriteFnWithAccumulation<K, V> extends WriteFn<K, V, WriteWithAccumulation<K, V>> {

        private static final String OUTPUT_TIMER_NAME = "output";
        private static final String GC_TIMER_NAME = "gc";
        private static final String KEY_STATE_NAME = "key";
        private static final String HEAP_STATE_NAME = "heap";

        @StateId(KEY_STATE_NAME)
        private final StateSpec<ValueState<K>> keyStateSpec;

        @StateId(HEAP_STATE_NAME)
        private final StateSpec<ValueState<Heap.HeapImpl<V>>> heapStateSpec;

        @TimerId(OUTPUT_TIMER_NAME)
        private final TimerSpec outputTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        @TimerId(GC_TIMER_NAME)
        private final TimerSpec gcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

        public WriteFnWithAccumulation(WriteWithAccumulation<K, V> spec, Duration allowedLateness) {
            super(spec, allowedLateness);
            keyStateSpec = StateSpecs.value(spec.keyCoder);
            heapStateSpec = StateSpecs.value(new Heap.HeapImpl.HeapImplCoder<>(spec.valueCoder));
        }

        @ProcessElement
        public void processElement(
                ProcessContext ctx,
                @StateId(KEY_STATE_NAME) ValueState<K> keyState,
                @AlwaysFetched @StateId(HEAP_STATE_NAME) ValueState<Heap.HeapImpl<V>> heapState,
                @TimerId(OUTPUT_TIMER_NAME) Timer outputTimer,
                @TimerId(GC_TIMER_NAME) Timer gcTimer
        ) throws Exception {
            var heap = heapState.read();
            if (heap == null) {
                LOG.debug("create heap - key: {}", ctx.element().getKey());
                heap = new Heap.HeapImpl<>(new EventTimestampIndexer(), new HashMap<>());
                // also set the keyState for remembering the key
                keyState.write(ctx.element().getKey());
            }
            if (heap.isEmpty()) {
                // the first value is added to the heap
                // -> that value has to be flushed after the given output delay
                outputTimer.offset(spec.accumulationDelay).setRelative();
            }
            LOG.trace("add value to heap - ctx.timestamp: {}; key: {}; value: {}", ctx.timestamp(), ctx.element().getKey(), ctx.element().getValue());
            heap.add(ctx.element().getValue(), ctx.timestamp(), Instant.now(), spec.combiner);
            heapState.write(heap);
            // a value was just added to the heap -> a newest event timestamp is available
            var newestEventTimestamp = heap.newestEventTimestamp().get();
            setGcTimer(gcTimer, newestEventTimestamp);
        }

        @OnTimer(OUTPUT_TIMER_NAME)
        public void onOutput(
                OnTimerContext ctx,
                @StateId(KEY_STATE_NAME) ValueState<K> keyState,
                @AlwaysFetched @StateId(HEAP_STATE_NAME) ValueState<Heap.HeapImpl<V>> heapState,
                @TimerId(OUTPUT_TIMER_NAME) Timer timer
        ) throws IOException {
            var heap = heapState.read();
            // check if the heap was garbage collected in the meantime
            if (heap != null) {
                Instant now = Instant.now();
                var changed = flushHeap(heap, keyState, now, spec.accumulationDelay, "after accumulation");
                if (changed) {
                    // some values where flushed from the heap
                    // -> the heap has changed and must be written back
                    heapState.write(heap);
                }
                // schedule the output timer if the heap contains more values
                var oldestTimestamp = heap.oldestProcessingTimestamp();
                if (oldestTimestamp.isPresent()) {
                    var d = new Duration(oldestTimestamp.get().plus(spec.accumulationDelay), now);
                    LOG.trace("schedule next heap check - duration: {}", d);
                    timer.offset(d).setRelative();
                }
            }
        }

        @OnTimer(GC_TIMER_NAME)
        public void onGc(
                @StateId(KEY_STATE_NAME) ValueState<K> keyState,
                @AlwaysFetched @StateId(HEAP_STATE_NAME) ValueState<Heap.HeapImpl<V>> heapState
        ) throws IOException {
            var heap = heapState.read();
            if (heap != null) {
                flushHeap(heap, keyState, Instant.now(), Duration.ZERO, "on garbage collection");
                keyState.clear();
                heapState.clear();
            }
        }

        private boolean flushHeap(Heap<V> heap, ValueState<K> keyState, Instant now, Duration outputDelay, String when) throws IOException {
            // flush all values (that are older than 'now - outputDelay')
            var fs = heap.flush(now, outputDelay);
            if (LOG.isTraceEnabled()) {
                LOG.trace("flush values from heap {} - key: {}; size: {}", when, keyState.read(), fs.size());
            }
            if (!fs.isEmpty()) {
                var key = keyState.read();
                for (var f : fs) {
                    outputTimeSeries(builder -> spec.build.accept(key, f.value, f.eventTimestamp, f.index, builder));
                }
            }
            return !fs.isEmpty();
        }

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
