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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.opennms.nephron.CompoundKeyType.EXPORTER_INTERFACE;
import static org.opennms.nephron.Pipeline.registerCoders;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.FlowReport;
import org.opennms.nephron.catheter.Simulation;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.generator.FlowDocGen;
import org.opennms.nephron.generator.FlowDocSender;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Complete end-to-end test - reading from Kafka / writing to ElasticSearch.
 *
 * Traffic volume is checked by comparing results of in-memory calculations with results produced by Nephron's pipeline.
 */
public class RandomFlowIT {
    private static final Logger LOG = LoggerFactory.getLogger(RandomFlowIT.class);

    private static final Duration WINDOW_SIZE = Duration.ofSeconds(10);

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Rule
    public ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.2");

    private RestHighLevelClient elasticClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    private static <T> ActionListener<T> toFuture(CompletableFuture<T> future) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                future.complete(result);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        };
    }

    @Before
    public void setUp() throws IOException {
        final HttpHost elasticHost = new HttpHost(elastic.getContainerIpAddress(), elastic.getMappedPort(9200), "http");
        final RestClientBuilder restClientBuilder = RestClient.builder(elasticHost);
        elasticClient = new RestHighLevelClient(restClientBuilder);
        // install the index mapping
        insertIndexMapping();
    }

    @After
    public void tearDown() throws IOException {
        if (elasticClient != null) {
            elasticClient.close();
        }
    }

    @Test
    public void testRates() throws Exception {
        var simulationResult = simulate(
                PipelineOptionsFactory.fromArgs(
                "--runner=FlinkRunner",
                "--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=" + WINDOW_SIZE.toMillis(),
                "--allowedLatenessMs=300000",
                "--lateProcessingDelayMs=2000",
                "--defaultMaxInputDelayMs=20000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class),
        builder -> builder
                .withTickMs(Duration.ofMillis(100))
                .withRealtime(true)
                .withExporters(Exporter.builder()
                        .withNodeId(1)
                        .withForeignSource("Test")
                        .withForeignId("Router1")
                        .withLocation("loc1")
                        .withBytesPerSecond(1_100_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(12)
                        .withOutputSnmp(21))
                .withExporters(Exporter.builder()
                        .withNodeId(2)
                        .withForeignSource("Test")
                        .withForeignId("Router2")
                        .withLocation("loc2")
                        .withBytesPerSecond(1_200_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(13)
                        .withOutputSnmp(31))
                .withExporters(Exporter.builder()
                        .withNodeId(3)
                        .withForeignSource("Test")
                        .withForeignId("Router3")
                        .withLocation("loc3")
                        .withBytesPerSecond(1_300_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(14)
                        .withOutputSnmp(41)),
            QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE")
        );

        checkSimulationResult(simulationResult, FlowSummary::getGroupedByKey);
    }

    @Test
    public void testRatesWithClockSkew() throws Exception {
        var simulationResult = simulate(
        PipelineOptionsFactory.fromArgs(
                "--runner=FlinkRunner",
                "--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=" + WINDOW_SIZE.toMillis(),
                "--allowedLatenessMs=100000",
                "--lateProcessingDelayMs=2000",
                "--earlyProcessingDelayMs=5000",
                "--defaultMaxInputDelayMs=2000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class),
        builder -> builder
                .withTickMs(Duration.ofMillis(100))
                .withRealtime(true)
                .withExporters(Exporter.builder()
                        .withNodeId(1)
                        .withClockOffset(Duration.ofSeconds(-40))
                        .withForeignSource("Test")
                        .withForeignId("Router1")
                        .withLocation("loc1")
                        .withBytesPerSecond(1_100_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(12)
                        .withOutputSnmp(21))
                .withExporters(Exporter.builder()
                        .withNodeId(2)
                        .withClockOffset(Duration.ofSeconds(40))
                        .withForeignSource("Test")
                        .withForeignId("Router2")
                        .withLocation("loc2")
                        .withBytesPerSecond(1_200_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(13)
                        .withOutputSnmp(31))
                .withExporters(Exporter.builder()
                        .withNodeId(3)
                        .withForeignSource("Test")
                        .withForeignId("Router3")
                        .withLocation("loc3")
                        .withBytesPerSecond(1_300_000L)
                        .withActiveTimeout(Duration.ofSeconds(1))
                        .withMaxFlowCount(100)
                        .withInputSnmp(14)
                        .withOutputSnmp(41)),
                QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE")
        );

        checkSimulationResult(simulationResult, FlowSummary::getGroupedByKey);
    }

    @Test
    public void testRateOnApplications() throws Exception {
        var simulationResult = simulate(
                PipelineOptionsFactory.fromArgs(
                        "--runner=FlinkRunner",
                        "--bootstrapServers=" + this.kafka.getBootstrapServers(),
                        "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                        "--parallelism=6",
                        "--fixedWindowSizeMs=" + WINDOW_SIZE.toMillis(),
                        "--allowedLatenessMs=300000",
                        "--lateProcessingDelayMs=2000",
                        "--defaultMaxInputDelayMs=20000",
                        "--flowDestTopic=opennms-flows-aggregated",
                        "--topK=1000")
                        .as(NephronOptions.class),
                builder -> builder
                        .withTickMs(Duration.ofMillis(100))
                        .withRealtime(true)
                        .withExporters(Exporter.builder()
                                .withNodeId(1)
                                .withForeignSource("Test")
                                .withForeignId("Router1")
                                .withLocation("loc1")
                                .withBytesPerSecond(1_100_000L)
                                .withActiveTimeout(Duration.ofSeconds(1))
                                .withMaxFlowCount(100)
                                .withInputSnmp(12)
                                .withOutputSnmp(21))
                        .withExporters(Exporter.builder()
                                .withNodeId(2)
                                .withForeignSource("Test")
                                .withForeignId("Router2")
                                .withLocation("loc2")
                                .withBytesPerSecond(1_200_000L)
                                .withActiveTimeout(Duration.ofSeconds(1))
                                .withMaxFlowCount(100)
                                .withInputSnmp(13)
                                .withOutputSnmp(31))
                        .withExporters(Exporter.builder()
                                .withNodeId(3)
                                .withForeignSource("Test")
                                .withForeignId("Router3")
                                .withLocation("loc3")
                                .withBytesPerSecond(1_300_000L)
                                .withActiveTimeout(Duration.ofSeconds(1))
                                .withMaxFlowCount(100)
                                .withInputSnmp(14)
                                .withOutputSnmp(41)),
                QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE_APPLICATION")
        );

        // the retrieved flow summaries are grouped by exporter/interface/application
        // -> we want to check the volume grouped by exporter/interface
        // -> use a prefix of the groupedBy key for exporter/interface only
        //    (that prefix is 15 characters long, e.g.: "Test:Router1-12")
        checkSimulationResult(simulationResult, fs -> fs.getGroupedByKey().substring(0, 15));
    }

    private SimulationResult simulate(
            NephronOptions options,
            Function<Simulation.Builder, Simulation.Builder> simulationConfigurer,
            QueryBuilder query
    ) throws Exception {
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        final Instant start = Instant.now().minus(WINDOW_SIZE);

        var random = new Random();
        long seed1 = random.nextLong();
        long seed2 = random.nextLong();

        LOG.info("simulation uses seeds - seed1: " + seed1 + "; seed2: " + seed2);

        try (var handler = new RecordingHandler(options, start, seed1)) {

            var simulationBuilder = Simulation.builder(handler).withSeed(seed2).withStartTime(start);

            final Simulation simulation = simulationConfigurer.apply(simulationBuilder).build();

            var pipeline = createPipeline(options);
            var simulationThread = forkSimulation(options, simulation);

            var pipelineResult = pipeline.run();
            pipelineResult.waitUntilFinish();
            simulationThread.join();

            // get all documents from elastic search
            final List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(10000, options, query).get();

            for (final FlowSummary flowSummary : flowSummaries) {
                LOG.debug("{}", flowSummary);
            }

            return new SimulationResult(flowSummaries, handler.inMemoryAggregator.aggregates);
        }
    }

    private static void checkSimulationResult(
            SimulationResult simulationResult,
            Function<FlowSummary, String> groupBy
    ) {
        List<FlowSummary> flowSummaries = simulationResult.flowSummaries;
        Map<WinAndGroupedByKey, Aggregate> inMemoryAggregates = simulationResult.aggregates;

        Map<WinAndGroupedByKey, LongSummaryStatistics> groupedSummaryStatistics = flowSummaries.stream()
                .collect(
                        Collectors.groupingBy(
                                fs -> new WinAndGroupedByKey(fs.getTimestamp(), groupBy.apply(fs)),
                                TreeMap::new,
                                Collectors.summarizingLong(FlowSummary::getBytesTotal))
                );

        for (var entry : groupedSummaryStatistics.entrySet()) {
            LOG.info(entry.getKey() + " --> avg: " + entry.getValue().getAverage() + ", min: " + entry.getValue().getMin() + ", max: " + entry.getValue().getMax() + ", count: " + entry.getValue().getCount());
        }

        for (var entry : inMemoryAggregates.entrySet()) {
            LOG.info(entry.getKey() + " --> in-memory bytes: " + entry.getValue().getBytes());
        }

        for (var entry : groupedSummaryStatistics.entrySet()) {
            var agg = inMemoryAggregates.get(entry.getKey());
            assertThat("missing in-memory result for key: " + entry.getKey(), agg, is(notNullValue()));
            assertThat(entry.getKey().toString(), entry.getValue().getSum(), is(agg.getBytes()));
        }

        for (var key : inMemoryAggregates.keySet()) {
            var ss = groupedSummaryStatistics.get(key);
            assertThat("missing flow summary statistic for key: " + key, ss, is(notNullValue()));
        }
    }

    private CompletableFuture<List<FlowSummary>> getFirstNFlowSummmariesFromES(int numDocs, NephronOptions options, QueryBuilder query) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(options.getElasticFlowIndex() + "-*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(numDocs);
        sourceBuilder.sort("@timestamp", SortOrder.ASC);
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        elasticClient.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
        return future.thenApply(s -> Arrays.stream(s.getHits().getHits())
                .map(hit -> {
                    try {
                        final FlowSummary flowSummary = objectMapper.readValue(hit.getSourceAsString(), FlowSummary.class);
                        flowSummary.setId(hit.getId());
                        return flowSummary;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList()));
    }

    /**
     * Adapted from https://stackoverflow.com/questions/59164611/how-to-efficiently-create-kafka-topics-with-testcontainers
     *
     * @param topics topics to create
     */
    private void createTopics(String... topics) {
        final List<NewTopic> newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        try (AdminClient admin = AdminClient.create(ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers())
                .build())) {
            admin.createTopics(newTopics);
        }
    }

    private void insertIndexMapping() throws IOException {
        Request request = new Request("PUT", "_template/netflow_agg");
        request.setJsonEntity(Resources.toString(Resources.getResource("netflow_agg-template.json"), StandardCharsets.UTF_8));
        Response response = elasticClient.getLowLevelClient().performRequest(request);
        assertThat(response.getWarnings(), hasSize(0));
    }

    /**
     * Waits for a consumer with the expected groupId to register.
     *
     * Blocks execution until the pipeline is ready to read from Kafka.
     */
    private static void waitForConsumer(NephronOptions options) throws Exception {
        var start = Instant.now();
        var conf = new HashMap<String, Object>() {{
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, options.getBootstrapServers());
        }};
        try (var kafkaClient = AdminClient.create(conf)) {
            while (true) {
                Duration elapsed = Duration.between(start, Instant.now());
                List<String> groupIds = kafkaClient.listConsumerGroups().all().get().
                        stream().map(s -> s.groupId()).collect(Collectors.toList());
                if (groupIds.contains(options.getGroupId())) {
                    LOG.info("waited for consumer: " + elapsed.getSeconds() + "s");
                    kafkaClient.close();
                    return;
                } else if (elapsed.getSeconds() > 90) {
                    throw new RuntimeException("Kafka consumer did not register");
                }
                Thread.sleep(100);
            }
        }
    }

    /**
     * Accumulates flow reports.
     *
     * Can be used for debugging reasons.
     */
    private static class SumAndList {
        public long sum = 0;
        public List<FlowReport> list = new ArrayList<>();
        public void add(FlowReport fr) {
            sum += fr.getBytes();
            list.add(fr);
        }
    }

    /**
     * A consumer that records flow reports indexed by exporter and window.
     */
    private static class FlowReportsRecorder implements BiConsumer<Exporter, FlowReport> {

        // exporter x window -> (sum, list)
        public final Map<Integer, Map<Instant, SumAndList>> sentReports = new HashMap<>();

        @Override
        public void accept(Exporter exporter, FlowReport flowReport) {
            Map<Instant, SumAndList> m = sentReports.get(exporter.getNodeId());
            if (m == null) {
                m = new HashMap<>();
                sentReports.put(exporter.getNodeId(), m);
            }
            var wnd = UnalignedFixedWindows.windowNumber(exporter.getNodeId(), WINDOW_SIZE.toMillis(), flowReport.getEnd().toEpochMilli() - 1);
            var sumAndList = m.get(wnd);
            if (sumAndList == null) {
                sumAndList = new SumAndList();
                m.put(Instant.ofEpochMilli(wnd), sumAndList);
            }
            sumAndList.add(flowReport);
        }
    }

    /**
     * Mimics the aggregation made by Nephron.
     */
    private static class InMemoryAggregator {

        public final Map<WinAndGroupedByKey, Aggregate> aggregates = new TreeMap<>();

        private final NephronOptions options;
        private final CompoundKeyType[] compoundKeyTypes;

        private final long windowSizeMs;
        private final long maxFlowDurationMs;

        public InMemoryAggregator(NephronOptions options, CompoundKeyType... compoundKeyTypes) {
            this.options = options;
            this.compoundKeyTypes = compoundKeyTypes;
            this.windowSizeMs = options.getFixedWindowSizeMs();
            this.maxFlowDurationMs = options.getMaxFlowDurationMs();
        }

        public void aggregate(FlowDocument flow) {
            // logic copied from attachTimestamps
            long deltaSwitched = flow.getDeltaSwitched().getValue();
            long lastSwitched = flow.getLastSwitched().getValue();
            int nodeId = flow.getExporterNode().getNodeId();

            long shift = UnalignedFixedWindows.perNodeShift(nodeId, windowSizeMs);
            if (deltaSwitched < shift) {
                return;
            }

            long firstWindow = UnalignedFixedWindows.windowNumber(nodeId, windowSizeMs, deltaSwitched); // the first window the flow falls into
            long lastWindow = UnalignedFixedWindows.windowNumber(nodeId, windowSizeMs, lastSwitched); // the last window the flow falls into (assuming lastSwitched is inclusive)
            long nbWindows = lastWindow - firstWindow + 1;

            long timestamp = deltaSwitched;
            for (long i = 0; i < nbWindows; i++) {
                if (timestamp > lastSwitched - maxFlowDurationMs) {

                    long windowStart = UnalignedFixedWindows.windowStartForTimestamp(
                            flow.getExporterNode().getNodeId(),
                            options.getFixedWindowSizeMs(),
                            timestamp
                    );
                    IntervalWindow window = new IntervalWindow(org.joda.time.Instant.ofEpochMilli(windowStart), new org.joda.time.Duration(windowSizeMs));

                    // total aggregations are calculated for EXPORTER_INTERFACE and EXPORTER_INTERFACE_TOS only
                    for (CompoundKeyType compoundKeyType : compoundKeyTypes) {
                        try {
                            CompoundKey key = compoundKeyType.create(flow);
                            // the timstamp of flow summaries is set by the pipeline to the window end (exclusive)
                            WinAndGroupedByKey winAndCompoundKey = new WinAndGroupedByKey(windowStart + windowSizeMs, key.groupedByKey());
                            Aggregate aggregate = Pipeline.aggregatize(window, flow, "", "");
                            Aggregate previous = aggregates.get(winAndCompoundKey);
                            Aggregate next = previous != null ? Aggregate.merge(previous, aggregate) : aggregate;
                            aggregates.put(winAndCompoundKey, next);
                        } catch (MissingFieldsException e) {
                            throw new RuntimeException(e);
                        }
                    }

                }
                // ensure that the timestamp used for the last window is not larger than lastSwitched
                if (timestamp + windowSizeMs < lastSwitched) {
                    timestamp += windowSizeMs;
                } else {
                    timestamp = lastSwitched;
                }
            }

        }
    }

    /**
     * A handler that tracks some information about sent flows in memory.
     */
    private static class RecordingHandler implements BiConsumer<Exporter, FlowReport>, Closeable {

        public final FlowReportsRecorder flowReportsRecorder = new FlowReportsRecorder();
        public final InMemoryAggregator inMemoryAggregator;

        private final FlowDocGen flowDocGen;
        private final FlowDocSender flowDocSender;

        public RecordingHandler(NephronOptions options, Instant start, long seed) {
            inMemoryAggregator = new InMemoryAggregator(options, EXPORTER_INTERFACE);
            flowDocGen = new FlowDocGen(new Random(seed));
            flowDocSender = new FlowDocSender(options.getBootstrapServers(), options.getFlowSourceTopic(), null);
        }

        @Override
        public void accept(Exporter exporter, FlowReport flowReport) {
            flowReportsRecorder.accept(exporter, flowReport);
            var flowDoc = flowDocGen.createFlowDocument(exporter, flowReport);
            inMemoryAggregator.aggregate(flowDoc);
            flowDocSender.send(flowDoc);
        }

        @Override
        public void close() throws IOException {
            flowDocSender.close();
        }
    }

    /**
     * Creates the pipeline and wires it with its Kafka input and ElasticSearch output.
     *
     * Uses a special timestamp policy factory that ensures that the pipeline run finishes.
     */
    private org.apache.beam.sdk.Pipeline createPipeline(NephronOptions options) {
        var pipeline = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(pipeline);
        Map<String, Object> kafkaConsumerConfig = new HashMap<>();
        kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, options.getGroupId());
        // Auto-commit should be disabled when checkpointing is on:
        // the state in the checkpoints are used to derive the offsets instead
        kafkaConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, options.getAutoCommit());

        // use a timestamp policy that finishes processing when the input is idle for some time
        TimestampPolicyFactory<byte[], FlowDocument> tpf = timestampPolicyFactory(
                org.joda.time.Duration.millis(options.getDefaultMaxInputDelayMs()),
                org.joda.time.Duration.standardSeconds(5)
        );

        var readFromKafka = new Pipeline.ReadFromKafka(
                options.getBootstrapServers(),
                options.getFlowSourceTopic(),
                kafkaConsumerConfig,
                tpf
        );

        pipeline.apply(readFromKafka)
                .apply(new Pipeline.CalculateFlowStatistics(options))
                .apply(new Pipeline.WriteToElasticsearch(options));

        return pipeline;
    }

    /**
     * Forks a thread that wait for the pipeline being ready and then writes the simulated flows.
     */
    private static Thread forkSimulation(NephronOptions options, Simulation simulation) {
        var thread = new Thread(() -> {
            try {
                waitForConsumer(options);
                simulation.start();
                Thread.sleep(100_000L);
                simulation.stop();
                simulation.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread.start();
        return thread;
    }

    /**
     * Returns a timestamp policy factory that advances the watermark to TIMESTAMP_MAX_VALUE when the it was
     * idle for the specified duration.
     * <p>
     * After the watermark is advanced to TIMESTAMP_MAX_VALUE the pipeline run finishes.
     */
    private static TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory(org.joda.time.Duration maxInputDelay, org.joda.time.Duration maxInputIdleDuration) {
        return (tp, previousWatermark) -> new CustomTimestampPolicyWithLimitedDelay<byte[], FlowDocument>(
                Pipeline.ReadFromKafka::getTimestamp,
                maxInputDelay,
                previousWatermark
        ) {
            private org.joda.time.Instant idleSince = null;
            private boolean closed = false;

            @Override
            public org.joda.time.Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<byte[], FlowDocument> record) {
                idleSince = org.joda.time.Instant.now();
                return super.getTimestampForRecord(ctx, record);
            }

            @Override
            public org.joda.time.Instant getWatermark(PartitionContext ctx) {
                // does not work if the source is split
                if (closed || idleSince != null &&
                              new org.joda.time.Duration(idleSince, org.joda.time.Instant.now()).isLongerThan(maxInputIdleDuration)
                ) {
                    closed = true;
                    return BoundedWindow.TIMESTAMP_MAX_VALUE;
                } else {
                    return super.getWatermark(ctx);
                }
            }

        };
    }

    private static class WinAndGroupedByKey implements Comparable<WinAndGroupedByKey> {

        public final Instant window;
        public final String key;

        public WinAndGroupedByKey(long window, String key) {
            this.window = Instant.ofEpochMilli(window);
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WinAndGroupedByKey winAndGroupedByKey = (WinAndGroupedByKey) o;
            return window.equals(winAndGroupedByKey.window) && key.equals(winAndGroupedByKey.key);
        }

        @Override
        public int compareTo(WinAndGroupedByKey o) {
            var c = key.compareTo(o.key);
            return c != 0 ? c : window.compareTo(o.window);
        }

        @Override
        public int hashCode() {
            return Objects.hash(window, key);
        }

        @Override
        public String toString() {
            return "WinAndKey{" +
                   "window=" + window +
                   ", key='" + key + '\'' +
                   '}';
        }
    }

    public static class SimulationResult {
        public final List<FlowSummary> flowSummaries;
        public final Map<WinAndGroupedByKey, Aggregate> aggregates;
        public SimulationResult(List<FlowSummary> flowSummaries, Map<WinAndGroupedByKey, Aggregate> aggregates) {
            this.flowSummaries = flowSummaries;
            this.aggregates = aggregates;
        }
    }
}
