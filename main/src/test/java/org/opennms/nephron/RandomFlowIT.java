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
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.awaitility.core.ThrowingRunnable;
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
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.Simulation;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.generator.Handler;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Complete end-to-end test - reading & writing to/from Kafka
 */
public class RandomFlowIT {
    private static final Logger LOG = LoggerFactory.getLogger(RandomFlowIT.class);

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Rule
    public ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.2");

    private RestHighLevelClient elasticClient;
    private ObjectMapper objectMapper = new ObjectMapper();

    private static Matcher<Long> longCloseTo(final long value, final long delta) {
        return allOf(greaterThanOrEqualTo(value - delta), lessThanOrEqualTo(value + delta));
    }

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
        final NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=10000",
                "--allowedLatenessMs=300000",
                "--lateProcessingDelayMs=2000",
                "--defaultMaxInputDelayMs=20000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class);

        options.as(FlinkPipelineOptions.class).setStreaming(true);

        // create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        final TestFlinkRunner runner = TestFlinkRunner.fromOptions(options);

        // create the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = createPipeline(runner.getPipelineOptions().as(NephronOptions.class));

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder(new Handler(this.kafka.getBootstrapServers(), options.getFlowSourceTopic(), new Random()))
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
                        .withOutputSnmp(41))
                .withStartTime(start)
                .build();

        runSimulation(options, pipeline, simulation);

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE");

        final List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(10000, options, query).get();

        final Map<String, LongSummaryStatistics> summaries = flowSummaries.stream()
                .collect(Collectors.groupingBy(FlowSummary::getGroupedByKey,
                        Collectors.summarizingLong(FlowSummary::getBytesTotal)));

        for (final Map.Entry<String, LongSummaryStatistics> entry : summaries.entrySet()) {
            LOG.info(entry.getKey() + " --> avg: " + entry.getValue().getAverage() + ", min: " + entry.getValue().getMin() + ", max: " + entry.getValue().getMax() + ", count: " + entry.getValue().getCount());
        }

        assertThat(summaries, is(aMapWithSize(3)));

        final long allowedError = 100;

        assertThat(summaries.get("Test:Router1-12").getAverage(), closeTo(11_000_000.0, allowedError));
        assertThat(summaries.get("Test:Router1-12").getMin(), longCloseTo(11_000_000L, allowedError));
        assertThat(summaries.get("Test:Router1-12").getMax(), longCloseTo(11_000_000L, allowedError));

        assertThat(summaries.get("Test:Router2-13").getAverage(), closeTo(12_000_000.0, allowedError));
        assertThat(summaries.get("Test:Router2-13").getMin(), longCloseTo(12_000_000L, allowedError));
        assertThat(summaries.get("Test:Router2-13").getMax(), longCloseTo(12_000_000L, allowedError));

        assertThat(summaries.get("Test:Router3-14").getAverage(), closeTo(13_000_000.0, allowedError));
        assertThat(summaries.get("Test:Router3-14").getMin(), longCloseTo(13_000_000L, allowedError));
        assertThat(summaries.get("Test:Router3-14").getMax(), longCloseTo(13_000_000L, allowedError));
    }

    @Test
    public void testRatesWithClockSkew() throws Exception {
        final NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=10000",
                "--allowedLatenessMs=50000",
                "--lateProcessingDelayMs=2000",
                "--defaultMaxInputDelayMs=2000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class);

        options.as(FlinkPipelineOptions.class).setStreaming(true);

        // create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        final TestFlinkRunner runner = TestFlinkRunner.fromOptions(options);

        // create the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = createPipeline(runner.getPipelineOptions().as(NephronOptions.class));

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder(new Handler(this.kafka.getBootstrapServers(), options.getFlowSourceTopic(), new Random()))
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
                        .withOutputSnmp(41))
                .withStartTime(start)
                .build();

        runSimulation(options, pipeline, simulation);

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE");

        // get all documents from elastic search
        final List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(10000, options, query).get();

        final Map<String, List<FlowSummary>> lists = flowSummaries.stream()
                .collect(Collectors.groupingBy(FlowSummary::getGroupedByKey,
                        Collectors.toList()));

        for (final Map.Entry<String, List<FlowSummary>> entry : lists.entrySet()) {
            LOG.info(entry.getValue().size() + " summaries received for '" + entry.getKey() + "'...");
            for (final FlowSummary flowSummary : entry.getValue()) {
                LOG.info("{}", flowSummary);
            }
        }

        final long allowedError = 100;

        // for each exporter check that the expected volume is contained in each window
        for (var exporterAndVolume: Arrays.asList(Map.entry(1, 11_000_000l), Map.entry(2, 12_000_000l), Map.entry(3, 13_000_000l))) {

            // Beam may output several panes for each window
            // -> the result in each single pane does not meet the expected data volume but the sum over all panes of a windows does

            // calculate the LongSummaryStatistic over all panes for each window
            var summaries = flowSummaries.stream()
                    .filter(fs -> fs.getExporter().getNodeId() == exporterAndVolume.getKey())
                    .collect(Collectors.groupingBy(FlowSummary::getTimestamp, Collectors.summarizingLong(FlowSummary::getBytesTotal)));

            assertThat(summaries, not(anEmptyMap()));

            LOG.info("flow summary statistics for node: {}", exporterAndVolume.getKey());
            for (var entry : summaries.entrySet()) {
                LOG.info(Instant.ofEpochMilli(entry.getKey()) + " --> " + entry.getValue());
            }

            // check that the sum of transferred bytes matches the expected volume in all windows
            summaries.values()
                    .forEach(summaryStatistic -> assertThat(summaryStatistic.getSum(), longCloseTo(exporterAndVolume.getValue(), allowedError)));

        }

    }

    public CompletableFuture<List<FlowSummary>> getFirstNFlowSummmariesFromES(int numDocs, NephronOptions options, QueryBuilder query) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(options.getElasticFlowIndex() + "-*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(numDocs);
        sourceBuilder.sort("@timestamp", SortOrder.ASC);
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        elasticClient.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
        return future.thenApply(s -> {
            var summaries = Arrays.stream(s.getHits().getHits())
                    .map(hit -> {
                        try {
                            final FlowSummary flowSummary = objectMapper.readValue(hit.getSourceAsString(), FlowSummary.class);
                            flowSummary.setId(hit.getId());
                            return flowSummary;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());

            // determine the minimum and maximum timestamps for each exporter / interface
            // -> the results with these timestamps may be incomplete because the simulation is not aligned to windows
            // -> remove flows with these timestamps
            var minColl = Collectors.mapping(FlowSummary::getTimestamp, Collectors.minBy(Comparator.naturalOrder()));
            var maxColl = Collectors.mapping(FlowSummary::getTimestamp, Collectors.maxBy(Comparator.naturalOrder()));
            var mins = summaries.stream().collect(Collectors.groupingBy(RandomFlowIT::exporterAndIfIdx, minColl));
            var maxs = summaries.stream().collect(Collectors.groupingBy(RandomFlowIT::exporterAndIfIdx, maxColl));

            LOG.info("retrieved flows - size: " + summaries.size());

            var filtered = summaries.stream()
                    .filter(fs -> mins.get(exporterAndIfIdx(fs)).map(m -> m != fs.getTimestamp()).orElse(true))
                    .filter(fs -> maxs.get(exporterAndIfIdx(fs)).map(m -> m != fs.getTimestamp()).orElse(true))
                    .collect(Collectors.toList());

            LOG.info("filtered flows - size: " + filtered.size());
            return filtered;
        });
    }

    private static Map.Entry exporterAndIfIdx(FlowSummary fs) {
        return Map.entry(fs.getExporter(), fs.getIfIndex());
    }

    @Test
    public void testRatesPerApplication() throws Exception {
        final NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=10000",
                "--allowedLatenessMs=300000",
                "--lateProcessingDelayMs=2000",
                "--defaultMaxInputDelayMs=20000",
                "--flowDestTopic=opennms-flows-aggregated",
                "--topK=1000")
                .as(NephronOptions.class);

        options.as(FlinkPipelineOptions.class).setStreaming(true);

        // create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        final TestFlinkRunner runner = TestFlinkRunner.fromOptions(options);

        // create the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = createPipeline(runner.getPipelineOptions().as(NephronOptions.class));

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder(new Handler(this.kafka.getBootstrapServers(), options.getFlowSourceTopic(), new Random()))
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
                        .withOutputSnmp(41))
                .withStartTime(start)
                .build();

        runSimulation(options, pipeline, simulation);

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE_APPLICATION");

        // get all documents from elastic search
        final List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(10000, options, query).get();

        for(final FlowSummary flowSummary : flowSummaries) {
            LOG.info("{}", flowSummary);
        }

        final Map<String, List<FlowSummary>> lists = flowSummaries.stream()
                .collect(Collectors.groupingBy(f -> f.getGroupedByKey().substring(0, 15),
                        Collectors.toList()));

        final Map<String, Map<Long, Long>> summaries = new TreeMap<>();

        for(final Map.Entry<String, List<FlowSummary>> list : lists.entrySet()) {
            summaries.put(list.getKey(), list.getValue().stream().collect(Collectors.groupingBy(FlowSummary::getRangeStartMs, TreeMap::new, Collectors.summingLong(FlowSummary::getBytesTotal))));
        }

        final long allowedError = 100;

        final Map<String, Long> expected = new TreeMap<>();

        expected.put("Test:Router1-12", 11_000_000L);
        expected.put("Test:Router2-13", 12_000_000L);
        expected.put("Test:Router3-14", 13_000_000L);

        for(final Map.Entry<String, Long> entry : expected.entrySet()) {
            boolean first = true;
            for(final long pointInTime : new TreeSet<Long>(summaries.get(entry.getKey()).keySet())) {
                LOG.info(entry.getKey() + "@" + pointInTime + " --> sum: " + summaries.get(entry.getKey()).get(pointInTime));
                if (first) {
                    first = false;
                } else {
                    assertThat(summaries.get(entry.getKey()).get(pointInTime), longCloseTo(entry.getValue(), allowedError));
                }
            }
        }
    }

    private PipelineResult runSimulation(NephronOptions options, org.apache.beam.sdk.Pipeline pipeline, Simulation simulation) throws InterruptedException {
        return runPipeline(options, pipeline, flowProducer(simulation));
    }

    public static PipelineResult runPipeline(NephronOptions options, org.apache.beam.sdk.Pipeline pipeline, ThrowingRunnable flowProducer) throws InterruptedException {
        var flowProducerThread = forkFlowProducer(options, flowProducer);
        var pipelineResult = pipeline.run();
        pipelineResult.waitUntilFinish();
        flowProducerThread.join();
        return pipelineResult;
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
     * Creates the pipeline and wires it with its Kafka input and ElasticSearch output.
     * <p>
     * Uses a special timestamp policy factory that ensures that the pipeline run finishes.
     */
    public static org.apache.beam.sdk.Pipeline createPipeline(NephronOptions options) {
        // use a timestamp policy that finishes processing when the input is idle for some time
        TimestampPolicyFactory<byte[], FlowDocument> tpf = timestampPolicyFactory(
                org.joda.time.Duration.millis(options.getDefaultMaxInputDelayMs()),
                org.joda.time.Duration.standardSeconds(5)
        );
        return Pipeline.create(options, tpf);
    }

    /**
     * Returns a timestamp policy factory that advances the watermark to TIMESTAMP_MAX_VALUE when it was
     * idle for the specified duration.
     * <p>
     * After the watermark is advanced to TIMESTAMP_MAX_VALUE the pipeline run finishes.
     */
    private static TimestampPolicyFactory<byte[], FlowDocument> timestampPolicyFactory(org.joda.time.Duration maxInputDelay, org.joda.time.Duration maxInputIdleDuration) {
        return (tp, previousWatermark) -> new CustomTimestampPolicyWithLimitedDelay<>(
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

    /**
     * Waits for a consumer with the expected groupId to register.
     * <p>
     * Blocks execution until the pipeline is ready to read from Kafka.
     */
    public static void waitForConsumer(NephronOptions options) throws Exception {
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
     * Forks a thread that waits for the pipeline being ready and then writes the simulated flows.
     */
    private static Thread forkFlowProducer(NephronOptions options, ThrowingRunnable flowProducer) {
        var thread = new Thread(() -> {
            try {
                waitForConsumer(options);
                flowProducer.run();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
        thread.start();
        return thread;
    }

    private static ThrowingRunnable flowProducer(Simulation simulation) {
        return () -> {
            simulation.start();
            Thread.sleep(100_000L);
            simulation.stop();
            simulation.join();
        };
    }

}
