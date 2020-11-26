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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.admin.AdminClient;
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
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.Simulation;
import org.opennms.nephron.elastic.FlowSummary;
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
    public MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
                                                                                                                .setNumberTaskManagers(2)
                                                                                                                .setNumberSlotsPerTaskManager(3)
                                                                                                                .build());
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
    public void canPee() throws Exception {
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

        // fire up the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = Pipeline.create(runner.getPipelineOptions().as(NephronOptions.class));

        final Thread t = new Thread(() -> {
            try {
                runner.run(pipeline);
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof InterruptedException) {
                    return;
                }
                ex.printStackTrace();
            }
        });
        t.start();

        // wait until the pipeline's Kafka consumer has started
        Thread.sleep(5_000);

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder()
                .withBootstrapServers(this.kafka.getBootstrapServers())
                .withFlowTopic(options.getFlowSourceTopic())
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

        simulation.start();

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE");

        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> getFirstNFlowSummmariesFromES(1, options, query).get().size(), greaterThanOrEqualTo(1));

        Thread.sleep(100_000L);

        simulation.stop();
        simulation.join();

        t.interrupt();
        t.join();

        // get all documents from elastic search while skipping the first 3 entries because they are incomplete windows
        final List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(100, options, query).get()
                .stream()
                .skip(3)
                .collect(Collectors.toList());

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
    public void canPeeWithClockSkew() throws Exception {
        final NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + this.kafka.getBootstrapServers(),
                "--elasticUrl=http://" + this.elastic.getHttpHostAddress(),
                "--parallelism=6",
                "--fixedWindowSizeMs=10000",
                "--allowedLatenessMs=10000",
                "--lateProcessingDelayMs=2000",
                "--defaultMaxInputDelayMs=2000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class);

        options.as(FlinkPipelineOptions.class).setStreaming(true);

        // create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        final TestFlinkRunner runner = TestFlinkRunner.fromOptions(options);

        // fire up the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = Pipeline.create(runner.getPipelineOptions().as(NephronOptions.class));

        final Thread t = new Thread(() -> {
            try {
                runner.run(pipeline);
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof InterruptedException) {
                    return;
                }
                ex.printStackTrace();
            }
        });
        t.start();

        // wait until the pipeline's Kafka consumer has started
        Thread.sleep(5_000);

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder()
                .withBootstrapServers(this.kafka.getBootstrapServers())
                .withFlowTopic(options.getFlowSourceTopic())
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

        simulation.start();

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE");

        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> getFirstNFlowSummmariesFromES(1, options, query).get().size(), greaterThanOrEqualTo(1));

        Thread.sleep(100_000L);

        simulation.stop();
        simulation.join();

        t.interrupt();
        t.join();

        // get all documents from elastic search
        final List<FlowSummary> flowSummaries = new ArrayList<>(getFirstNFlowSummmariesFromES(100, options, query).get());

        final Map<String, List<FlowSummary>> lists = flowSummaries.stream()
                .collect(Collectors.groupingBy(FlowSummary::getGroupedByKey,
                        Collectors.toList()));

        for (final Map.Entry<String, List<FlowSummary>> entry : lists.entrySet()) {
            LOG.info(entry.getValue().size() + " summaries received for '" + entry.getKey() + "'...");
            for (final FlowSummary flowSummary : entry.getValue()) {
                LOG.info("{}", flowSummary);
            }
        }

        final Map<String, LongSummaryStatistics> summaries = new TreeMap<>();

        for (final Map.Entry<String, List<FlowSummary>> list : lists.entrySet()) {
            // skip first entry per router since it is incomplete
            if (!list.getValue().isEmpty()) {
                list.getValue().remove(0);
                summaries.put(list.getKey(), list.getValue().stream().collect(Collectors.summarizingLong(FlowSummary::getBytesTotal)));
            }
        }

        for (final Map.Entry<String, LongSummaryStatistics> entry : summaries.entrySet()) {
            LOG.info(entry.getKey() + " --> avg: " + entry.getValue().getAverage() + ", min: " + entry.getValue().getMin() + ", max: " + entry.getValue().getMax() + ", count: " + entry.getValue().getCount());
        }

        assertThat(summaries, is(aMapWithSize(3)));

        final long allowedError = 100;

        assertThat(summaries.get("Test:Router1-12").getCount(), lessThan(allowedError));

        assertThat(summaries.get("Test:Router2-13").getCount(), lessThan(summaries.get("Test:Router3-14").getCount()));

        assertThat(summaries.get("Test:Router2-13").getAverage(), closeTo(12_000_000.0, allowedError));
        assertThat(summaries.get("Test:Router2-13").getMin(), longCloseTo(12_000_000L, allowedError));
        assertThat(summaries.get("Test:Router2-13").getMax(), longCloseTo(12_000_000L, allowedError));

        assertThat(summaries.get("Test:Router3-14").getAverage(), closeTo(13_000_000.0, allowedError));
        assertThat(summaries.get("Test:Router3-14").getMin(), longCloseTo(13_000_000L, allowedError));
        assertThat(summaries.get("Test:Router3-14").getMax(), longCloseTo(13_000_000L, allowedError));

    }

    public CompletableFuture<List<FlowSummary>> getFirstNFlowSummmariesFromES(int numDocs, NephronOptions options) {
        return getFirstNFlowSummmariesFromES(numDocs, options, QueryBuilders.matchAllQuery());
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

    @Test
    public void canPeeApplications() throws Exception {
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

        // fire up the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = Pipeline.create(runner.getPipelineOptions().as(NephronOptions.class));

        final Thread t = new Thread(() -> {
            try {
                runner.run(pipeline);
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof InterruptedException) {
                    return;
                }
                ex.printStackTrace();
            }
        });
        t.start();

        // wait until the pipeline's Kafka consumer has started
        Thread.sleep(5_000);

        final Instant start = Instant.now().minus(Duration.ofSeconds(10));

        final Simulation simulation = Simulation.builder()
                .withBootstrapServers(this.kafka.getBootstrapServers())
                .withFlowTopic(options.getFlowSourceTopic())
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

        simulation.start();

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE_APPLICATION");

        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(() -> getFirstNFlowSummmariesFromES(1, options, query).get().size(), greaterThanOrEqualTo(1));

        Thread.sleep(100_000L);

        simulation.stop();
        simulation.join();

        t.interrupt();
        t.join();

        // get all documents from elastic search
        final List<FlowSummary> flowSummaries = new ArrayList<>(getFirstNFlowSummmariesFromES(10000, options, query).get());

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
                LOG.info(entry.getKey() + "@"+pointInTime+" --> sum: " + summaries.get(entry.getKey()).get(pointInTime));
                if (first) {
                    first = false;
                } else {
                    assertThat(summaries.get(entry.getKey()).get(pointInTime), longCloseTo(entry.getValue(), allowedError));
                }
            }
        }
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
}
