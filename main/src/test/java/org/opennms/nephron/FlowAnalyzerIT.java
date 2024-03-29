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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.opennms.nephron.RandomFlowIT.createPipeline;
import static org.opennms.nephron.RandomFlowIT.runPipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.TestFlinkRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.flowgen.KafkaFlowGenerator;
import org.opennms.nephron.flowgen.SyntheticFlowBuilder;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;

/**
 * Complete end-to-end test - reading & writing to/from Kafka
 */
public class FlowAnalyzerIT {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Rule
    public ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.2");

    private RestHighLevelClient elasticClient;

    @Before
    public void setUp() throws IOException {
        HttpHost elasticHost = new HttpHost(elastic.getContainerIpAddress(), elastic.getMappedPort(9200), "http");
        RestClientBuilder restClientBuilder = RestClient.builder(elasticHost);
        elasticClient = new RestHighLevelClient(restClientBuilder);
        // Install the index mapping
        insertIndexMapping();
    }

    @After
    public void tearDown() throws IOException {
        if (elasticClient != null) {
            elasticClient.close();
        }
    }

    @Test
    public void canStreamIt() throws InterruptedException, ExecutionException {
        NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + kafka.getBootstrapServers(),
                "--fixedWindowSizeMs=10000",
                "--defaultMaxInputDelayMs=1000",
                "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class);
        options.setElasticUrl("http://" + elastic.getHttpHostAddress());

        Executor executor = Executors.newCachedThreadPool();

        // Create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        // Start our output consumer
        List<FlowSummary> allRecords = new LinkedList<>();
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(options.getFlowDestTopic()));
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (final ConsumerRecord<String, String> record : records) {
                        System.out.println("Got record: " + record);
                        try {
                            allRecords.add(objectMapper.readValue(record.value(), FlowSummary.class));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });

        // Fire up the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = createPipeline(options);

        // Now write some flows
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);
        KafkaFlowGenerator flowGenerator = new KafkaFlowGenerator(producer, options);
        final List<FlowDocument> flowsSent = new LinkedList<>();
        flowGenerator.setCallback(flowsSent::add);

        runPipeline(options, pipeline, () -> flowGenerator.run());

        // Wait until we've sent at least one flow
        await().atMost(1, TimeUnit.MINUTES).until(() -> flowsSent, hasSize(greaterThanOrEqualTo(1)));

        // Wait for some flow summaries to appear
        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
                .until(() -> allRecords, hasSize(greaterThanOrEqualTo(5)));

        // Wait for documents to be indexed in Elasticsearch
        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> getFirstNFlowSummmariesFromES(5, options).get(), hasSize(5));

        // We know there are document in ES let, let's retrieve one and validate the contents
        List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(5, options).get();
        assertThat(flowSummaries, hasSize(5));

        // Basic sanity check on the flow summary
        FlowSummary firstFlowSummary = flowSummaries.get(0);
        assertThat(firstFlowSummary.getRangeEndMs(), greaterThanOrEqualTo(firstFlowSummary.getRangeStartMs()));
    }

    @Test
    public void canHandleClockSkewIt() throws Exception {
        NephronOptions options = PipelineOptionsFactory.fromArgs("--bootstrapServers=" + this.kafka.getBootstrapServers(),
                                                                 "--parallelism=6",
                                                                 "--fixedWindowSizeMs=10000",
                                                                 "--allowedLatenessMs=300000",
                                                                 "--earlyProcessingDelayMs=2000",
                                                                 "--lateProcessingDelayMs=2000",
                                                                 "--flowDestTopic=opennms-flows-aggregated")
                .as(NephronOptions.class);
        options.setElasticUrl("http://" + elastic.getHttpHostAddress());
        options.as(FlinkPipelineOptions.class).setStreaming(true);

        // Create the topic
        createTopics(options.getFlowSourceTopic(), options.getFlowDestTopic());

        // create the pipeline
        final org.apache.beam.sdk.Pipeline pipeline = createPipeline(options);

        // Shift start to a controlled point in window (3 secs after window start) to avoid flapping test
        final Instant almostNow = Instant.ofEpochMilli(Instant.now().toEpochMilli() / 10_000L * 10_000L + 3_000L);

        final Instant timestamp1 = almostNow.plus(1, ChronoUnit.HOURS);
        final Instant timestamp2 = almostNow;
        final Instant timestamp3 = almostNow.minus(1, ChronoUnit.HOURS);

        runPipeline(options, pipeline, skewedFlowProducer(options, almostNow, timestamp1, timestamp2, timestamp3));

        final QueryBuilder query = QueryBuilders.termQuery("grouped_by", "EXPORTER_INTERFACE");

        await().atMost(2, TimeUnit.MINUTES).pollInterval(1, TimeUnit.SECONDS)
               .ignoreExceptions()
               .until(() -> getFirstNFlowSummmariesFromES(20, options, query).get(), hasSize(6));

        Thread.sleep(10_000);

        List<FlowSummary> flowSummaries = getFirstNFlowSummmariesFromES(20, options, query).get();
        assertThat(flowSummaries, hasSize(6));

        final Map<String, LongSummaryStatistics> summaries = flowSummaries.stream()
                .collect(Collectors.groupingBy(FlowAnalyzerIT::exporterAndInterface,
                        Collectors.summarizingLong(FlowSummary::getBytesTotal)));

        assertThat(summaries, is(aMapWithSize(3)));

        assertThat(summaries.get("Test:Router1-98").getCount(), is(2L));
        assertThat(summaries.get("Test:Router1-98").getSum(), is(300L));

        assertThat(summaries.get("Test:Router2-98").getCount(), is(2L));
        assertThat(summaries.get("Test:Router2-98").getSum(), is(300L));

        assertThat(summaries.get("Test:Router3-98").getCount(), is(2L));
        assertThat(summaries.get("Test:Router3-98").getSum(), is(300L));
    }

    private ThrowingRunnable skewedFlowProducer(
            NephronOptions options,
            Instant almostNow,
            Instant timestamp1,
            Instant timestamp2,
            Instant timestamp3
    ) {
        return () -> {

            // Now write some flows
            final Map<String, Object> producerProps = new HashMap<>();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

            final SyntheticFlowBuilder builder = new SyntheticFlowBuilder()
                    .withSnmpInterfaceId(98)
                    .withApplication("SomeApplication");

            builder.withExporter("Test", "Router1", 1)
                    .withFlow(timestamp1.plus(5, ChronoUnit.SECONDS), timestamp1.plus(11, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            50);

            builder.withExporter("Test", "Router2", 2)
                    .withFlow(timestamp2.plus(5, ChronoUnit.SECONDS), timestamp2.plus(11, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            50);

            builder.withExporter("Test", "Router3", 3)
                    .withFlow(timestamp3.plus(5, ChronoUnit.SECONDS), timestamp3.plus(11, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            50);

            builder.withExporter("Test", "Router1", 1)
                    .withFlow(timestamp1.plus(7, ChronoUnit.SECONDS), timestamp1.plus(12, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            100);

            builder.withExporter("Test", "Router2", 2)
                    .withFlow(timestamp2.plus(7, ChronoUnit.SECONDS), timestamp2.plus(12, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            100);

            builder.withExporter("Test", "Router3", 3)
                    .withFlow(timestamp3.plus(7, ChronoUnit.SECONDS), timestamp3.plus(12, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            100);

            builder.withExporter("Test", "Router1", 1)
                    .withFlow(timestamp1.plus(9, ChronoUnit.SECONDS), timestamp1.plus(14, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            150);

            builder.withExporter("Test", "Router2", 2)
                    .withFlow(timestamp2.plus(9, ChronoUnit.SECONDS), timestamp2.plus(14, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            150);

            builder.withExporter("Test", "Router3", 3)
                    .withFlow(timestamp3.plus(9, ChronoUnit.SECONDS), timestamp3.plus(14, ChronoUnit.SECONDS),
                            "10.0.0.1", 88,
                            "10.0.0.3", 99,
                            150);

            builder.withExporter("Test", "Buzz", 0)
                    .withFlow(timestamp1.plus(1, ChronoUnit.MINUTES), almostNow.plus(1, ChronoUnit.MINUTES),
                            "0.0.0.0", 0,
                            "0.0.0.0", 0,
                            2000);

            for (final FlowDocument flow : builder.build()) {
                producer.send(new ProducerRecord<>(options.getFlowSourceTopic(), flow.toByteArray()), (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }
        };
    }

    public static String exporterAndInterface(FlowSummary fs) {
        return fs.getExporter().getForeignSource() + ':' + fs.getExporter().getForeignId() + '-' + fs.getIfIndex();
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

    /**
     * Adapted from https://stackoverflow.com/questions/59164611/how-to-efficiently-create-kafka-topics-with-testcontainers
     *
     * @param topics topics to create
     */
    private void createTopics(String... topics) {
        List<NewTopic> newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        try (AdminClient admin = AdminClient.create(ImmutableMap.<String,Object>builder()
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

    private static <T> ActionListener<T> toFuture(CompletableFuture<T> future) {
        return new ActionListener<T>(){
            @Override
            public void onResponse(T result) {
                future.complete(result);
            }
            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        };
    };
}
