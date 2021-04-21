package org.opennms.nephron;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class NephronSslIT {
    private static final Logger LOG = LoggerFactory.getLogger(NephronSslIT.class);

    @Rule
    public KafkaSSLContainer kafka = new KafkaSSLContainer();

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

    private void createTopics(Properties properties, String... topics) {
        final List<NewTopic> newTopics =
                Arrays.stream(topics)
                        .map(topic -> new NewTopic(topic, 1, (short) 1))
                        .collect(Collectors.toList());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(newTopics);
        }
    }

    @Test
    public void canStreamViaSsl() throws InterruptedException, ExecutionException, Exception {
        final File temporaryFile = File.createTempFile("client-ssl-", ".properties");

        final Properties p = new Properties();
        p.put("security.protocol", "SSL");
        p.put("ssl.truststore.location", getClass().getResource("/ssl/client.truststore").getFile());
        p.put("ssl.truststore.password", "123456");
        p.put("ssl.keystore.location", getClass().getResource("/ssl/client.keystore").getFile());
        p.put("ssl.keystore.password", "123456");
        p.put("ssl.key.password", "123456");

        p.store(new FileWriter(temporaryFile), "Client SSL properties");

        System.err.println(kafka.getBootstrapServers());
        System.err.println(temporaryFile.getAbsolutePath());

        NephronOptions options = PipelineOptionsFactory.fromArgs(
                "--bootstrapServers=" + kafka.getBootstrapServers(),
                "--fixedWindowSizeMs=10000",
                "--defaultMaxInputDelayMs=1000",
                "--flowDestTopic=opennms-flows-aggregated",
                "--kafkaClientProperties=" + temporaryFile.getAbsolutePath())
                .as(NephronOptions.class);
        options.setElasticUrl("http://" + elastic.getHttpHostAddress());

        Executor executor = Executors.newCachedThreadPool();

        // Create the topic
        createTopics(p, options.getFlowSourceTopic(), options.getFlowDestTopic());

        // Start our output consumer
        List<FlowSummary> allRecords = new LinkedList<>();
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.putAll(p.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue())));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
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
        final org.apache.beam.sdk.Pipeline pipeline = Pipeline.create(options);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    pipeline.run();
                } catch (RuntimeException ex) {
                    if (ex.getCause() instanceof InterruptedException) {
                        return;
                    }
                    ex.printStackTrace();
                }
            }
        });
        t.start();
        // Wait until the pipeline's Kafka consumer has started
        Thread.sleep(10 * 1000);

        // Now write some flows
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProps.putAll(p.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue())));

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

        KafkaFlowGenerator flowGenerator = new KafkaFlowGenerator(producer, options);
        executor.execute(flowGenerator);

        // Wait until we've sent at least one flow
        final List<FlowDocument> flowsSent = new LinkedList<>();
        flowGenerator.setCallback(flowsSent::add);
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
        assertThat(firstFlowSummary.getGroupedByKey(), notNullValue());
        assertThat(firstFlowSummary.getRangeEndMs(), greaterThanOrEqualTo(firstFlowSummary.getRangeStartMs()));
        assertThat(firstFlowSummary.getRanking(), greaterThanOrEqualTo(0));

        t.interrupt();
        t.join();
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

    private void insertIndexMapping() throws IOException {
        Request request = new Request("PUT", "_template/netflow_agg");
        request.setJsonEntity(Resources.toString(Resources.getResource("netflow_agg-template.json"), StandardCharsets.UTF_8));
        Response response = elasticClient.getLowLevelClient().performRequest(request);
        assertThat(response.getWarnings(), hasSize(0));
    }
}
