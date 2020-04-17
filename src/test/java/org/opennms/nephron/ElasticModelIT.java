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
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.elastic.IndexStrategy;
import org.opennms.nephron.query.NGFlowRepository;
import org.opennms.netmgt.flows.api.Conversation;
import org.opennms.netmgt.flows.api.Host;
import org.opennms.netmgt.flows.api.TrafficSummary;
import org.opennms.netmgt.flows.filter.api.Filter;
import org.opennms.netmgt.flows.filter.api.SnmpInterfaceIdFilter;
import org.opennms.netmgt.flows.filter.api.TimeRangeFilter;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

public class ElasticModelIT {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @ClassRule
    public static ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.2");

    private HttpHost elasticHost;

    private RestHighLevelClient client;
    private NGFlowRepository flowRepository;

    @Before
    public void setUp() throws IOException {
       // elasticHost = new HttpHost("maas-m1-demo.opennms.com", 9200, "http");
        elasticHost = new HttpHost(elastic.getContainerIpAddress(), elastic.getMappedPort(9200), "http");
        RestClientBuilder restClientBuilder = RestClient.builder(elasticHost);
        client = new RestHighLevelClient(restClientBuilder);

        deleteExistingIndices();
        insertIndexMapping();

        flowRepository = new NGFlowRepository(elasticHost);
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
        flowRepository.destroy();
    }

    private void insertIndexMapping() throws IOException {
        Request request = new Request("PUT", "_template/netflow_agg");
        request.setJsonEntity(Resources.toString(Resources.getResource("aggregated-flows-template.json"), StandardCharsets.UTF_8));
        Response response = client.getLowLevelClient().performRequest(request);
        assertThat(response.getWarnings(), hasSize(0));
    }

    private void deleteExistingIndices() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
        deleteIndexRequest.indices(NGFlowRepository.NETFLOW_AGG_INDEX_PREFIX + "-*");
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void canIndexDocument() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        FlowSummary flowSummary = new FlowSummary();

        IndexRequest request = new IndexRequest("aggregated-flows-2020");
        request.source(mapper.writeValueAsString(flowSummary), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        assertThat(response.getId(), notNullValue());
    }

    @Test
    public void canGetTopKApplications() throws ExecutionException, InterruptedException {
        // Take some flows
        final List<FlowDocument> flows = defaultFlows();

        // Pass those same flows through the pipeline and persist the aggregations
        NephronOptions options = PipelineOptionsFactory.as(NephronOptions.class);
        options.setFixedWindowSize("30s");
        doPipeline(flows, options);

        // Verify
        await().atMost(1, TimeUnit.MINUTES).until(() ->
                flowRepository.getTopNApplicationSummaries(10, true, getFilters()).get(), hasSize(4));

        // Retrieve the Top N applications over the entire time range
        List<TrafficSummary<String>> appTrafficSummary = flowRepository.getTopNApplicationSummaries(10, true, getFilters()).get();

        // Expect all of the applications, with the sum of all the bytes from all the flows
        assertThat(appTrafficSummary, hasSize(4));
        TrafficSummary<String> https = appTrafficSummary.get(0);
        assertThat(https.getEntity(), equalTo("https"));
        assertThat(https.getBytesIn(), equalTo(210L));
        assertThat(https.getBytesOut(), equalTo(2100L));

        // Unclassified applications should show up too
        TrafficSummary<String> unknown = appTrafficSummary.get(1);
        assertThat(unknown.getEntity(), equalTo("Unknown"));
        assertThat(unknown.getBytesIn(), equalTo(200L));
        assertThat(unknown.getBytesOut(), equalTo(100L));

        TrafficSummary<String> http = appTrafficSummary.get(2);
        assertThat(http.getEntity(), equalTo("http"));
        assertThat(http.getBytesIn(), equalTo(10L));
        assertThat(http.getBytesOut(), equalTo(100L));

        TrafficSummary<String> other = appTrafficSummary.get(3);
        assertThat(other.getEntity(), equalTo("Other"));
        assertThat(other.getBytesIn(), equalTo(0L));
        assertThat(other.getBytesOut(), equalTo(0L));

        // Now decrease N, expect all of the counts to pool up in "Other"
        appTrafficSummary = flowRepository.getTopNApplicationSummaries(1, true, getFilters()).get();

        // Expect all of the applications, with the sum of all the bytes from all the flows
        assertThat(appTrafficSummary, hasSize(2));
        https = appTrafficSummary.get(0);
        assertThat(https.getEntity(), equalTo("https"));
        assertThat(https.getBytesIn(), equalTo(210L));
        assertThat(https.getBytesOut(), equalTo(2100L));

        other = appTrafficSummary.get(1);
        assertThat(other.getEntity(), equalTo("Other"));
        assertThat(other.getBytesIn(), equalTo(210L));
        assertThat(other.getBytesOut(), equalTo(200L));

        // Now set N to zero
        appTrafficSummary = flowRepository.getTopNApplicationSummaries(0, false, getFilters()).get();
        assertThat(appTrafficSummary, hasSize(0));

        // N=0, but include other
        appTrafficSummary = flowRepository.getTopNApplicationSummaries(0, true, getFilters()).get();
        assertThat(appTrafficSummary, hasSize(1));

        other = appTrafficSummary.get(0);
        assertThat(other.getEntity(), equalTo("Other"));
        assertThat(other.getBytesIn(), equalTo(420L));
        assertThat(other.getBytesOut(), equalTo(2300L));
    }

    @Test
    public void canGetTopKHosts() throws ExecutionException, InterruptedException {
        // Take some flows
        final List<FlowDocument> flows = defaultFlows();

        // Pass those same flows through the pipeline and persist the aggregations
        NephronOptions options = PipelineOptionsFactory.as(NephronOptions.class);
        options.setFixedWindowSize("30s");
        doPipeline(flows, options);

        // Verify
        await().atMost(1, TimeUnit.MINUTES).until(() -> flowRepository.getTopNHostSummaries(10, false, getFilters()).get(), hasSize(6));

        // Retrieve the Top N applications over the entire time range
        List<TrafficSummary<Host>> hostTrafficSummary = flowRepository.getTopNHostSummaries(10, false, getFilters()).get();

        // Expect all of the hosts, with the sum of all the bytes from all the flows
        assertThat(hostTrafficSummary, hasSize(6));
        TrafficSummary<Host> top = hostTrafficSummary.get(0);
        assertThat(top.getEntity(), equalTo(new Host("10.1.1.12")));
        assertThat(top.getBytesIn(), equalTo(210L));
        assertThat(top.getBytesOut(), equalTo(2100L));

        TrafficSummary<Host> bottom = hostTrafficSummary.get(5);
        assertThat(bottom.getEntity(), equalTo(new Host("10.1.1.11")));
        assertThat(bottom.getBytesIn(), equalTo(10L));
        assertThat(bottom.getBytesOut(), equalTo(100L));

        // Now decrease N, expect all of the counts to pool up in "Other"
        hostTrafficSummary = flowRepository.getTopNHostSummaries(1, true, getFilters()).get();

        // Expect two summaries
        assertThat(hostTrafficSummary, hasSize(2));
        top = hostTrafficSummary.get(0);
        assertThat(top.getEntity(), equalTo(new Host("10.1.1.12")));
        assertThat(top.getBytesIn(), equalTo(210L));
        assertThat(top.getBytesOut(), equalTo(2100L));

        TrafficSummary<Host> other = hostTrafficSummary.get(1);
        assertThat(other.getEntity(), equalTo(new Host("Other")));
        assertThat(other.getBytesIn(), equalTo(210L));
        assertThat(other.getBytesOut(), equalTo(200L));

        // Now set N to zero
        hostTrafficSummary = flowRepository.getTopNHostSummaries(0, false, getFilters()).get();
        assertThat(hostTrafficSummary, hasSize(0));

        // N=0, but include other
        hostTrafficSummary = flowRepository.getTopNHostSummaries(0, true, getFilters()).get();
        assertThat(hostTrafficSummary, hasSize(1));
        other = hostTrafficSummary.get(0);
        assertThat(other.getEntity(), equalTo(new Host("Other")));
        assertThat(other.getBytesIn(), equalTo(420L));
        assertThat(other.getBytesOut(), equalTo(2300L));
    }

    @Test
    public void canGetTopNConversationSummaries() throws Exception {
        // Take some flows
        final List<FlowDocument> flows = defaultFlows();

        // Pass those same flows through the pipeline and persist the aggregations
        NephronOptions options = PipelineOptionsFactory.as(NephronOptions.class);
        options.setFixedWindowSize("30s");
        doPipeline(flows, options);

        // Verify
        await().atMost(1, TimeUnit.MINUTES).until(() -> flowRepository.getTopNConversationSummaries(2, false, getFilters()).get(), hasSize(2));

        // Retrieve the Top N conversation over the entire time range
        List<TrafficSummary<Conversation>> convoTrafficSummary = flowRepository.getTopNConversationSummaries(2, false, getFilters()).get();
        assertThat(convoTrafficSummary, hasSize(2));

        // Expect the conversations, with the sum of all the bytes from all the flows
        TrafficSummary<Conversation> convo = convoTrafficSummary.get(0);
        assertThat(convo.getEntity().getLowerIp(), equalTo("10.1.1.12"));
        assertThat(convo.getEntity().getUpperIp(), equalTo("192.168.1.101"));
//        assertThat(convo.getEntity().getLowerHostname(), equalTo(Optional.of("la.le.lu")));
//        assertThat(convo.getEntity().getUpperHostname(), equalTo(Optional.of("ingress.only")));
        assertThat(convo.getEntity().getApplication(), equalTo("https"));
        assertThat(convo.getBytesIn(), equalTo(110L));
        assertThat(convo.getBytesOut(), equalTo(1100L));

        convo = convoTrafficSummary.get(1);
        assertThat(convo.getEntity().getLowerIp(), equalTo("10.1.1.12"));
        assertThat(convo.getEntity().getUpperIp(), equalTo("192.168.1.100"));
//        assertThat(convo.getEntity().getLowerHostname(), equalTo(Optional.of("la.le.lu")));
//        assertThat(convo.getEntity().getUpperHostname(), equalTo(Optional.empty()));
        assertThat(convo.getEntity().getApplication(), equalTo("https"));
        assertThat(convo.getBytesIn(), equalTo(100L));
        assertThat(convo.getBytesOut(), equalTo(1000L));

        // Get the top 1 plus others
        convoTrafficSummary = flowRepository.getTopNConversationSummaries(1, true, getFilters()).get();
        assertThat(convoTrafficSummary, hasSize(2));

        convo = convoTrafficSummary.get(0);
        assertThat(convo.getEntity().getLowerIp(), equalTo("10.1.1.12"));
        assertThat(convo.getEntity().getUpperIp(), equalTo("192.168.1.101"));
        assertThat(convo.getEntity().getApplication(), equalTo("https"));
        assertThat(convo.getBytesIn(), equalTo(110L));
        assertThat(convo.getBytesOut(), equalTo(1100L));

        convo = convoTrafficSummary.get(1);
        assertThat(convo.getEntity(), equalTo(Conversation.forOther().build()));
        assertThat(convo.getBytesIn(), equalTo(310L));
        assertThat(convo.getBytesOut(), equalTo(1200L));
    }

    private void doPipeline(List<FlowDocument> flows, NephronOptions options) {
        FlowAnalyzer.registerCoders(p);

        // Build a stream from the given set of flows
        long timestampOffsetMillis = TimeUnit.MINUTES.toMillis(1);
        TestStream.Builder<FlowDocument> flowStreamBuilder = TestStream.create(new FlowAnalyzer.FlowDocumentProtobufCoder());
        for (FlowDocument flow : flows) {
            flowStreamBuilder = flowStreamBuilder.addElements(TimestampedValue.of(flow,
                    org.joda.time.Instant.ofEpochMilli(flow.getLastSwitched().getValue() + timestampOffsetMillis)));
        }
        TestStream<FlowDocument> flowStream = flowStreamBuilder.advanceWatermarkToInfinity();

        // Build the pipeline
        options.setElasticUrl(elasticHost.toURI());
        p.apply(flowStream)
                .apply(new FlowAnalyzer.CalculateFlowStatistics(options.getFixedWindowSize(), options.getTopK()))
                .apply(new FlowAnalyzer.WriteToElasticsearch(options.getElasticUrl(), options.getElasticIndex(), IndexStrategy.DAILY));

        // Run the pipeline until completion
        p.run().waitUntilFinish();
    }

    private List<Filter> getFilters(Filter... filters) {
        final List<Filter> filterList = Lists.newArrayList(filters);
        filterList.add(new TimeRangeFilter(0, System.currentTimeMillis()));
        // Match the SNMP interface id in the flows
        filterList.add(new SnmpInterfaceIdFilter(98));
        return filterList;
    }

    private static List<FlowDocument> defaultFlows() {
        return new SyntheticFlowBuilder()
                .withExporter("SomeFs", "SomeFid", 99)
                .withSnmpInterfaceId(98)
                // 192.168.1.100:43444 <-> 10.1.1.11:80 (110 bytes in [3,15])
                .withApplication("http")
                .withDirection(Direction.INGRESS)
                .withFlow(Instant.ofEpochMilli(3), Instant.ofEpochMilli(15), "192.168.1.100", 43444, "10.1.1.11", 80, 10)
                .withDirection(Direction.EGRESS)
                .withFlow(Instant.ofEpochMilli(3), Instant.ofEpochMilli(15), "10.1.1.11", 80, "192.168.1.100", 43444, 100)
                // 192.168.1.100:43445 <-> 10.1.1.12:443 (1100 bytes in [13,26])
                .withApplication("https")
                .withDirection(Direction.INGRESS)
                .withHostnames(null, "la.le.lu")
                .withFlow(Instant.ofEpochMilli(13), Instant.ofEpochMilli(26), "192.168.1.100", 43445, "10.1.1.12", 443, 100)
                .withDirection(Direction.EGRESS)
                .withHostnames("la.le.lu", null)
                .withFlow(Instant.ofEpochMilli(13), Instant.ofEpochMilli(26), "10.1.1.12", 443, "192.168.1.100", 43445, 1000)
                // 192.168.1.101:43442 <-> 10.1.1.12:443 (1210 bytes in [14, 45])
                .withApplication("https")
                .withDirection(Direction.INGRESS)
                .withHostnames("ingress.only", "la.le.lu")
                .withFlow(Instant.ofEpochMilli(14), Instant.ofEpochMilli(45), "192.168.1.101", 43442, "10.1.1.12", 443, 110)
                .withDirection(Direction.EGRESS)
                .withHostnames("la.le.lu", null)
                .withFlow(Instant.ofEpochMilli(14), Instant.ofEpochMilli(45), "10.1.1.12", 443, "192.168.1.101", 43442, 1100)
                // 192.168.1.102:50000 <-> 10.1.1.13:50001 (200 bytes in [50, 52])
                .withApplication(null) // Unknown
                .withDirection(Direction.INGRESS)
                .withFlow(Instant.ofEpochMilli(50), Instant.ofEpochMilli(52), "192.168.1.102", 50000, "10.1.1.13", 50001, 200)
                .withDirection(Direction.EGRESS)
                .withFlow(Instant.ofEpochMilli(50), Instant.ofEpochMilli(52), "10.1.1.13", 50001, "192.168.1.102", 50000, 100)
                .build();
    }

}
