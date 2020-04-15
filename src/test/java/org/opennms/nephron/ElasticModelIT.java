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
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpHost;
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
import org.junit.Test;
import org.opennms.nephron.elastic.TopKFlow;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;

public class ElasticModelIT {

//    @Rule
//    public ElasticsearchContainer elastic = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.6.2");

    private RestHighLevelClient client;

    @Before
    public void setUp() throws IOException {
     //   RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elastic.getContainerIpAddress(), elastic.getMappedPort(9200), "http"));
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("maas-m1-demo.opennms.com", 9200, "http"));
        client = new RestHighLevelClient(restClientBuilder);
        insertIndexMapping();
    }

    @After
    public void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    private void insertIndexMapping() throws IOException {
        Request request = new Request("PUT", "_template/aggregated-flows");
        request.setJsonEntity(Resources.toString(Resources.getResource("aggregated-flows-template.json"), StandardCharsets.UTF_8));
        Response response = client.getLowLevelClient().performRequest(request);
        assertThat(response.getWarnings(), hasSize(0));
    }

    @Test
    public void canIndexDocument() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        TopKFlow topKFlow = new TopKFlow();

        IndexRequest request = new IndexRequest("aggregated-flows-2020");
        request.source(mapper.writeValueAsString(topKFlow), XContentType.JSON);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        assertThat(response.getId(), notNullValue());
    }

    @Test
    public void canIndexAndRenderSeries() throws IOException {
        // Take some flows

        // Index the raw flows into ES

        // Pass those same flows through the pipeline and persist the aggregations

        // Issue queries against both data sets

        // Expect results to be close by some margin

        // MUST be able to render series with a single query

        // Top N applications by ifIndex, by time
        //   Filter by time, range_start, range_end
        //   Filter by exporter & ifindex
        //   filter by group_by = {export:X, ifIndex:Y, iafd:Y}
        //   Group by application - there's only 1 result per application per range
        //   Generate time series (time, app, in, out)
    }

}
