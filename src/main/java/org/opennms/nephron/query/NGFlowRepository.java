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

package org.opennms.nephron.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.opennms.nephron.NephronOptions;
import org.opennms.nephron.elastic.TopKFlow;
import org.opennms.netmgt.flows.api.Conversation;
import org.opennms.netmgt.flows.api.Directional;
import org.opennms.netmgt.flows.api.Flow;
import org.opennms.netmgt.flows.api.FlowRepository;
import org.opennms.netmgt.flows.api.FlowSource;
import org.opennms.netmgt.flows.api.Host;
import org.opennms.netmgt.flows.api.TrafficSummary;
import org.opennms.netmgt.flows.filter.api.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Table;

public class NGFlowRepository implements FlowRepository {
    private static final Logger LOG = LoggerFactory.getLogger(NGFlowRepository.class);

    private final RestHighLevelClient client;

    public NGFlowRepository(HttpHost host) {
        RestClientBuilder restClientBuilder = RestClient.builder(host);
        client = new RestHighLevelClient(restClientBuilder);
    }

    public void destroy() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String[] getIndices(List<Filter> filters) {
        // FIXME: Do smart index limiting
        return new String[]{"aggregated-flows-*"};
    }

    @Override
    public void persist(Collection<Flow> packets, FlowSource source) {
        throw new UnsupportedOperationException("Not here.");
    }

    @Override
    public CompletableFuture<Long> getFlowCount(List<Filter> filters) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(getIndices(filters));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        client.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
        return future.thenApply(s -> s.getHits().getTotalHits().value);
    }

    @Override
    public CompletableFuture<List<String>> getApplications(String matchingPrefix, long limit, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<TrafficSummary<String>>> getTopNApplicationSummaries(int N, boolean includeOther, List<Filter> filters) {
        if (includeOther) {
            throw new UnsupportedOperationException("Not supported yet - need custom accumulator - Top K + remainder");
        }

        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(getIndices(filters));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("grouped_by.application", true));
        sourceBuilder.size(0); // We don't need the hits - only the aggregations
        searchRequest.source(sourceBuilder);

        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_application")
                .size(NephronOptions.MAX_K + 3)
                .order(BucketOrder.aggregation("bytes_total", false))
                .field("application");
        // Track the total bytes for result ordering
        aggregation.subAggregation(AggregationBuilders.sum("bytes_total")
                .field("bytes_total"));
        aggregation.subAggregation(AggregationBuilders.sum("bytes_ingress")
                .field("bytes_ingress"));
        aggregation.subAggregation(AggregationBuilders.sum("bytes_egress")
                .field("bytes_egress"));
        sourceBuilder.aggregation(aggregation);

        client.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
        return future.thenApply(s -> {
            // Extract the aggregations and build a map of traffic summaries
            Map<String, TrafficSummary<String>> trafficSummaryByApplication = new LinkedHashMap<>();
            Aggregations aggregations = s.getAggregations();
            Terms byApplicationAggregation = aggregations.get("by_application");
            for (Terms.Bucket bucket : byApplicationAggregation.getBuckets()) {
                Aggregations sums = bucket.getAggregations();
                Sum ingress = sums.get("bytes_ingress");
                Sum egress = sums.get("bytes_egress");

                String effectiveApplicationName = bucket.getKeyAsString();
                if (TopKFlow.UNKNOWN_APPLICATION_NAME_KEY.equals(effectiveApplicationName)) {
                    effectiveApplicationName = TopKFlow.UNKNOWN_APPLICATION_NAME_DISPLAY;
                } else if (TopKFlow.OTHER_APPLICATION_NAME_KEY.equals(effectiveApplicationName)) {
                    effectiveApplicationName = TopKFlow.OTHER_APPLICATION_NAME_DISPLAY;
                }

                TrafficSummary<String> trafficSummary = TrafficSummary.<String>builder()
                        .withEntity(effectiveApplicationName)
                        .withBytesIn(((Double)ingress.getValue()).longValue())
                        .withBytesOut(((Double)egress.getValue()).longValue())
                        .build();

                trafficSummaryByApplication.put(trafficSummary.getEntity(), trafficSummary);
            }

            // Nothing to do
            if (trafficSummaryByApplication.isEmpty()) {
                return Collections.emptyList();
            }

            // Remove the "other" bucket if we're not supposed to include it
            if (!includeOther) {
                trafficSummaryByApplication.remove(TopKFlow.OTHER_APPLICATION_NAME_KEY);
            }

            List<TrafficSummary<String>> apps = new ArrayList<>(trafficSummaryByApplication.values());

            int targetSize = N;
            targetSize += includeOther ? 0 : 1;
            if (trafficSummaryByApplication.size() > targetSize) {
                // TODO: We need to further reduce the results
                apps = apps.subList(0, Math.min(N, apps.size()));
            }

            return apps;
        });
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

    @Override
    public CompletableFuture<List<TrafficSummary<String>>> getApplicationSummaries(Set<String> applications, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<String>, Long, Double>> getApplicationSeries(Set<String> applications, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<String>, Long, Double>> getTopNApplicationSeries(int N, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getConversations(String locationPattern, String protocolPattern, String lowerIPPattern, String upperIPPattern, String applicationPattern, long limit, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<TrafficSummary<Conversation>>> getTopNConversationSummaries(int N, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<TrafficSummary<Conversation>>> getConversationSummaries(Set<String> conversations, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<Conversation>, Long, Double>> getConversationSeries(Set<String> conversations, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<Conversation>, Long, Double>> getTopNConversationSeries(int N, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getHosts(String regex, long limit, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<TrafficSummary<Host>>> getTopNHostSummaries(int N, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<List<TrafficSummary<Host>>> getHostSummaries(Set<String> hosts, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<Host>, Long, Double>> getHostSeries(Set<String> hosts, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }

    @Override
    public CompletableFuture<Table<Directional<Host>, Long, Double>> getTopNHostSeries(int N, long step, boolean includeOther, List<Filter> filters) {
        return null;
    }
}
