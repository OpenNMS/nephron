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

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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
import org.opennms.nephron.elastic.Context;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.elastic.GroupedBy;
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

    public static final String NETFLOW_AGG_INDEX_PREFIX = "netflow_agg-";

    private final RestHighLevelClient client;

    public NGFlowRepository(HttpHost host) {
        RestClientBuilder restClientBuilder = RestClient.builder(host);
        client = new RestHighLevelClient(restClientBuilder);
    }

    public void destroy() {
        try {
            client.close();
        } catch (IOException e) {
            LOG.warn("Exception occurred when closing the client.", e);
        }
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
        return getTopNSummary(N, includeOther, filters, GroupedBy.EXPORTER_INTERFACE_APPLICATION, "application", (app) -> {
            String effectiveApplicationName = app;
            if (FlowSummary.UNKNOWN_APPLICATION_NAME_KEY.equals(effectiveApplicationName)) {
                effectiveApplicationName = FlowSummary.UNKNOWN_APPLICATION_NAME_DISPLAY;
            }
            return effectiveApplicationName;
        }, FlowSummary.OTHER_APPLICATION_NAME_DISPLAY );
    }
    @Override
    public CompletableFuture<List<TrafficSummary<Host>>> getTopNHostSummaries(int N, boolean includeOther, List<Filter> filters) {
        return getTopNSummary(N, includeOther, filters, GroupedBy.EXPORTER_INTERFACE_HOST, "host_address",
                (host) -> Host.from(host).build(), Host.forOther().build() );
    }

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

    private <T> CompletableFuture<List<TrafficSummary<T>>> getTopNSummary(int N, boolean includeOther, List<Filter> filters,
                                                                         GroupedBy groupedBy, String key, Function<String,T> sumFunc, T otherEntity) {
        CompletableFuture<List<TrafficSummary<T>>> summaryFutures;
        if (N > 0) {
            CompletableFuture<SearchResponse> future = new CompletableFuture<>();
            SearchRequest searchRequest = new SearchRequest(getIndices(filters));
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.boolQuery()
                    .must(termQuery("grouped_by", groupedBy))
                    .must(termQuery("context", Context.TOPK)));
            sourceBuilder.size(0); // We don't need the hits - only the aggregations
            searchRequest.source(sourceBuilder);

            TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_key")
                    .size(N)
                    .order(BucketOrder.aggregation("bytes_total", false))
                    .field(key);
            // Track the total bytes for result ordering
            aggregation.subAggregation(AggregationBuilders.sum("bytes_total")
                    .field("bytes_total"));
            aggregation.subAggregation(AggregationBuilders.sum("bytes_ingress")
                    .field("bytes_ingress"));
            aggregation.subAggregation(AggregationBuilders.sum("bytes_egress")
                    .field("bytes_egress"));
            sourceBuilder.aggregation(aggregation);

            client.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
            summaryFutures = future.thenApply(s -> {
                List<TrafficSummary<T>> trafficSummaries = new ArrayList<>(N);
                Aggregations aggregations = s.getAggregations();
                Terms byApplicationAggregation = aggregations.get("by_key");
                for (Terms.Bucket bucket : byApplicationAggregation.getBuckets()) {
                    Aggregations sums = bucket.getAggregations();
                    Sum ingress = sums.get("bytes_ingress");
                    Sum egress = sums.get("bytes_egress");

                    trafficSummaries.add(TrafficSummary.<T>builder()
                            .withEntity(sumFunc.apply(bucket.getKeyAsString()))
                            .withBytesIn(((Double)ingress.getValue()).longValue())
                            .withBytesOut(((Double)egress.getValue()).longValue())
                            .build());
                }
                return trafficSummaries;
            });
        } else {
            summaryFutures = CompletableFuture.completedFuture(Collections.emptyList());
        }

        if (!includeOther) {
            return summaryFutures;
        }

        CompletableFuture<TrafficSummary<T>> totalTrafficFuture = getOtherTraffic(otherEntity, filters);
        return summaryFutures.thenCombine(totalTrafficFuture, (topK,total) -> {
            long bytesInRemainder = total.getBytesIn();
            long bytesOutRemainder = total.getBytesOut();
            for (TrafficSummary<?> topEl : topK) {
                bytesInRemainder -= topEl.getBytesIn();
                bytesOutRemainder -= topEl.getBytesOut();
            }

            List<TrafficSummary<T>> newTopK = new ArrayList<>(topK);
            newTopK.add(TrafficSummary.<T>builder()
                    .withEntity(otherEntity)
                    .withBytesIn(Math.max(bytesInRemainder, 0L))
                    .withBytesOut(Math.max(bytesOutRemainder, 0L))
                    .build());
            return newTopK;
        });
    }


    private <T> CompletableFuture<TrafficSummary<T>> getOtherTraffic(T entity, List<Filter> filters) {
        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        SearchRequest searchRequest = new SearchRequest(getIndices(filters));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.boolQuery()
                .must(termQuery("grouped_by", GroupedBy.EXPORTER_INTERFACE))
                .must(termQuery("context", Context.TOTAL)));
        sourceBuilder.size(0); // We don't need the hits - only the aggregations
        searchRequest.source(sourceBuilder);

        // Sum all ingress/egress
        sourceBuilder.aggregation(AggregationBuilders.sum("bytes_ingress")
                .field("bytes_ingress"));
        sourceBuilder.aggregation(AggregationBuilders.sum("bytes_egress")
                .field("bytes_egress"));

        client.searchAsync(searchRequest, RequestOptions.DEFAULT, toFuture(future));
        return future.thenApply(s -> {
            Sum ingress = s.getAggregations().get("bytes_ingress");
            Sum egress = s.getAggregations().get("bytes_egress");

            return TrafficSummary.<T>builder()
                    .withEntity(entity)
                    .withBytesIn(((Double)ingress.getValue()).longValue())
                    .withBytesOut(((Double)egress.getValue()).longValue())
                    .build();
        });
    }



    private String[] getIndices(List<Filter> filters) {
        // FIXME: Do smart index limiting
        return new String[]{NETFLOW_AGG_INDEX_PREFIX + "*"};
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
