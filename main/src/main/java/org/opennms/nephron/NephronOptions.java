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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.opennms.nephron.elastic.IndexStrategy;

public interface NephronOptions extends PipelineOptions {

    String DEFAULT_NETFLOW_AGG_INDEX_PREFIX = "netflow_agg";
    String DEFAULT_FLOW_SOURCE_TOPIC = "opennms-flows";

    @Description("Kafka Bootstrap Servers")
    @Default.String("localhost:9092")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("Kafka Consumer Group ID")
    @Default.String("opennms-nephron")
    String getGroupId();

    void setGroupId(String value);

    @Description("Enable/disable auto-commit for the Kafka consumer. Should be enabled when checkpointing is disabled.")
    @Default.Boolean(true)
    boolean getAutoCommit();

    void setAutoCommit(boolean value);

    @Description("Source topic for flows")
    @Default.String(DEFAULT_FLOW_SOURCE_TOPIC)
    String getFlowSourceTopic();

    void setFlowSourceTopic(String value);

    @Description("Destination topic for aggregated flows")
    String getFlowDestTopic();

    void setFlowDestTopic(String value);

    @Description("Size of the window in milliseconds")
    @Default.Long(60 * 1000L)
    @Validation.Required
    long getFixedWindowSizeMs();

    void setFixedWindowSizeMs(long value);

    @Description("Top K")
    @Default.Integer(10)
    int getTopK();

    void setTopK(int value);

    @Description("Elasticsearch URL")
    @Default.String("http://localhost:9200")
    String getElasticUrl();

    void setElasticUrl(String value);

    @Description("Elasticsearch Username")
    String getElasticUser();

    void setElasticUser(String value);

    @Description("Elasticsearch Password")
    String getElasticPassword();

    void setElasticPassword(String value);

    @Description("Elasticsearch Index Strategy")
    @Default.Enum("MONTHLY")
    IndexStrategy getElasticIndexStrategy();

    void setElasticIndexStrategy(IndexStrategy value);

    @Description("Elasticsearch Flow Index")
    @Default.String(DEFAULT_NETFLOW_AGG_INDEX_PREFIX)
    String getElasticFlowIndex();

    void setElasticFlowIndex(String value);

    @Description("Max input delay in milliseconds. Messages received from a Kafka topic are expected to be delayed" +
            " by no more than this duration when compared to the latest timestamp observed, or the current time if " +
            "there is no backlog.")
    @Default.Long(2 * 60 * 1000L) // 2 minutes
    long getDefaultMaxInputDelayMs();

    void setDefaultMaxInputDelayMs(long value);

    @Description("Amount of time to wait before firing the pane after late data has arrived." +
            "Decrease this value for faster updates, at the cost of more update being fired.")
    @Default.Long(60 * 1000L) // 1 minute
    long getLateProcessingDelayMs();

    void setLateProcessingDelayMs(long value);

    @Description("Max amount of time to wait for late flows to appear. " +
            "Changing this value will affect state size (less state to keep for smaller values) and the ability to process late data.")
    @Default.Long(4 * 60 * 60 * 1000L) // 4 hours
    long getAllowedLatenessMs();

    void setAllowedLatenessMs(long value);

    @Description("Elasticsearch Connection Timeout in milliseconds")
    @Default.Integer(30 * 1000) // 30 seconds
    int getElasticConnectTimeout();

    void setElasticConnectTimeout(int value);

    @Description("Elasticsearch Socket Timeout in milliseconds")
    @Default.Integer(30 * 1000) // 30 seconds
    int getElasticSocketTimeout();

    void setElasticSocketTimeout(int value);

    @Description("Elasticsearch Retry Count")
    @Default.Integer(3) // 3 Retries
    int getElasticRetryCount();

    void setElasticRetryCount(int value);

    @Description("Elasticsearch Retry Duration")
    @Default.Long(3 * 1000L) // 3 Seconds
    long getElasticRetryDuration();

    void setElasticRetryDuration(long value);
}
