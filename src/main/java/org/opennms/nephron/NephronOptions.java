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

public interface NephronOptions extends PipelineOptions {

    String DEFAULT_FLOW_SOURCE_TOPIC = "opennms-flows";
    String DEFAULT_FLOW_DEST_TOPIC = "opennms-flows-agg";

    @Description("Kafka Bootstrap Servers")
    @Default.String("localhost:9092")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("Kafka Consumer Group ID")
    @Default.String("opennms-nephron")
    String getGroupId();

    void setGroupId(String value);

    @Description("Source topic for flows")
    @Default.String(DEFAULT_FLOW_SOURCE_TOPIC)
    String getFlowSourceTopic();

    void setFlowSourceTopic(String value);

    @Description("Destination topic for aggregated flows")
    String getFlowDestTopic();

    void setFlowDestTopic(String value);

    @Description("Size of the window i.e. 30s")
    @Default.String("10s")
    @Validation.Required
    String getFixedWindowSize();

    void setFixedWindowSize(String value);

    @Description("Top K")
    @Default.Integer(10)
    int getTopK();

    void setTopK(int value);

    @Description("Elasticsearch URL")
    @Default.String("http://localhost:9200")
    String getElasticUrl();

    void setElasticUrl(String value);

    @Description("Elasticsearch Index")
    @Default.String("aggregated-flows")
    String getElasticIndex();

    void setElasticIndex(String value);

}
