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

package org.opennms.nephron.elastic;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FlowSummary {

    public static final String UNKNOWN_APPLICATION_NAME_KEY = "__unknown";
    public static final String UNKNOWN_APPLICATION_NAME_DISPLAY = "Unknown";

    public static final String OTHER_APPLICATION_NAME_KEY = "__other";
    public static final String OTHER_APPLICATION_NAME_DISPLAY = "Other";

    @JsonProperty("@timestamp")
    private long timestamp;

    @JsonProperty("range_start")
    private long rangeStartMs;

    @JsonProperty("range_end")
    private long rangeEndMs;

    @JsonProperty("ranking")
    private int ranking;

    @JsonProperty("grouped_by")
    private GroupedBy groupedBy;

    @JsonProperty("context")
    private Context context;

    @JsonProperty("bytes_ingress")
    private Long bytesIngress;

    @JsonProperty("bytes_egress")
    private Long bytesEgress;

    @JsonProperty("bytes_total")
    private Long bytesTotal;

    @JsonProperty("exporter")
    private ExporterNode exporter;

    @JsonProperty("if_index")
    private Integer ifIndex;

    @JsonProperty("application")
    private String application;

    @JsonProperty("host_address")
    private String hostAddress;

    @JsonProperty("host_name")
    private String hostName;

    @JsonProperty("conversation_key")
    private String conversationKey;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getRangeStartMs() {
        return rangeStartMs;
    }

    public void setRangeStartMs(long rangeStartMs) {
        this.rangeStartMs = rangeStartMs;
    }

    public long getRangeEndMs() {
        return rangeEndMs;
    }

    public void setRangeEndMs(long rangeEndMs) {
        this.rangeEndMs = rangeEndMs;
    }

    public int getRanking() {
        return ranking;
    }

    public void setRanking(int ranking) {
        this.ranking = ranking;
    }

    public GroupedBy getGroupedBy() {
        return groupedBy;
    }

    public void setGroupedBy(GroupedBy groupedBy) {
        this.groupedBy = groupedBy;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public Long getBytesIngress() {
        return bytesIngress;
    }

    public void setBytesIngress(Long bytesIngress) {
        this.bytesIngress = bytesIngress;
    }

    public Long getBytesEgress() {
        return bytesEgress;
    }

    public void setBytesEgress(Long bytesEgress) {
        this.bytesEgress = bytesEgress;
    }

    public Long getBytesTotal() {
        return bytesTotal;
    }

    public void setBytesTotal(Long bytesTotal) {
        this.bytesTotal = bytesTotal;
    }

    public Integer getIfIndex() {
        return ifIndex;
    }

    public void setIfIndex(Integer ifIndex) {
        this.ifIndex = ifIndex;
    }

    public ExporterNode getExporter() {
        return exporter;
    }

    public void setExporter(ExporterNode exporter) {
        this.exporter = exporter;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getConversationKey() {
        return conversationKey;
    }

    public void setConversationKey(String conversationKey) {
        this.conversationKey = conversationKey;
    }
}
