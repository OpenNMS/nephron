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

import java.util.Objects;

import org.opennms.nephron.Groupings;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class FlowSummary {

    public static final String UNKNOWN_APPLICATION_NAME_KEY = "__unknown";

    @JsonProperty("_id")
    private String id;

    @JsonProperty("@timestamp")
    private long timestamp;

    @JsonProperty("range_start")
    private long rangeStartMs;

    @JsonProperty("range_end")
    private long rangeEndMs;

    @JsonProperty("ranking")
    private int ranking;

    @JsonProperty("grouped_by")
    private Groupings.CompoundKeyType groupedBy;

    @JsonProperty("grouped_by_key")
    private String groupedByKey;

    @JsonProperty("aggregation_type")
    private AggregationType aggregationType;

    @JsonProperty("bytes_ingress")
    private Long bytesIngress;

    @JsonProperty("bytes_egress")
    private Long bytesEgress;

    @JsonProperty("bytes_total")
    private Long bytesTotal;

    @JsonProperty("dscp")
    private Integer dscp;

    @JsonProperty("ecn")
    private Integer ecn;

    @JsonProperty("congestion_encountered")
    private Boolean congestionEncountered;

    @JsonProperty("non_ect")
    private Boolean nonEcnCapableTransport;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getGroupedByKey() {
        return groupedByKey;
    }

    public void setGroupedByKey(String groupedByKey) {
        this.groupedByKey = groupedByKey;
    }

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

    public Groupings.CompoundKeyType getGroupedBy() {
        return groupedBy;
    }

    public void setGroupedBy(Groupings.CompoundKeyType groupedBy) {
        this.groupedBy = groupedBy;
    }

    public AggregationType getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(AggregationType aggregationType) {
        this.aggregationType = aggregationType;
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

    public Integer getDscp() {
        return dscp;
    }

    public void setDscp(Integer dscp) {
        this.dscp = dscp;
    }

    public Integer getEcn() {
        return ecn;
    }

    public void setEcn(Integer ecn) {
        this.ecn = ecn;
    }

    public Boolean getCongestionEncountered() {
        return congestionEncountered;
    }

    public void setCongestionEncountered(Boolean congestionEncountered) {
        this.congestionEncountered = congestionEncountered;
    }

    public Boolean getNonEcnCapableTransport() {
        return nonEcnCapableTransport;
    }

    public void setNonEcnCapableTransport(Boolean nonEcnCapableTransport) {
        this.nonEcnCapableTransport = nonEcnCapableTransport;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlowSummary that = (FlowSummary) o;
        return timestamp == that.timestamp &&
               rangeStartMs == that.rangeStartMs &&
               rangeEndMs == that.rangeEndMs &&
               ranking == that.ranking &&
               Objects.equals(id, that.id) &&
               groupedBy == that.groupedBy &&
               Objects.equals(groupedByKey, that.groupedByKey) &&
               aggregationType == that.aggregationType &&
               Objects.equals(bytesIngress, that.bytesIngress) &&
               Objects.equals(bytesEgress, that.bytesEgress) &&
               Objects.equals(bytesTotal, that.bytesTotal) &&
               Objects.equals(dscp, that.dscp) &&
               Objects.equals(ecn, that.ecn) &&
               Objects.equals(congestionEncountered, that.congestionEncountered) &&
               Objects.equals(nonEcnCapableTransport, that.nonEcnCapableTransport) &&
               Objects.equals(exporter, that.exporter) &&
               Objects.equals(ifIndex, that.ifIndex) &&
               Objects.equals(application, that.application) &&
               Objects.equals(hostAddress, that.hostAddress) &&
               Objects.equals(hostName, that.hostName) &&
               Objects.equals(conversationKey, that.conversationKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, rangeStartMs, rangeEndMs, ranking, groupedBy, groupedByKey, aggregationType, bytesIngress, bytesEgress, bytesTotal, dscp, ecn, congestionEncountered, nonEcnCapableTransport, exporter, ifIndex, application, hostAddress, hostName, conversationKey);
    }

    @Override
    public String toString() {
        return "FlowSummary{" +
               "id='" + id + '\'' +
               ", timestamp=" + timestamp +
               ", rangeStartMs=" + rangeStartMs +
               ", rangeEndMs=" + rangeEndMs +
               ", ranking=" + ranking +
               ", groupedBy=" + groupedBy +
               ", groupedByKey='" + groupedByKey + '\'' +
               ", aggregationType=" + aggregationType +
               ", bytesIngress=" + bytesIngress +
               ", bytesEgress=" + bytesEgress +
               ", bytesTotal=" + bytesTotal +
               ", dscp=" + dscp +
               ", ecn=" + ecn +
               ", congestionEncountered=" + congestionEncountered +
               ", nonEcnCapableTransport=" + nonEcnCapableTransport +
               ", exporter=" + exporter +
               ", ifIndex=" + ifIndex +
               ", application='" + application + '\'' +
               ", hostAddress='" + hostAddress + '\'' +
               ", hostName='" + hostName + '\'' +
               ", conversationKey='" + conversationKey + '\'' +
               '}';
    }
}
