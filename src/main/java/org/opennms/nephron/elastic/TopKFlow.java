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

import com.fasterxml.jackson.annotation.JsonProperty;

public class TopKFlow {

    @JsonProperty("@timestamp")
    private long timestamp;

    @JsonProperty("range_start")
    private long rangeStartMs;

    @JsonProperty("range_end")
    private long rangeEndMs;

    @JsonProperty("ranking")
    private int ranking;

    @JsonProperty("grouped_by")
    private GroupedBy grouped_by;

    @JsonProperty("bytes_ingress")
    private Long bytes_ingress;

    @JsonProperty("bytes_egress")
    private Long bytes_egress;

    @JsonProperty("exporter")
    private ExporterNode exporter;

    @JsonProperty("application")
    private String application;


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

    public GroupedBy getGrouped_by() {
        return grouped_by;
    }

    public void setGrouped_by(GroupedBy grouped_by) {
        this.grouped_by = grouped_by;
    }

    public int getRanking() {
        return ranking;
    }

    public void setRanking(int ranking) {
        this.ranking = ranking;
    }

    public Long getBytes_ingress() {
        return bytes_ingress;
    }

    public void setBytes_ingress(Long bytes_ingress) {
        this.bytes_ingress = bytes_ingress;
    }

    public Long getBytes_egress() {
        return bytes_egress;
    }

    public void setBytes_egress(Long bytes_egress) {
        this.bytes_egress = bytes_egress;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopKFlow topKFlow = (TopKFlow) o;
        return timestamp == topKFlow.timestamp &&
                rangeStartMs == topKFlow.rangeStartMs &&
                rangeEndMs == topKFlow.rangeEndMs &&
                ranking == topKFlow.ranking &&
                Objects.equals(grouped_by, topKFlow.grouped_by) &&
                Objects.equals(bytes_ingress, topKFlow.bytes_ingress) &&
                Objects.equals(bytes_egress, topKFlow.bytes_egress) &&
                Objects.equals(exporter, topKFlow.exporter) &&
                Objects.equals(application, topKFlow.application);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, rangeStartMs, rangeEndMs, ranking, grouped_by, bytes_ingress, bytes_egress, exporter, application);
    }

    @Override
    public String toString() {
        return "TopKFlow{" +
                "timestamp=" + timestamp +
                ", rangeStartMs=" + rangeStartMs +
                ", rangeEndMs=" + rangeEndMs +
                ", ranking=" + ranking +
                ", grouped_by=" + grouped_by +
                ", bytes_ingress=" + bytes_ingress +
                ", bytes_egress=" + bytes_egress +
                ", exporter=" + exporter +
                ", application='" + application + '\'' +
                '}';
    }
}
