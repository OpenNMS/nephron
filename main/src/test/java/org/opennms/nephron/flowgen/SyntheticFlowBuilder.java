/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2017 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2017 The OpenNMS Group, Inc.
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

package org.opennms.nephron.flowgen;

import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

public class SyntheticFlowBuilder {
    private static final Gson gson = new GsonBuilder().create();

    private final List<FlowDocument> flows = new ArrayList<>();

    private NodeInfo exporterNode;
    private Integer snmpInterfaceId;
    private String application = null;
    private Direction direction = Direction.INGRESS;
    private String srcHostname = null;
    private String dstHostname = null;
    private int tos = 0;

    public SyntheticFlowBuilder withExporter(String fs, String fid, int nodeId) {
        exporterNode = NodeInfo.newBuilder()
                .setNodeId(nodeId)
                .setForeignSource(fs)
                .setForeginId(fid)
                .build();
        return this;
    }

    public SyntheticFlowBuilder withSnmpInterfaceId(Integer snmpInterfaceId) {
        this.snmpInterfaceId = snmpInterfaceId;
        return this;
    }

    public SyntheticFlowBuilder withApplication(String application) {
        this.application = application;
        return this;
    }

    public SyntheticFlowBuilder withDirection(Direction direction) {
        this.direction = Objects.requireNonNull(direction);
        return this;
    }

    public SyntheticFlowBuilder withHostnames(final String srcHostname, final String dstHostname) {
        this.srcHostname = srcHostname;
        this.dstHostname = dstHostname;
        return this;
    }

    public SyntheticFlowBuilder withTos(int tos) {
        this.tos = tos;
        return this;
    }

    public SyntheticFlowBuilder withFlow(Instant date, String sourceIp, int sourcePort, String destIp, int destPort, long numBytes) {
        return withFlow(date, date, date, sourceIp, sourcePort, destIp, destPort, numBytes);
    }

    public SyntheticFlowBuilder withFlow(Instant firstSwitched, Instant lastSwitched, String sourceIp, int sourcePort, String destIp, int destPort, long numBytes) {
        return withFlow(firstSwitched, firstSwitched, lastSwitched, sourceIp, sourcePort, destIp, destPort, numBytes);
    }

    public SyntheticFlowBuilder withFlow(Instant firstSwitched, Instant deltaSwitched, Instant lastSwitched, String sourceIp, int sourcePort, String destIp, int destPort, long numBytes) {
        final FlowDocument.Builder builder = FlowDocument.newBuilder();
        builder.setTimestamp(lastSwitched.toEpochMilli());
        builder.setFirstSwitched(UInt64Value.of(firstSwitched.toEpochMilli()));
        builder.setDeltaSwitched(UInt64Value.of(deltaSwitched.toEpochMilli()));
        builder.setLastSwitched(UInt64Value.of(lastSwitched.toEpochMilli()));
        builder.setSrcAddress(sourceIp);
        builder.setSrcPort(UInt32Value.of(sourcePort));
        if (this.srcHostname != null) {
            builder.setSrcHostname(this.srcHostname);
        }
        builder.setDstAddress(destIp);
        builder.setDstPort(UInt32Value.of(destPort));
        if (this.dstHostname != null) {
            builder.setDstHostname(this.dstHostname);
        };
        builder.setNumBytes(UInt64Value.of(numBytes));
        builder.setProtocol(UInt32Value.of(6)); // TCP
        if (exporterNode !=  null) {
            builder.setExporterNode(exporterNode);
        }
        if (direction == Direction.INGRESS) {
            builder.setInputSnmpIfindex(UInt32Value.of(snmpInterfaceId));
        } else if (direction == Direction.EGRESS) {
            builder.setOutputSnmpIfindex(UInt32Value.of(snmpInterfaceId));
        }
        if (application != null) {
            builder.setApplication(application);
        }
        builder.setDirection(direction);

        builder.setConvoKey(getConvoKeyAsJsonString(builder));
        builder.setSamplingInterval(DoubleValue.of(1.0d));

        builder.setTos(UInt32Value.of(tos));

        flows.add(builder.build());
        return this;
    }

    private static String getConvoKeyAsJsonString(FlowDocument.Builder document) {
        // Only generate the key if all of the required fields are set
        if (document.getLocation() != null
                && document.getProtocol() != null
                && document.getSrcAddress() != null
                && document.getDstAddress() != null) {
            // Build the JSON string manually
            // This is faster than creating some new object on which we can use gson.toJson or similar
            final StringWriter writer = new StringWriter();
            writer.write("[");

            // Use GSON to encode the location, since this may contain characters that need to be escape
            writer.write(gson.toJson(document.getLocation()));
            writer.write(",");
            writer.write(Integer.toString(document.getProtocol().getValue()));
            writer.write(",");

            // Write out addresses in canonical format (lower one first)
            final String srcAddr = document.getSrcAddress();
            final String dstAddr = document.getDstAddress();
            if (Objects.compare(srcAddr, dstAddr, String::compareTo) < 0) {
                writer.write(gson.toJson(srcAddr));
                writer.write(",");
                writer.write(gson.toJson(dstAddr));
            } else {
                writer.write(gson.toJson(dstAddr));
                writer.write(",");
                writer.write(gson.toJson(srcAddr));
            }
            writer.write(",");

            if (document.getApplication() != null) {
                writer.write(gson.toJson(document.getApplication()));
            } else {
                writer.write("null");
            }

            writer.write("]");
            return writer.toString();
        }
        return null;
    }

    public List<FlowDocument> build() {
        return flows;
    }
}
