/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2021 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2021 The OpenNMS Group, Inc.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.opennms.nephron.elastic.ExporterNode;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.google.common.base.Strings;

/**
 * Describes the dimensions flow data can be grouped into.
 */
public abstract class RefType {

    public abstract void encode(CompoundKeyData data, OutputStream os) throws IOException;

    public abstract void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException;

    public abstract void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException;

    public abstract void populate(CompoundKeyData data, FlowSummary summary);

    public abstract void groupedByKey(CompoundKeyData data, StringBuilder sb);

    public abstract boolean isCompleteConversationRef(CompoundKeyData data);

    public abstract boolean equals(CompoundKeyData d1, CompoundKeyData d2);

    public abstract int hashCode(CompoundKeyData d);

    private final static Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
    private final static Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

    public static final RefType EXPORTER_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            INT_CODER.encode(data.nodeId, os);
            STRING_CODER.encode(data.foreignSource, os);
            STRING_CODER.encode(data.foreignId, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.nodeId = INT_CODER.decode(is);
            builder.foreignSource = STRING_CODER.decode(is);
            builder.foreignId = STRING_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException {
            if (!flow.hasExporterNode()) {
                throw new MissingFieldsException("exporterNode", flow);
            }
            final NodeInfo exporterNode = flow.getExporterNode();
            builder.nodeId = exporterNode.getNodeId();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                builder.foreignSource = exporterNode.getForeignSource();
                builder.foreignId = exporterNode.getForeginId();
            }
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            ExporterNode exporterNode = new ExporterNode();
            exporterNode.setNodeId(data.nodeId);
            exporterNode.setForeignSource(data.foreignSource);
            exporterNode.setForeignId(data.foreignId);
            summary.setExporter(exporterNode);
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            if (data.foreignSource != null) {
                sb.append(data.foreignSource).append(':').append(data.foreignId);
            } else {
                sb.append(data.nodeId);
            }
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return false;
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return d1.nodeId == d2.nodeId && Objects.equals(d1.foreignId, d2.foreignId) && Objects.equals(d1.foreignSource, d2.foreignSource);
        }

        @Override
        public int hashCode(CompoundKeyData d) {
            return Objects.hash(d.nodeId, d.foreignId, d.foreignSource);
        }
    };

    public static final RefType INTERFACE_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            INT_CODER.encode(data.ifIndex, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.ifIndex = INT_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException {
            if (Direction.INGRESS.equals(flow.getDirection())) {
                builder.ifIndex = flow.getInputSnmpIfindex().getValue();
            } else {
                builder.ifIndex = flow.getOutputSnmpIfindex().getValue();
            }
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            summary.setIfIndex(data.ifIndex);
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            sb.append(data.ifIndex);
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return false;
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return d1.ifIndex == d2.ifIndex;
        }

        @Override
        public int hashCode(CompoundKeyData d) {
            return d.ifIndex;
        }
    };

    public static int DEFAULT_CODE = 0;

    public static final RefType DSCP_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            INT_CODER.encode(data.dscp, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.dscp = INT_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException {
            builder.dscp = flow.hasDscp() ? flow.getDscp().getValue() : DEFAULT_CODE;
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            summary.setDscp(data.dscp);
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            sb.append(data.dscp);
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return false;
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return d1.dscp == d2.dscp;
        }

        @Override
        public int hashCode(CompoundKeyData d) {
            return d.dscp;
        }
    };

    public static final RefType APPLICATION_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            STRING_CODER.encode(data.application, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.application = STRING_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) {
            String application = flow.getApplication();
            builder.application = Strings.isNullOrEmpty(application) ? FlowSummary.UNKNOWN_APPLICATION_NAME_KEY : application;
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            summary.setApplication(data.application);
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            sb.append(data.application);
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return false;
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return Objects.equals(d1.application, d2.application);
        }

        @Override
        public int hashCode(CompoundKeyData data) {
            return data.application != null ? data.application.hashCode() : 0;
        }
    };

    public static final RefType HOST_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            STRING_CODER.encode(data.address, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.address = STRING_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException {
            // considers the src address only (the dst address is ignored)
            // -> the aggregation that is keyed by hosts is derived from the aggregation that is keyed by conversations
            // -> the src and dst address of flows is considered there (cf. the ProjConvWithTos transformation)
            String src = Strings.nullToEmpty(flow.getSrcAddress());
            builder.address = src;
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            summary.setHostAddress(data.address);
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            sb.append(data.address);
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return false;
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return Objects.equals(d1.address, d2.address);
        }

        @Override
        public int hashCode(CompoundKeyData data) {
            return data.address != null ? data.address.hashCode() : 0;
        }
    };

    public static final RefType CONVERSATION_PART = new RefType() {
        @Override
        public void encode(CompoundKeyData data, OutputStream os) throws IOException {
            STRING_CODER.encode(data.location, os);
            INT_CODER.encode(data.protocol, os);
            STRING_CODER.encode(data.address, os);
            STRING_CODER.encode(data.largerAddress, os);
            STRING_CODER.encode(data.application, os);
        }

        @Override
        public void decode(CompoundKeyData.Builder builder, InputStream is) throws IOException {
            builder.location = STRING_CODER.decode(is);
            builder.protocol = INT_CODER.decode(is);
            builder.address = STRING_CODER.decode(is);
            builder.largerAddress = STRING_CODER.decode(is);
            builder.application = STRING_CODER.decode(is);
        }

        @Override
        public void create(CompoundKeyData.Builder builder, FlowDocument flow) throws MissingFieldsException {
            builder.location = flow.getLocation();
            builder.protocol = flow.hasProtocol() ? flow.getProtocol().getValue() : null;
            String src = Strings.nullToEmpty(flow.getSrcAddress());
            String dst = Strings.nullToEmpty(flow.getDstAddress());
            if (src.compareTo(dst) < 0) {
                builder.address = src;
                builder.largerAddress = dst;
            } else {
                builder.address = dst;
                builder.largerAddress = src;
            }
            String application = flow.getApplication();
            builder.application = Strings.isNullOrEmpty(application) ? FlowSummary.UNKNOWN_APPLICATION_NAME_KEY : application;
        }

        @Override
        public void populate(CompoundKeyData data, FlowSummary summary) {
            summary.setConversationKey(data.getConversationKey());
        }

        @Override
        public void groupedByKey(CompoundKeyData data, StringBuilder sb) {
            sb.append(data.getConversationKey());
        }

        @Override
        public boolean isCompleteConversationRef(CompoundKeyData data) {
            return data.location != null && data.protocol != null && !Strings.isNullOrEmpty(data.address) && !Strings.isNullOrEmpty(data.largerAddress);
        }

        @Override
        public boolean equals(CompoundKeyData d1, CompoundKeyData d2) {
            return Objects.equals(d1.location, d2.location) && Objects.equals(d1.protocol, d2.protocol) &&
                   Objects.equals(d1.address, d2.address) && Objects.equals(d1.largerAddress, d2.largerAddress) &&
                   Objects.equals(d1.application, d2.application);
        }

        @Override
        public int hashCode(CompoundKeyData data) {
            return Objects.hash(data.location, data.protocol, data.address, data.largerAddress, data.application);
        }
    };

}
