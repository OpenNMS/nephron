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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.opennms.nephron.elastic.ExporterNode;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

/**
 * Describes a dimensions flow data can be grouped into.
 */
abstract class RefType<T extends Ref> {

    public abstract void encode(T ref, OutputStream os) throws IOException;

    public abstract T decode(InputStream is) throws IOException;

    public abstract T create(FlowDocument flow) throws MissingFieldsException;

    public abstract void populate(T ref, FlowSummary summary);

    private final static Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
    private final static Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

    public static final RefType<Ref.Node> EXPORTER_PART = new RefType<Ref.Node>() {
        @Override
        public void encode(Ref.Node ref, OutputStream os) throws IOException {
            STRING_CODER.encode(ref.getForeignSource(), os);
            STRING_CODER.encode(ref.getForeignId(), os);
            INT_CODER.encode(ref.getNodeId(), os);
        }

        @Override
        public Ref.Node decode(InputStream is) throws IOException {
            Ref.Node ref = new Ref.Node();
            ref.setForeignSource(STRING_CODER.decode(is));
            ref.setForeignId(STRING_CODER.decode(is));
            ref.setNodeId(INT_CODER.decode(is));
            return ref;
        }

        @Override
        public Ref.Node create(FlowDocument flow) throws MissingFieldsException {
            return Ref.Node.of(flow);
        }

        @Override
        public void populate(Ref.Node ref, FlowSummary summary) {
            ExporterNode exporterNode = new ExporterNode();
            exporterNode.setForeignSource(ref.getForeignSource());
            exporterNode.setForeignId(ref.getForeignId());
            exporterNode.setNodeId(ref.getNodeId());
            summary.setExporter(exporterNode);
        }
    };

    public static final RefType<Ref.Interface> INTERFACE_PART = new RefType<Ref.Interface>() {
        @Override
        public void encode(Ref.Interface ref, OutputStream os) throws IOException {
            INT_CODER.encode(ref.getIfIndex(), os);
        }

        @Override
        public Ref.Interface decode(InputStream is) throws IOException {
            Ref.Interface ref = new Ref.Interface();
            ref.setIfIndex(INT_CODER.decode(is));
            return ref;
        }

        @Override
        public Ref.Interface create(FlowDocument flow) throws MissingFieldsException {
            return Ref.Interface.of(flow);
        }

        @Override
        public void populate(Ref.Interface ref, FlowSummary summary) {
            summary.setIfIndex(ref.getIfIndex());
        }
    };

    public static final RefType<Ref.Dscp> DSCP_PART = new RefType<Ref.Dscp>() {
        @Override
        public void encode(Ref.Dscp ref, OutputStream os) throws IOException {
            INT_CODER.encode(ref.getDscp(), os);
        }

        @Override
        public Ref.Dscp decode(InputStream is) throws IOException {
            return new Ref.Dscp(INT_CODER.decode(is));
        }

        @Override
        public Ref.Dscp create(FlowDocument flow) throws MissingFieldsException {
            return Ref.Dscp.of(flow);
        }

        @Override
        public void populate(Ref.Dscp ref, FlowSummary summary) {
            summary.setDscp(ref.getDscp());
        }
    };

    public static final RefType<Ref.Application> APPLICATION_PART = new RefType<Ref.Application>() {
        @Override
        public void encode(Ref.Application ref, OutputStream os) throws IOException {
            STRING_CODER.encode(ref.getApplication(), os);
        }

        @Override
        public Ref.Application decode(InputStream is) throws IOException {
            Ref.Application ref = new Ref.Application();
            ref.setApplication(STRING_CODER.decode(is));
            return ref;
        }

        @Override
        public Ref.Application create(FlowDocument flow) throws MissingFieldsException {
            return Ref.Application.of(flow);
        }

        @Override
        public void populate(Ref.Application ref, FlowSummary summary) {
            summary.setApplication(ref.getApplication());
        }
    };

    public static final RefType<Ref.Host> HOST_PART = new RefType<Ref.Host>() {
        @Override
        public void encode(Ref.Host ref, OutputStream os) throws IOException {
            STRING_CODER.encode(ref.getAddress(), os);
        }

        @Override
        public Ref.Host decode(InputStream is) throws IOException {
            Ref.Host ref = new Ref.Host();
            ref.setAddress(STRING_CODER.decode(is));
            return ref;
        }

        @Override
        public Ref.Host create(FlowDocument flow) throws MissingFieldsException {
            // considers the src address only (the dst address is ignored)
            // -> the aggregation that is keyed by hosts is derived from the aggregation that is keyed by conversations
            // -> the src and dst address of flows is considered there (cf. the ProjConvWithTos transformation)
            return Ref.Host.of(flow.getSrcAddress());
        }

        @Override
        public void populate(Ref.Host ref, FlowSummary summary) {
            summary.setHostAddress(ref.getAddress());
        }
    };

    public static final RefType<Ref.Conversation> CONVERSATION_PART = new RefType<Ref.Conversation>() {
        @Override
        public void encode(Ref.Conversation ref, OutputStream os) throws IOException {
            STRING_CODER.encode(ref.location, os);
            INT_CODER.encode(ref.protocol, os);
            STRING_CODER.encode(ref.smallerAddress, os);
            STRING_CODER.encode(ref.largerAddress, os);
            STRING_CODER.encode(ref.application, os);
        }

        @Override
        public Ref.Conversation decode(InputStream is) throws IOException {
            return new Ref.Conversation(
                    STRING_CODER.decode(is),
                    INT_CODER.decode(is),
                    STRING_CODER.decode(is),
                    STRING_CODER.decode(is),
                    STRING_CODER.decode(is)
            );
        }

        @Override
        public Ref.Conversation create(FlowDocument flow) throws MissingFieldsException {
            return Ref.Conversation.of(flow);
        }

        @Override
        public void populate(Ref.Conversation ref, FlowSummary summary) {
            summary.setConversationKey(ref.asConversationKey());
        }
    };

}
