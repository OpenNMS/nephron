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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.opennms.nephron.elastic.ExporterNode;
import org.opennms.nephron.elastic.GroupedBy;
import org.opennms.nephron.elastic.TopKFlow;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.google.common.base.Strings;

public class Groupings {

    static class KeyByExporterInterface extends DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            if (!flow.hasExporterNode()) {
                return;
            }

            final NodeRef nodeRef;
            final NodeInfo exporterNode = flow.getExporterNode();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                    && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                nodeRef = NodeRef.of(exporterNode.getForeignSource(), exporterNode.getForeginId());
            } else {
                nodeRef = NodeRef.of(exporterNode.getNodeId());
            }

            final InterfaceRef interfaceRef;
            if (Direction.INGRESS.equals(flow.getDirection())) {
                interfaceRef = InterfaceRef.of(flow.getInputSnmpIfindex().getValue());
            } else {
                interfaceRef = InterfaceRef.of(flow.getOutputSnmpIfindex().getValue());
            }

            c.output(KV.of(ExporterInterfaceKey.of(nodeRef, interfaceRef), flow));
        }
    }

    static class KeyByExporterInterfaceApplication extends DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            if (!flow.hasExporterNode()) {
                return;
            }

            final NodeRef nodeRef;
            final NodeInfo exporterNode = flow.getExporterNode();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                    && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                nodeRef = NodeRef.of(exporterNode.getForeignSource(), exporterNode.getForeginId());
            } else {
                nodeRef = NodeRef.of(exporterNode.getNodeId());
            }

            final InterfaceRef interfaceRef;
            if (Direction.INGRESS.equals(flow.getDirection())) {
                interfaceRef = InterfaceRef.of(flow.getInputSnmpIfindex().getValue());
            } else {
                interfaceRef = InterfaceRef.of(flow.getOutputSnmpIfindex().getValue());
            }

            final ApplicationRef applicationRef;
            if (Strings.isNullOrEmpty(flow.getApplication())) {
                applicationRef = ApplicationRef.of(TopKFlow.UNKNOWN_APPLICATION_NAME_KEY);
            } else {
                applicationRef = ApplicationRef.of(flow.getApplication());
            }

            c.output(KV.of(ExporterInterfaceApplicationKey.of(nodeRef, interfaceRef, applicationRef), flow));
        }
    }

    static class KeyByExporterInterfaceHost extends DoFn<FlowDocument, KV<Groupings.CompoundKey, FlowDocument>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            final FlowDocument flow = c.element();
            if (!flow.hasExporterNode()) {
                return;
            }

            final NodeRef nodeRef;
            final NodeInfo exporterNode = flow.getExporterNode();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                    && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                nodeRef = NodeRef.of(exporterNode.getForeignSource(), exporterNode.getForeginId());
            } else {
                nodeRef = NodeRef.of(exporterNode.getNodeId());
            }

            final InterfaceRef interfaceRef;
            if (Direction.INGRESS.equals(flow.getDirection())) {
                interfaceRef = InterfaceRef.of(flow.getInputSnmpIfindex().getValue());
            } else {
                interfaceRef = InterfaceRef.of(flow.getOutputSnmpIfindex().getValue());
            }

            c.output(KV.of(ExporterInterfaceHostKey.of(nodeRef, interfaceRef, HostRef.of(flow.getSrcAddress(), flow.getSrcHostname())), flow));
            c.output(KV.of(ExporterInterfaceHostKey.of(nodeRef, interfaceRef, HostRef.of(flow.getDstAddress(), flow.getDstHostname())), flow));
        }
    }

    public static class NodeRef {
        private String foreignSource;
        private String foreignId;
        private Integer nodeId;

        public static NodeRef of(String foreignSource, String foreignId) {
            NodeRef nodeRef = new NodeRef();
            nodeRef.setForeignSource(foreignSource);
            nodeRef.setForeignId(foreignId);
            return nodeRef;
        }

        public static NodeRef of(int nodeId) {
            NodeRef nodeRef = new NodeRef();
            nodeRef.setNodeId(nodeId);
            return nodeRef;
        }

        public String getForeignSource() {
            return foreignSource;
        }

        public void setForeignSource(String foreignSource) {
            this.foreignSource = foreignSource;
        }

        public String getForeignId() {
            return foreignId;
        }

        public void setForeignId(String foreignId) {
            this.foreignId = foreignId;
        }

        public Integer getNodeId() {
            return nodeId;
        }

        public void setNodeId(Integer nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeRef nodeRef = (NodeRef) o;
            return Objects.equals(foreignSource, nodeRef.foreignSource) &&
                    Objects.equals(foreignId, nodeRef.foreignId) &&
                    Objects.equals(nodeId, nodeRef.nodeId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foreignSource, foreignId, nodeId);
        }

        @Override
        public String toString() {
            return "NodeRef{" +
                    "foreignSource='" + foreignSource + '\'' +
                    ", foreignId='" + foreignId + '\'' +
                    ", nodeId=" + nodeId +
                    '}';
        }
    }


    public static class InterfaceRef {
        private int ifIndex;

        public static InterfaceRef of(int ifIndex) {
            InterfaceRef interfaceRef = new InterfaceRef();
            interfaceRef.setIfIndex(ifIndex);
            return interfaceRef;
        }

        public int getIfIndex() {
            return ifIndex;
        }

        public void setIfIndex(int ifIndex) {
            this.ifIndex = ifIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InterfaceRef that = (InterfaceRef) o;
            return ifIndex == that.ifIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ifIndex);
        }

        @Override
        public String toString() {
            return "InterfaceRef{" +
                    "ifIndex=" + ifIndex +
                    '}';
        }
    }

    public static class ApplicationRef {
        private String application;

        public static ApplicationRef of(String application) {
            ApplicationRef applicationRef = new ApplicationRef();
            applicationRef.setApplication(application);
            return applicationRef;
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
            ApplicationRef that = (ApplicationRef) o;
            return Objects.equals(application, that.application);
        }

        @Override
        public int hashCode() {
            return Objects.hash(application);
        }

        @Override
        public String toString() {
            return "ApplicationRef{" +
                    "application='" + application + '\'' +
                    '}';
        }
    }

    public static class HostRef {
        private String address;
        private String hostname;

        public static HostRef of(String address, String hostname) {
            HostRef hostRef = new HostRef();
            hostRef.setAddress(address);
            hostRef.setHostname(hostname);
            return hostRef;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HostRef hostRef = (HostRef) o;
            return Objects.equals(address, hostRef.address) &&
                    Objects.equals(hostname, hostRef.hostname);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address, hostname);
        }

        @Override
        public String toString() {
            return "HostRef{" +
                    "address='" + address + '\'' +
                    ", hostname='" + hostname + '\'' +
                    '}';
        }
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterInterfaceApplicationKey extends CompoundKey {
        private NodeRef nodeRef;
        private InterfaceRef interfaceRef;
        private ApplicationRef applicationRef;

        public static ExporterInterfaceApplicationKey of(NodeRef nodeRef, InterfaceRef interfaceRef, ApplicationRef applicationRef) {
            ExporterInterfaceApplicationKey key = new ExporterInterfaceApplicationKey();
            key.setNodeRef(Objects.requireNonNull(nodeRef));
            key.setInterfaceRef(Objects.requireNonNull(interfaceRef));
            key.setApplicationRef(Objects.requireNonNull(applicationRef));
            return key;
        }

        public NodeRef getNodeRef() {
            return nodeRef;
        }

        public void setNodeRef(NodeRef nodeRef) {
            this.nodeRef = nodeRef;
        }

        public InterfaceRef getInterfaceRef() {
            return interfaceRef;
        }

        public void setInterfaceRef(InterfaceRef interfaceRef) {
            this.interfaceRef = interfaceRef;
        }

        public ApplicationRef getApplicationRef() {
            return applicationRef;
        }

        public void setApplicationRef(ApplicationRef applicationRef) {
            this.applicationRef = applicationRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExporterInterfaceApplicationKey that = (ExporterInterfaceApplicationKey) o;
            return Objects.equals(nodeRef, that.nodeRef) &&
                    Objects.equals(interfaceRef, that.interfaceRef) &&
                    Objects.equals(applicationRef, that.applicationRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeRef, interfaceRef, applicationRef);
        }

        @Override
        public String toString() {
            return "ExporterInterfaceApplicationKey{" +
                    "nodeRef=" + nodeRef +
                    ", interfaceRef=" + interfaceRef +
                    ", applicationRef=" + applicationRef +
                    '}';
        }

        @Override
        public void visit(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public CompoundKey getOuterKey() {
            return ExporterInterfaceKey.of(nodeRef, interfaceRef);
        }
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterInterfaceKey extends CompoundKey {
        private NodeRef nodeRef;
        private InterfaceRef interfaceRef;

        public static ExporterInterfaceKey of(NodeRef nodeRef, InterfaceRef interfaceRef) {
            ExporterInterfaceKey key = new ExporterInterfaceKey();
            key.setNodeRef(Objects.requireNonNull(nodeRef));
            key.setInterfaceRef(Objects.requireNonNull(interfaceRef));
            return key;
        }

        public NodeRef getNodeRef() {
            return nodeRef;
        }

        public void setNodeRef(NodeRef nodeRef) {
            this.nodeRef = nodeRef;
        }

        public InterfaceRef getInterfaceRef() {
            return interfaceRef;
        }

        public void setInterfaceRef(InterfaceRef interfaceRef) {
            this.interfaceRef = interfaceRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExporterInterfaceKey that = (ExporterInterfaceKey) o;
            return Objects.equals(nodeRef, that.nodeRef) &&
                    Objects.equals(interfaceRef, that.interfaceRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeRef, interfaceRef);
        }

        @Override
        public String toString() {
            return "ExporterInterfaceKey{" +
                    "nodeRef=" + nodeRef +
                    ", interfaceRef=" + interfaceRef +
                    '}';
        }

        @Override
        public void visit(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public CompoundKey getOuterKey() {
            throw new UnsupportedOperationException("No export key yet.");
        }
    }


    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterInterfaceHostKey extends CompoundKey {
        private NodeRef nodeRef;
        private InterfaceRef interfaceRef;
        private HostRef hostRef;

        public static ExporterInterfaceHostKey of(NodeRef nodeRef, InterfaceRef interfaceRef, HostRef hostRef) {
            ExporterInterfaceHostKey key = new ExporterInterfaceHostKey();
            key.setNodeRef(Objects.requireNonNull(nodeRef));
            key.setInterfaceRef(Objects.requireNonNull(interfaceRef));
            key.setHostRef(Objects.requireNonNull(hostRef));
            return key;
        }

        public NodeRef getNodeRef() {
            return nodeRef;
        }

        public void setNodeRef(NodeRef nodeRef) {
            this.nodeRef = nodeRef;
        }

        public InterfaceRef getInterfaceRef() {
            return interfaceRef;
        }

        public void setInterfaceRef(InterfaceRef interfaceRef) {
            this.interfaceRef = interfaceRef;
        }

        public HostRef getHostRef() {
            return hostRef;
        }

        public void setHostRef(HostRef hostRef) {
            this.hostRef = hostRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExporterInterfaceHostKey that = (ExporterInterfaceHostKey) o;
            return Objects.equals(nodeRef, that.nodeRef) &&
                    Objects.equals(interfaceRef, that.interfaceRef) &&
                    Objects.equals(hostRef, that.hostRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeRef, interfaceRef, hostRef);
        }

        @Override
        public String toString() {
            return "ExporterInterfaceHostKey{" +
                    "nodeRef=" + nodeRef +
                    ", interfaceRef=" + interfaceRef +
                    ", applicationRef=" + hostRef +
                    '}';
        }

        @Override
        public void visit(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public CompoundKey getOuterKey() {
            return ExporterInterfaceKey.of(nodeRef, interfaceRef);
        }
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static abstract class CompoundKey {
        public abstract void visit(Visitor visitor);
        public abstract CompoundKey getOuterKey();
    }

    public interface Visitor {
        void visit(ExporterInterfaceKey key);
        void visit(ExporterInterfaceApplicationKey key);
        void visit(ExporterInterfaceHostKey key);
    }

    public static class FlowPopulatingVisitor implements Visitor {
        private final TopKFlow flow;

        public FlowPopulatingVisitor(TopKFlow flow) {
            this.flow = Objects.requireNonNull(flow);
        }

        @Override
        public void visit(ExporterInterfaceKey key) {
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
        }

        @Override
        public void visit(ExporterInterfaceApplicationKey key) {
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_APPLICATION);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setApplication(key.applicationRef.application);
        }

        @Override
        public void visit(ExporterInterfaceHostKey key) {
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_HOST);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setHostAddress(key.hostRef.address);
            flow.setHostName(key.hostRef.hostname);
        }

        private static ExporterNode toExporterNode(NodeRef nodeRef) {
            ExporterNode exporterNode = new ExporterNode();
            exporterNode.setForeignSource(nodeRef.foreignSource);
            exporterNode.setForeignId(nodeRef.foreignId);
            exporterNode.setNodeId(nodeRef.nodeId);
            return exporterNode;
        }
    }

    public static class CompoundKeyCoder extends AtomicCoder<CompoundKey> {
        private static final NodeRefKeyCoder NODE_REF_KEY_CODER = NodeRefKeyCoder.of();
        private Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
        private Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

        @Override
        public void encode(CompoundKey value, OutputStream os) throws IOException {
            value.visit(new Visitor() {
                @Override
                public void visit(ExporterInterfaceKey key) {
                    try {
                        INT_CODER.encode(1, os);
                        NODE_REF_KEY_CODER.encode(key.nodeRef, os);
                        INT_CODER.encode(key.interfaceRef.ifIndex, os);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void visit(ExporterInterfaceApplicationKey key) {
                    try {
                        INT_CODER.encode(2, os);
                        NODE_REF_KEY_CODER.encode(key.nodeRef, os);
                        INT_CODER.encode(key.interfaceRef.ifIndex, os);
                        STRING_CODER.encode(key.applicationRef.application, os);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void visit(ExporterInterfaceHostKey key) {
                    try {
                        INT_CODER.encode(3, os);
                        NODE_REF_KEY_CODER.encode(key.nodeRef, os);
                        INT_CODER.encode(key.interfaceRef.ifIndex, os);
                        STRING_CODER.encode(key.hostRef.address, os);
                        STRING_CODER.encode(key.hostRef.hostname, os);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        @Override
        public CompoundKey decode(InputStream is) throws IOException {
            int type = INT_CODER.decode(is);
            if (type == 1) {
                ExporterInterfaceKey key = new ExporterInterfaceKey();

                key.nodeRef = NODE_REF_KEY_CODER.decode(is);
                key.interfaceRef = new InterfaceRef();
                key.interfaceRef.ifIndex = INT_CODER.decode(is);

                return key;
            } else if (type == 2) {
                ExporterInterfaceApplicationKey key = new ExporterInterfaceApplicationKey();

                key.nodeRef = NODE_REF_KEY_CODER.decode(is);
                key.interfaceRef = new InterfaceRef();
                key.interfaceRef.ifIndex = INT_CODER.decode(is);

                key.applicationRef = new ApplicationRef();
                key.applicationRef.application = STRING_CODER.decode(is);
                return key;
            } else if (type == 3) {
                ExporterInterfaceHostKey key = new ExporterInterfaceHostKey();

                key.nodeRef = NODE_REF_KEY_CODER.decode(is);

                key.interfaceRef = new InterfaceRef();
                key.interfaceRef.ifIndex = INT_CODER.decode(is);

                key.hostRef = new HostRef();
                key.hostRef.address = STRING_CODER.decode(is);
                key.hostRef.hostname = STRING_CODER.decode(is);
                return key;
            }
            throw new RuntimeException("oops: " + type);
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }

    public static class NodeRefKeyCoder extends AtomicCoder<NodeRef> {
        public static NodeRefKeyCoder of() {
            return INSTANCE;
        }

        private static final NodeRefKeyCoder INSTANCE = new NodeRefKeyCoder();

        private NodeRefKeyCoder() {}

        private final static Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
        private final static  Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

        @Override
        public void encode(NodeRef nodeRef, OutputStream os) throws IOException {
            STRING_CODER.encode(nodeRef.foreignSource, os);
            STRING_CODER.encode(nodeRef.foreignId, os);
            INT_CODER.encode(nodeRef.nodeId, os);
        }

        @Override
        public NodeRef decode(InputStream is) throws IOException {
            NodeRef nodeRef = new NodeRef();
            nodeRef.foreignSource = STRING_CODER.decode(is);
            nodeRef.foreignId = STRING_CODER.decode(is);
            nodeRef.nodeId = INT_CODER.decode(is);
            return nodeRef;
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }
}
