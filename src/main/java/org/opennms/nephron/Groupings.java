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

    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterInterfaceApplicationKey extends CompoundKey {
        private NodeRef nodeRef;
        private InterfaceRef interfaceRef;
        private ApplicationRef applicationRef;

        public static ExporterInterfaceApplicationKey of(NodeRef nodeRef, InterfaceRef interfaceRef, ApplicationRef applicationRef) {
            ExporterInterfaceApplicationKey key = new ExporterInterfaceApplicationKey();
            key.setNodeRef(nodeRef);
            key.setInterfaceRef(interfaceRef);
            key.setApplicationRef(applicationRef);
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
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static abstract class CompoundKey {
        public abstract void visit(Visitor visitor);
    }

    public interface Visitor {
        void visit(ExporterInterfaceApplicationKey key);
    }

    public static class FlowPopulatingVisitor implements Visitor {
        private final TopKFlow flow;

        public FlowPopulatingVisitor(TopKFlow flow) {
            this.flow = Objects.requireNonNull(flow);
        }

        @Override
        public void visit(ExporterInterfaceApplicationKey key) {
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_APPLICATION);
            ExporterNode exporterNode = new ExporterNode();
            exporterNode.setForeignSource(key.nodeRef.foreignSource);
            exporterNode.setForeignId(key.nodeRef.foreignId);
            exporterNode.setNodeId(key.nodeRef.nodeId);
            flow.setExporter(exporterNode);
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setApplication(key.applicationRef.application);
        }
    }

    public static class CompoundKeyCoder extends AtomicCoder<CompoundKey> {
        private Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
        private Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

        @Override
        public void encode(CompoundKey value, OutputStream os) throws IOException {
            value.visit(new Visitor() {
                @Override
                public void visit(ExporterInterfaceApplicationKey key) {
                    try {
                        INT_CODER.encode(1, os);
                        STRING_CODER.encode(key.nodeRef.foreignSource, os);
                        STRING_CODER.encode(key.nodeRef.foreignId, os);
                        INT_CODER.encode(key.nodeRef.nodeId, os);
                        INT_CODER.encode(key.interfaceRef.ifIndex, os);
                        STRING_CODER.encode(key.applicationRef.application, os);
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
                ExporterInterfaceApplicationKey key = new ExporterInterfaceApplicationKey();

                key.nodeRef = new NodeRef();
                key.nodeRef.foreignSource = STRING_CODER.decode(is);
                key.nodeRef.foreignId = STRING_CODER.decode(is);
                key.nodeRef.nodeId = INT_CODER.decode(is);

                key.interfaceRef = new InterfaceRef();
                key.interfaceRef.ifIndex = INT_CODER.decode(is);

                key.applicationRef = new ApplicationRef();
                key.applicationRef.application = STRING_CODER.decode(is);
                return key;
            }
            throw new RuntimeException("oops: " + type);
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }
}
