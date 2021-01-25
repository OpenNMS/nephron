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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.opennms.nephron.elastic.ExporterNode;
import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.nephron.elastic.GroupedBy;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Contains functions used to group flows by different keys.
 *
 * The keys are modeled as subclasses of {@link CompoundKey} and include
 * associated {@link Coder}s for serdes.
 *
 * A visitor pattern is used to handle the different key types to
 * ensure that all the implementations that need to be aware of them get updated
 * when new types are added.
 *
 * @author jwhite
 */
public class Groupings {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    @DefaultCoder(CompoundKeyCoder.class)
    public static abstract class CompoundKey {
        public abstract void visit(Visitor visitor);

        /**
         * Ordered list of references from which the key is composed
         *
         * @return immutable list of keys
         */
        public abstract List<Ref> getKeys();

        /**
         * Build the parent, or "outer" key for the current key.
         *
         * @return the outer key, or null if no such key exists
         */
        public abstract CompoundKey getOuterKey();

    }

    public interface Visitor {
        void visit(ExporterKey key);
        void visit(ExporterInterfaceKey key);
        void visit(ExporterInterfaceApplicationKey key);
        void visit(ExporterInterfaceHostKey key);
        void visit(ExporterInterfaceConversationKey key);
    }

    public static abstract class KeyFlowBy extends DoFn<FlowDocument, KV<CompoundKey, Aggregate>> {

        public static class WithHostname<T> {
            public final T value;
            public final String hostname;

            public WithHostname(final T value, final String hostname) {
                this.value = value;
                this.hostname = hostname;
            }

            public static class Builder<T> {
                private final T value;

                private Builder(final T value) {
                    this.value = value;
                }

                public WithHostname<T> withoutHostname() {
                    return new WithHostname<>(this.value, null);
                }

                public WithHostname<T> andHostname(final String hostname) {
                    return new WithHostname<>(this.value, hostname);
                }
            }

            public static <T> Builder<T> having(final T value) {
                return new Builder<>(value);
            }
        }

        private final Counter flowsWithMissingFields = Metrics.counter(Groupings.class, "flowsWithMissingFields");
        private final Counter flowsInWindow = Metrics.counter("flows", "in_window");

        private Optional<Aggregate> aggregatize(final FlowWindows.FlowWindow window, final FlowDocument flow, final String hostname) {
            // The flow duration ranges [delta_switched, last_switched)
            long flowDurationMs = flow.getLastSwitched().getValue() - flow.getDeltaSwitched().getValue();
            if (flowDurationMs < 0) {
                // Negative duration, pass
                LOG.warn("Ignoring flow with negative duration: {}. Flow: {}", flowDurationMs, flow);
                return Optional.empty();
            }

            final double multiplier;
            if (flowDurationMs == 0) {
                // Double check that the flow is in fact in this window
                if (flow.getDeltaSwitched().getValue() >= window.start().getMillis()
                    && flow.getLastSwitched().getValue() <= window.end().getMillis()) {
                    // Use the entirety of the flow bytes
                    multiplier = 1.0;
                } else {
                    return Optional.empty();
                }

            } else {
                // Now determine how many milliseconds the flow overlaps with the window bounds
                long flowWindowOverlapMs = Math.min(flow.getLastSwitched().getValue(), window.end().getMillis())
                                           - Math.max(flow.getDeltaSwitched().getValue(), window.start().getMillis());
                if (flowWindowOverlapMs < 0) {
                    // Flow should not be in this windows! pass
                    return Optional.empty();
                }

                // Output value proportional to the overlap with the window
                multiplier = flowWindowOverlapMs / (double) flowDurationMs;
            }

            // Track
            flowsInWindow.inc();

            double effectiveMultiplier = multiplier;
            if (flow.hasSamplingInterval()) {
                double samplingInterval = flow.getSamplingInterval().getValue();
                if (samplingInterval > 0) {
                    effectiveMultiplier *= samplingInterval;
                }
            }

            // Rounding to whole numbers to avoid loosing some bytes on window bounds. This, in theory, shifts the
            // window bound a little bit but makes sums correct.
            final long bytes = Math.round(flow.getNumBytes().getValue() * effectiveMultiplier);

            final long bytesIn;
            final long bytesOut;
            if (Direction.INGRESS.equals(flow.getDirection())) {
                bytesIn = bytes;
                bytesOut = 0;
            } else {
                bytesIn = 0;
                bytesOut = bytes;
            }

            return Optional.of(new Aggregate(bytesIn, bytesOut, hostname));
        }

        @ProcessElement
        public void processElement(ProcessContext c, FlowWindows.FlowWindow window) {
            final FlowDocument flow = c.element();
            try {
                for (final WithHostname<? extends CompoundKey> key: key(flow)) {
                    aggregatize(window, flow, key.hostname).ifPresent(aggregate -> c.output(KV.of(key.value, aggregate)));
                }
            } catch (MissingFieldsException mfe) {
                flowsWithMissingFields.inc();
            }
        }

        public abstract Collection<WithHostname<? extends CompoundKey>> key(FlowDocument flow) throws MissingFieldsException;
    }

    public static class KeyByExporterInterface extends KeyFlowBy {
        @Override
        public Collection<WithHostname<? extends CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            return Collections.singleton(WithHostname.having(ExporterInterfaceKey.from(flow)).withoutHostname());
        }
    }

    public static class KeyByExporterInterfaceApplication extends KeyFlowBy {
        @Override
        public Collection<WithHostname<? extends CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            return Collections.singleton(WithHostname.having(ExporterInterfaceApplicationKey.from(flow)).withoutHostname());
        }
    }

    public static class KeyByExporterInterfaceHost extends KeyFlowBy {
        @Override
        public Collection<WithHostname<? extends CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            final NodeRef nodeRef = NodeRef.of(flow);
            final InterfaceRef interfaceRef = InterfaceRef.of(flow);
            return Arrays.asList(
                    WithHostname.having(ExporterInterfaceHostKey.of(nodeRef, interfaceRef, HostRef.of(flow.getSrcAddress()))).andHostname(flow.getSrcHostname()),
                    WithHostname.having(ExporterInterfaceHostKey.of(nodeRef, interfaceRef, HostRef.of(flow.getDstAddress()))).andHostname(flow.getDstHostname()));
        }
    }

    public static class KeyByExporterInterfaceConversation extends KeyFlowBy {
        @Override
        public Collection<WithHostname<? extends CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            return Collections.singleton(WithHostname.having(ExporterInterfaceConversationKey.from(flow)).withoutHostname());
        }
    }

    interface Ref {
        String idAsString();
    }

    public static class NodeRef implements Ref {
        private String foreignSource;
        private String foreignId;
        private Integer nodeId;

        public static NodeRef of(int nodeId, String foreignSource, String foreignId) {
            NodeRef nodeRef = new NodeRef();
            nodeRef.setNodeId(nodeId);
            nodeRef.setForeignSource(foreignSource);
            nodeRef.setForeignId(foreignId);
            return nodeRef;
        }

        public static NodeRef of(int nodeId) {
            NodeRef nodeRef = new NodeRef();
            nodeRef.setNodeId(nodeId);
            return nodeRef;
        }

        public static NodeRef of(FlowDocument flow) throws MissingFieldsException {
            if (!flow.hasExporterNode()) {
                throw new MissingFieldsException("exporterNode", flow);
            }
            final NodeInfo exporterNode = flow.getExporterNode();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                    && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                return NodeRef.of(exporterNode.getNodeId(), exporterNode.getForeignSource(), exporterNode.getForeginId());
            } else {
                return NodeRef.of(exporterNode.getNodeId());
            }
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

        @Override
        public String idAsString() {
            if (foreignSource != null) {
                return foreignSource + ":" + foreignId;
            }
            return Integer.toString(nodeId);
        }
    }

    public static class InterfaceRef implements Ref {
        private int ifIndex;

        public static InterfaceRef of(int ifIndex) {
            InterfaceRef interfaceRef = new InterfaceRef();
            interfaceRef.setIfIndex(ifIndex);
            return interfaceRef;
        }

        public static InterfaceRef of(FlowDocument flow) {
            if (Direction.INGRESS.equals(flow.getDirection())) {
                return InterfaceRef.of(flow.getInputSnmpIfindex().getValue());
            } else {
                return InterfaceRef.of(flow.getOutputSnmpIfindex().getValue());
            }
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

        @Override
        public String idAsString() {
            return Integer.toString(ifIndex);
        }
    }

    public static class ApplicationRef implements Ref {
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

        @Override
        public String idAsString() {
            return application;
        }
    }

    public static class HostRef implements Ref {
        private String address;

        public static HostRef of(String address) {
            HostRef hostRef = new HostRef();
            hostRef.setAddress(address);
            return hostRef;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HostRef hostRef = (HostRef) o;
            return Objects.equals(address, hostRef.address);
        }

        @Override
        public int hashCode() {
            return Objects.hash(address);
        }

        @Override
        public String toString() {
            return "HostRef{" +
                    "address='" + address + '\'' +
                    '}';
        }

        @Override
        public String idAsString() {
            return address;
        }
    }

    public static class ConversationRef implements Ref {
        private String conversationKey;

        public static ConversationRef of(String conversationKey) {
            ConversationRef conversationRef = new ConversationRef();
            conversationRef.setConversationKey(conversationKey);
            return conversationRef;
        }

        public static ConversationRef of(FlowDocument flow) {
            return of(flow.getConvoKey());
        }

        public String getConversationKey() {
            return conversationKey;
        }

        public void setConversationKey(String conversationKey) {
            this.conversationKey = conversationKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConversationRef that = (ConversationRef) o;
            return Objects.equals(conversationKey, that.conversationKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(conversationKey);
        }

        @Override
        public String toString() {
            return "ConversationRef{" +
                    "conversationKey='" + conversationKey + '\'' +
                    '}';
        }

        @Override
        public String idAsString() {
            return conversationKey;
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

        public static ExporterInterfaceApplicationKey from(FlowDocument flow) throws MissingFieldsException {
            final NodeRef nodeRef = NodeRef.of(flow);
            final InterfaceRef interfaceRef = InterfaceRef.of(flow);

            final ApplicationRef applicationRef;
            if (Strings.isNullOrEmpty(flow.getApplication())) {
                applicationRef = ApplicationRef.of(FlowSummary.UNKNOWN_APPLICATION_NAME_KEY);
            } else {
                applicationRef = ApplicationRef.of(flow.getApplication());
            }

            return of(nodeRef, interfaceRef, applicationRef);
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

        @Override
        public List<Ref> getKeys() {
            return Arrays.asList(nodeRef, interfaceRef, applicationRef);
        }
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterKey extends CompoundKey {
        private NodeRef nodeRef;

        public static ExporterKey from(FlowDocument flow) throws MissingFieldsException {
            final ExporterKey key = new ExporterKey();
            key.setNodeRef(NodeRef.of(flow));
            return key;
        }

        public NodeRef getNodeRef() {
            return nodeRef;
        }

        public void setNodeRef(NodeRef nodeRef) {
            this.nodeRef = nodeRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ExporterKey)) return false;
            ExporterKey that = (ExporterKey) o;
            return Objects.equals(nodeRef, that.nodeRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeRef);
        }

        @Override
        public String toString() {
            return "ExporterKey{" +
                    "nodeRef=" + nodeRef +
                    '}';
        }

        @Override
        public void visit(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public CompoundKey getOuterKey() {
            // No parent
            return null;
        }

        @Override
        public List<Ref> getKeys() {
            return Arrays.asList(nodeRef);
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

        public static ExporterInterfaceKey from(FlowDocument flow) throws MissingFieldsException {
            return of(NodeRef.of(flow), InterfaceRef.of(flow));
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
            ExporterKey parentKey = new ExporterKey();
            parentKey.setNodeRef(nodeRef);
            return parentKey;
        }

        @Override
        public List<Ref> getKeys() {
            return Arrays.asList(nodeRef, interfaceRef);
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

        @Override
        public List<Ref> getKeys() {
            return Arrays.asList(nodeRef, interfaceRef, hostRef);
        }
    }

    @DefaultCoder(CompoundKeyCoder.class)
    public static class ExporterInterfaceConversationKey extends CompoundKey {
        private NodeRef nodeRef;
        private InterfaceRef interfaceRef;
        private ConversationRef conversationRef;

        public static ExporterInterfaceConversationKey of(NodeRef nodeRef, InterfaceRef interfaceRef, ConversationRef conversationRef) {
            final ExporterInterfaceConversationKey key = new ExporterInterfaceConversationKey();
            key.setNodeRef(Objects.requireNonNull(nodeRef));
            key.setInterfaceRef(Objects.requireNonNull(interfaceRef));
            key.setConversationRef(Objects.requireNonNull(conversationRef));
            return key;
        }

        public static CompoundKey from(FlowDocument flow) throws MissingFieldsException {
            if (!flow.hasExporterNode()) {
                throw new MissingFieldsException("exporterNode", flow);
            }

            final NodeRef nodeRef = NodeRef.of(flow);
            final InterfaceRef interfaceRef = InterfaceRef.of(flow);
            final ConversationRef conversationRef = ConversationRef.of(flow);
            return of(nodeRef, interfaceRef, conversationRef);
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

        public ConversationRef getConversationRef() {
            return conversationRef;
        }

        public void setConversationRef(ConversationRef conversationRef) {
            this.conversationRef = conversationRef;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ExporterInterfaceConversationKey that = (ExporterInterfaceConversationKey) o;
            return Objects.equals(nodeRef, that.nodeRef) &&
                    Objects.equals(interfaceRef, that.interfaceRef) &&
                    Objects.equals(conversationRef, that.conversationRef);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeRef, interfaceRef, conversationRef);
        }

        @Override
        public void visit(Visitor visitor) {
            visitor.visit(this);
        }

        @Override
        public CompoundKey getOuterKey() {
            return ExporterInterfaceKey.of(nodeRef, interfaceRef);
        }

        @Override
        public List<Ref> getKeys() {
            return Arrays.asList(nodeRef, interfaceRef, conversationRef);
        }
    }

    public static class FlowPopulatingVisitor implements Visitor {
        private final FlowSummary flow;

        public FlowPopulatingVisitor(FlowSummary flow) {
            this.flow = Objects.requireNonNull(flow);
        }

        private static String keyToString(List<Ref> keys) {
            return keys.stream()
                    .map(Ref::idAsString)
                    .collect(Collectors.joining("-"));
        }

        @Override
        public void visit(ExporterKey key) {
            flow.setGroupedByKey(keyToString(key.getKeys()));
            flow.setGroupedBy(GroupedBy.EXPORTER);
            flow.setExporter(toExporterNode(key.nodeRef));
        }

        @Override
        public void visit(ExporterInterfaceKey key) {
            flow.setGroupedByKey(keyToString(key.getKeys()));
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
        }

        @Override
        public void visit(ExporterInterfaceApplicationKey key) {
            flow.setGroupedByKey(keyToString(key.getKeys()));
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_APPLICATION);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setApplication(key.applicationRef.application);
        }

        @Override
        public void visit(ExporterInterfaceHostKey key) {
            flow.setGroupedByKey(keyToString(key.getKeys()));
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_HOST);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setHostAddress(key.hostRef.address);
        }

        @Override
        public void visit(ExporterInterfaceConversationKey key) {
            flow.setGroupedByKey(keyToString(key.getKeys()));
            flow.setGroupedBy(GroupedBy.EXPORTER_INTERFACE_CONVERSATION);
            flow.setExporter(toExporterNode(key.nodeRef));
            flow.setIfIndex(key.interfaceRef.ifIndex);
            flow.setConversationKey(key.conversationRef.conversationKey);
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
        public void encode(CompoundKey value, OutputStream os) {
            value.visit(new Visitor() {
                @Override
                public void visit(ExporterKey key) {
                    try {
                        INT_CODER.encode(0, os);
                        NODE_REF_KEY_CODER.encode(key.nodeRef, os);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

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
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void visit(ExporterInterfaceConversationKey key) {
                    try {
                        INT_CODER.encode(4, os);
                        NODE_REF_KEY_CODER.encode(key.nodeRef, os);
                        INT_CODER.encode(key.interfaceRef.ifIndex, os);
                        STRING_CODER.encode(key.conversationRef.conversationKey, os);
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
                return key;
            } else if (type == 4) {
                ExporterInterfaceConversationKey key = new ExporterInterfaceConversationKey();

                key.nodeRef = NODE_REF_KEY_CODER.decode(is);

                key.interfaceRef = new InterfaceRef();
                key.interfaceRef.ifIndex = INT_CODER.decode(is);

                key.conversationRef = new ConversationRef();
                key.conversationRef.conversationKey = STRING_CODER.decode(is);
                return key;
            }
            throw new RuntimeException("Unsupported type: " + type);
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
