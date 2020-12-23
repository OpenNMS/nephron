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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
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
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Contains functions used to group flows by different keys.
 *
 * Different types of keys are modeled by the {@link CompoundKeyType} enum and key instances are represented by the
 * {@link CompoundKey} class. The associated {@link CompoundKeyCoder} is used for serdes.
 *
 * Compound keys are made up from a list of parts that correspond to different dimensions flow data can be grouped
 * into. Different types of key parts are modeled by instances of the {@link CompoundKeyTypePart} interface. Instances
 * of key parts are represented by implementations of the {@link Ref} interface.
 *
 * @author jwhite
 */
public class Groupings {

    private interface CompoundKeyTypePart<T extends Ref> {
        void encode(T ref, OutputStream os) throws IOException;

        T decode(InputStream is) throws IOException;

        List<KeyFlowBy.WithHostname<T>> create(FlowDocument flow) throws MissingFieldsException;

        void populate(T ref, FlowSummary summary);
    }

    @FunctionalInterface
    private interface CheckedFunction<T, R, E extends Exception> {
        R apply(T t) throws E;
    }

    @FunctionalInterface
    private interface CheckedConsumer<T> {
        void accept(T t) throws IOException;
    }

    private static <T extends Ref> CompoundKeyTypePart<T> keyPart(
            Function<T, CheckedConsumer<OutputStream>> encode,
            CheckedFunction<InputStream, T, IOException> decode,
            CheckedFunction<FlowDocument, List<KeyFlowBy.WithHostname<T>>, MissingFieldsException> create,
            Function<T, Consumer<FlowSummary>> populate
    ) {
        return new CompoundKeyTypePart<T>() {
            @Override
            public void encode(T ref, OutputStream os) throws IOException {
                encode.apply(ref).accept(os);
            }

            @Override
            public T decode(InputStream is) throws IOException {
                return decode.apply(is);
            }

            @Override
            public List<KeyFlowBy.WithHostname<T>> create(FlowDocument flow) throws MissingFieldsException {
                return create.apply(flow);
            }

            @Override
            public void populate(T ref, FlowSummary summary) {
                populate.apply(ref).accept(summary);
            }
        };
    }

    private final static Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
    private final static Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

    private static <T> List<KeyFlowBy.WithHostname<T>> singlePartWithoutHostName(T t) {
        return Collections.singletonList(KeyFlowBy.WithHostname.<T>having(t).withoutHostname());
    }

    private static final CompoundKeyTypePart<NodeRef> EXPORTER_PART = keyPart(
            ref -> os -> {
                STRING_CODER.encode(ref.getForeignSource(), os);
                STRING_CODER.encode(ref.getForeignId(), os);
                INT_CODER.encode(ref.getNodeId(), os);
            },
            is -> {
                NodeRef ref = new NodeRef();
                ref.setForeignSource(STRING_CODER.decode(is));
                ref.setForeignId(STRING_CODER.decode(is));
                ref.setNodeId(INT_CODER.decode(is));
                return ref;
            },
            flow -> singlePartWithoutHostName(NodeRef.of(flow)),
            ref -> flow -> {
                ExporterNode exporterNode = new ExporterNode();
                exporterNode.setForeignSource(ref.getForeignSource());
                exporterNode.setForeignId(ref.getForeignId());
                exporterNode.setNodeId(ref.getNodeId());
                flow.setExporter(exporterNode);
            }

    );

    private static final CompoundKeyTypePart<InterfaceRef> INTERFACE_PART = keyPart(
            ref -> os -> INT_CODER.encode(ref.getIfIndex(), os),
            is -> {
                InterfaceRef ref = new InterfaceRef();
                ref.setIfIndex(INT_CODER.decode(is));
                return ref;
            },
            flow -> singlePartWithoutHostName(InterfaceRef.of(flow)),
            ref -> flow -> flow.setIfIndex(ref.getIfIndex())
    );

    private static final CompoundKeyTypePart<DscpRef> DSCP_PART = keyPart(
            ref -> os -> INT_CODER.encode(ref.getDscp(), os),
            is -> new DscpRef(INT_CODER.decode(is)),
            flow -> singlePartWithoutHostName(DscpRef.of(flow)),
            ref -> flow -> flow.setDscp(ref.getDscp())
    );

    private static final CompoundKeyTypePart<EcnRef> ECN_PART = keyPart(
            ref -> os -> INT_CODER.encode(ref.getEcn().ordinal(), os),
            is -> new EcnRef(Ecn.values()[INT_CODER.decode(is)]),
            flow -> singlePartWithoutHostName(EcnRef.of(flow)),
            ref -> flow -> flow.setEcn(ref.getEcn().code)
    );

    private static final CompoundKeyTypePart<ApplicationRef> APPLICATION_PART = keyPart(
            ref -> os -> STRING_CODER.encode(ref.getApplication(), os),
            is -> {
                ApplicationRef ref = new ApplicationRef();
                ref.setApplication(STRING_CODER.decode(is));
                return ref;
            },
            flow -> singlePartWithoutHostName(ApplicationRef.of(flow)),
            ref -> flow -> flow.setApplication(ref.getApplication())
    );

    private static final CompoundKeyTypePart<HostRef> HOST_PART = keyPart(
            ref -> os -> STRING_CODER.encode(ref.getAddress(), os),
            is -> {
                HostRef ref = new HostRef();
                ref.setAddress(STRING_CODER.decode(is));
                return ref;
            },
            flow -> Arrays.asList(
                    KeyFlowBy.WithHostname.having(HostRef.of(flow.getSrcAddress())).andHostname(flow.getSrcHostname()),
                    KeyFlowBy.WithHostname.having(HostRef.of(flow.getDstAddress())).andHostname(flow.getDstHostname())
            ),
            ref -> flow -> flow.setHostAddress(ref.getAddress())
    );

    private static final CompoundKeyTypePart<ConversationRef> CONVERSATION_PART = keyPart(
            ref -> os -> STRING_CODER.encode(ref.getConversationKey(), os),
            is -> {
                ConversationRef ref = new ConversationRef();
                ref.setConversationKey(STRING_CODER.decode(is));
                return ref;
            },
            flow -> singlePartWithoutHostName(ConversationRef.of(flow)),
            ref -> flow -> flow.setConversationKey(ref.getConversationKey())
    );


    public enum CompoundKeyType {

        EXPORTER(null, EXPORTER_PART),
        EXPORTER_INTERFACE(EXPORTER, INTERFACE_PART),

        // The ToS aggregation adds two parts to the compound key, namely a part for distinguishing DSCPs and another
        // part for distinguishing ECNs.
        // -> the ToS aggregation and its subaggregations (for applications, conversations, and hosts) include
        //    a "dscp" and a "ecn" field that can be used to filter for specific DSCPs or ECNs.
        // -> the ToS aggregation can be use to retrieve series/summaries for DSCPs or for ECNs by aggregating over all
        //    ECNs or all DSCPs, respectively.
        EXPORTER_INTERFACE_TOS(EXPORTER_INTERFACE, DSCP_PART, ECN_PART),

        EXPORTER_INTERFACE_TOS_APPLICATION(EXPORTER_INTERFACE_TOS, APPLICATION_PART),
        EXPORTER_INTERFACE_TOS_CONVERSATION(EXPORTER_INTERFACE_TOS, CONVERSATION_PART),
        EXPORTER_INTERFACE_TOS_HOST(EXPORTER_INTERFACE_TOS, HOST_PART);

        private CompoundKeyType parent;
        private CompoundKeyTypePart<Ref>[] parts;

        CompoundKeyType(CompoundKeyType parent, CompoundKeyTypePart<? extends Ref>... parts) {
            this.parent = parent;
            this.parts = parent == null ? (CompoundKeyTypePart<Ref>[])parts : ArrayUtils.addAll(parent.parts, (CompoundKeyTypePart<Ref>[])parts);
        }

        CompoundKey decode(InputStream is) throws IOException {
            List<Ref> refs = new ArrayList<>(parts.length);
            for (int i = 0; i < parts.length; i++) {
                refs.add(parts[i].decode(is));
            }
            return new CompoundKey(this, refs);
        }

        void encode(List<Ref> refs, OutputStream os) throws IOException {
            for (int i = 0; i < parts.length; i++) {
                parts[i].encode(refs.get(i), os);
            }
        }

        /**
         * Creates a compound key that may be accompanied by a host name from a flow document.
         *
         * The key parts of the created key are created by delegating to the key type parts of this compound key type.
         */
        List<KeyFlowBy.WithHostname<CompoundKey>> create(FlowDocument flow) throws MissingFieldsException {
            // the method returns a list of compound keys
            // -> a list of lists of the corresponding key parts must be determined
            List<List<KeyFlowBy.WithHostname<Ref>>> refss = null;
            for (CompoundKeyTypePart part : parts) {
                // each key part type yields a list of choices (refs)
                // -> all current lists in refss have to be extended by all choices
                List<KeyFlowBy.WithHostname<Ref>> refs = part.create(flow);
                if (refss == null) {
                    // first part
                    // -> each choice yields a singleton list of key parts
                    refss = refs.stream().map(whn -> Collections.singletonList(whn)).collect(Collectors.toList());
                } else {
                    // append choices to current lists
                    // -> determine the next refss list
                    List<List<KeyFlowBy.WithHostname<Ref>>> next = new ArrayList<>();
                    // for each current list and each choice:
                    // -> copy the current list, extends it by the choice and add it to the next refss
                    for (List<KeyFlowBy.WithHostname<Ref>> prefix : refss) {
                        for (KeyFlowBy.WithHostname<Ref> suffix : refs) {
                            List<KeyFlowBy.WithHostname<Ref>> l = new ArrayList<>();
                            l.addAll(prefix);
                            l.add(suffix);
                            next.add(l);
                        }
                    }
                    refss = next;
                }
            }
            // convert the list of part lists into a list of compound keys that may be accompanied by a host name
            return refss
                    .stream()
                    .map(refs -> {
                        // get the first host name from all the parts
                        String hostname = refs.stream().map(whn -> whn.hostname).filter(hn -> hn != null).findFirst().orElse(null);
                        CompoundKey key = new CompoundKey(this, refs.stream().map(whn -> whn.value).collect(Collectors.toList()));
                        return KeyFlowBy.WithHostname.having(key).andHostname(hostname);
                    })
                    .collect(Collectors.toList());
        }

    }



    @DefaultCoder(CompoundKeyCoder.class)
    public static class CompoundKey {

        private final CompoundKeyType type;
        private final List<Ref> refs;

        private CompoundKey(CompoundKeyType type, List<Ref> refs) {
            this.type = type;
            this.refs = refs;
        }

        /**
         * Build the parent, or "outer" key for the current key.
         *
         * @return the outer key, or null if no such key exists
         */
        public CompoundKey getOuterKey() {
            return new CompoundKey(type.parent, refs.subList(0, type.parent.parts.length));
        }

        public String groupedByKey() {
            return refs.stream().map(Ref::idAsString).collect(Collectors.joining("-"));
        }

        public void populate(FlowSummary flow) {
            flow.setGroupedBy(type);
            flow.setGroupedByKey(groupedByKey());
            for (int i = 0; i < type.parts.length; i++) {
                type.parts[i].populate(refs.get(i), flow);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CompoundKey cKey = (CompoundKey) o;
            return type == cKey.type &&
                   Objects.equals(refs, cKey.refs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, refs);
        }

        @Override
        public String toString() {
            return "CompoundKey{" +
                   "type=" + type +
                   ", refs=" + refs +
                   '}';
        }
    }

    public static class KeyFlowBy extends DoFn<FlowDocument, KV<CompoundKey, Aggregate>> {

        private final CompoundKeyType type;

        public KeyFlowBy(CompoundKeyType type) {
            this.type = type;
        }

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

        public static long bytesInWindow(
                long deltaSwitched,
                long lastSwitchedInclusive,
                double multipliedNumBytes,
                long windowStart,
                long windowEndInclusive
        ) {
            // The flow duration ranges [delta_switched, last_switched] (both bounds are inclusive)
            long flowDurationMs = lastSwitchedInclusive - deltaSwitched + 1;

            // the start (inclusive) of the flow in this window
            long overlapStart = Math.max(deltaSwitched, windowStart);
            // the end (inclusive) of the flow in this window
            long overlapEnd = Math.min(lastSwitchedInclusive, windowEndInclusive);

            // the end of the previous window (inclusive)
            long previousEnd = overlapStart - 1;

            long bytesAtPreviousEnd = (long) ((previousEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);
            long bytesAtEnd = (long) ((overlapEnd - deltaSwitched + 1) * multipliedNumBytes / flowDurationMs);

            return bytesAtEnd - bytesAtPreviousEnd;
        }

        private Aggregate aggregatize(final FlowWindows.FlowWindow window, final FlowDocument flow, final String hostname) {
            double multiplier = 1;
            if (flow.hasSamplingInterval()) {
                double samplingInterval = flow.getSamplingInterval().getValue();
                if (samplingInterval > 0) {
                    multiplier = samplingInterval;
                }
            }
            long bytes = bytesInWindow(
                    flow.getDeltaSwitched().getValue(),
                    flow.getLastSwitched().getValue(),
                    flow.getNumBytes().getValue() * multiplier,
                    window.start().getMillis(),
                    window.maxTimestamp().getMillis()
            );
            // Track
            flowsInWindow.inc();
            return Direction.INGRESS.equals(flow.getDirection()) ?
                   new Aggregate(bytes, 0, hostname) :
                   new Aggregate(0, bytes, hostname);
        }

        @ProcessElement
        public void processElement(ProcessContext c, FlowWindows.FlowWindow window) {
            final FlowDocument flow = c.element();
            try {
                for (final WithHostname<? extends CompoundKey> key: key(flow)) {
                    Aggregate aggregate = aggregatize(window, flow, key.hostname);
                    c.output(KV.of(key.value, aggregate));
                }
            } catch (MissingFieldsException mfe) {
                flowsWithMissingFields.inc();
            }
        }

        public Collection<WithHostname<CompoundKey>> key(FlowDocument flow) throws MissingFieldsException {
            return type.create(flow);
        }
    }

    //
    //
    //

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

    public static class DscpRef implements Ref {
        private int dscp;

        public static DscpRef of(FlowDocument flow) {
            int dscp = flow.getTos().getValue() >>> 2;
            return new DscpRef(dscp);
        }

        public DscpRef (int dscp) {
            this.dscp = dscp;
        }

        public int getDscp() {
            return dscp;
        }

        public void setDscp(int dscp) {
            this.dscp = dscp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DscpRef dscpRef = (DscpRef) o;
            return dscp == dscpRef.dscp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(dscp);
        }

        @Override
        public String toString() {
            return "DscpRef{" +
                   "dscp=" + dscp +
                   '}';
        }

        @Override
        public String idAsString() {
            return Integer.toString(dscp);
        }
    }

    public static class EcnRef implements Ref {
        private Ecn ecn;

        public static EcnRef of(FlowDocument flow) {
            Ecn ecn;
            switch(flow.getTos().getValue() & 0x03) {
                case 0: ecn = Ecn.NON_ECT; break;
                case 1:
                case 2: ecn = Ecn.ECT; break;
                default: ecn = Ecn.CE;
            }
            return new EcnRef(ecn);
        }

        public EcnRef (Ecn ecn) {
            this.ecn = ecn;
        }

        public Ecn getEcn() {
            return ecn;
        }

        public void setEcn(Ecn ecn) {
            this.ecn = ecn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            EcnRef ecnRef = (EcnRef) o;
            return ecn == ecnRef.ecn;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ecn);
        }

        @Override
        public String toString() {
            return "EcnRef{" +
                   "ecn=" + ecn +
                   '}';
        }

        @Override
        public String idAsString() {
            return ecn.name();
        }
    }

    public static class ApplicationRef implements Ref {
        private String application;

        public static ApplicationRef of(String application) {
            ApplicationRef applicationRef = new ApplicationRef();
            applicationRef.setApplication(application);
            return applicationRef;
        }

        public static ApplicationRef of(FlowDocument flow) {
            String application = flow.getApplication();
            return Strings.isNullOrEmpty(application) ? of(FlowSummary.UNKNOWN_APPLICATION_NAME_KEY) : of(application);
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

    public static class CompoundKeyCoder extends AtomicCoder<CompoundKey> {
        @Override
        public void encode(CompoundKey value, OutputStream outStream) throws CoderException, IOException {
            INT_CODER.encode(value.type.ordinal(), outStream);
            value.type.encode(value.refs, outStream);
        }

        @Override
        public CompoundKey decode(InputStream inStream) throws CoderException, IOException {
            CompoundKeyType type = CompoundKeyType.values()[INT_CODER.decode(inStream)];
            return type.decode(inStream);
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }

    }

}
