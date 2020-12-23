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

import java.util.Objects;

import org.opennms.nephron.elastic.FlowSummary;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.google.common.base.Strings;

/**
 * Represents the value a compound key has for a dimension.
 */
abstract class Ref {

    abstract String idAsString();

    public static class Node extends Ref {
        private String foreignSource;
        private String foreignId;
        private Integer nodeId;

        public static Node of(int nodeId, String foreignSource, String foreignId) {
            Node nodeRef = new Node();
            nodeRef.setNodeId(nodeId);
            nodeRef.setForeignSource(foreignSource);
            nodeRef.setForeignId(foreignId);
            return nodeRef;
        }

        public static Node of(int nodeId) {
            Node nodeRef = new Node();
            nodeRef.setNodeId(nodeId);
            return nodeRef;
        }

        public static Node of(FlowDocument flow) throws MissingFieldsException {
            if (!flow.hasExporterNode()) {
                throw new MissingFieldsException("exporterNode", flow);
            }
            final NodeInfo exporterNode = flow.getExporterNode();
            if (!Strings.isNullOrEmpty(exporterNode.getForeignSource())
                    && !Strings.isNullOrEmpty(exporterNode.getForeginId())) {
                return Node.of(exporterNode.getNodeId(), exporterNode.getForeignSource(), exporterNode.getForeginId());
            } else {
                return Node.of(exporterNode.getNodeId());
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
            Node nodeRef = (Node) o;
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

    public static class Interface extends Ref {
        private int ifIndex;

        public static Interface of(int ifIndex) {
            Interface interfaceRef = new Interface();
            interfaceRef.setIfIndex(ifIndex);
            return interfaceRef;
        }

        public static Interface of(FlowDocument flow) {
            if (Direction.INGRESS.equals(flow.getDirection())) {
                return Interface.of(flow.getInputSnmpIfindex().getValue());
            } else {
                return Interface.of(flow.getOutputSnmpIfindex().getValue());
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
            Interface that = (Interface) o;
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

    public static class Dscp extends Ref {
        private int dscp;

        public static int DEFAULT_CODE = 0;

        public static Dscp of(FlowDocument flow) {
            int dscp = flow.hasDscp() ? flow.getDscp().getValue() : DEFAULT_CODE;
            return new Dscp(dscp);
        }

        public Dscp(int dscp) {
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
            Dscp dscpRef = (Dscp) o;
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

    public static class Ecn extends Ref {
        private org.opennms.nephron.Ecn ecn;

        public static Ecn of(NephronOptions opts, FlowDocument flow) {
            org.opennms.nephron.Ecn ecn;
            if (opts.getKeyByEcn() && flow.hasEcn()) {
                ecn = org.opennms.nephron.Ecn.fromCode(flow.getEcn().getValue());
            } else {
                ecn = org.opennms.nephron.Ecn.IGNORED;
            }
            return new Ecn(ecn);
        }

        public Ecn(org.opennms.nephron.Ecn ecn) {
            this.ecn = Objects.requireNonNull(ecn);
        }

        public org.opennms.nephron.Ecn getEcn() {
            return ecn;
        }

        public void setEcn(org.opennms.nephron.Ecn ecn) {
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
            Ecn ecnRef = (Ecn) o;
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

    public static class Application extends Ref {
        private String application;

        public static Application of(String application) {
            Application applicationRef = new Application();
            applicationRef.setApplication(application);
            return applicationRef;
        }

        public static Application of(FlowDocument flow) {
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
            Application that = (Application) o;
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

    public static class Host extends Ref {
        private String address;

        public static Host of(String address) {
            Host hostRef = new Host();
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
            Host hostRef = (Host) o;
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

    public static class Conversation extends Ref {
        private String conversationKey;

        public static Conversation of(String conversationKey) {
            Conversation conversationRef = new Conversation();
            conversationRef.setConversationKey(conversationKey);
            return conversationRef;
        }

        public static Conversation of(FlowDocument flow) {
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
            Conversation that = (Conversation) o;
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
}
