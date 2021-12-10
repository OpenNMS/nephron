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

package org.opennms.nephron.testing.flowgen;

import static org.opennms.nephron.testing.flowgen.FlowDocuments.getFlowDataArbitrary;
import static org.opennms.nephron.testing.flowgen.FlowDocuments.ipAddress;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;

import com.codepoetics.protonpack.StreamUtils;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

/**
 * Generates a file that can be used as a bulk request to post raw flow data into ElasticSearch.
 * <ul>
 * <li>The generated flows have timestamps starting at epoche + 100 days + 30 minutes.</li>
 * <li>OpenNMS uses hourly indexes. The bulk request file has to be posted into the index "netflow-1970-04-11-00"</li>
 * </ul>
 * <pre>
 * curl -s -o /dev/null  -H "Content-Type: application/x-ndjson" -XPOST localhost:9200/netflow-1970-04-11-00/_bulk --data-binary  @es-bulk-request.json
 * </pre>
 */
public class GenerateElasticSearchBulkRequests {

    private static final Instant START_TIMESTAMP = Instant.EPOCH.plus(Duration.standardDays(100)).plus(Duration.standardMinutes(30));
    private static final int NUMBER_OF_FLOWS = 1000;
    private static final Duration DURATION_BETWEEN_FLOWS = Duration.millis(10);

    public static void main(String[] args) throws Exception {
        var df = DateTimeFormat.forPattern("YYYY-MM-dd-HH").withZoneUTC();
        System.out.println("generate flows for index: netflow-" + df.print(START_TIMESTAMP));
        var flowConfig = new FlowConfig(
                2, // minExporter (minimum nodeId)
                2, // numExporters (nodeIds minExporter ... minExporter + numExporters - 1 are used)
                3, // minInterface (minimum snmp input and output interface number
                2, // numInterfaces
                4, // numProtocols
                4, // numApplications
                4, // numHosts
                4, // numEcns
                4, // numDscp
                FlowConfig.linearIncreasingLastSwitchedPolicy(START_TIMESTAMP, DURATION_BETWEEN_FLOWS),
                i -> Duration.ZERO, // no clock skew
                Duration.ZERO, // last switched sigma
                0.25 // lambda (decay) factor for flow duration (cf. FlowGenOptions#flowDurationLambda)
        );

        var flowDataStream = getFlowDataArbitrary(flowConfig)
                .generator(1000)
                .stream(new Random(123456l))
                .limit(NUMBER_OF_FLOWS);

        var indexAction = "{\"index\":{}}";

        var flows = StreamUtils.zipWithIndex(flowDataStream)
                .map(zipped -> {
                    var idx = zipped.getIndex();
                    var fd = zipped.getValue().value();
                    var fd1 = fd.fd1;
                    var fd2 = fd.fd2;
                    var lastSwitched = flowConfig.lastSwitched.apply(idx, fd).plus(fd2.lastSwitchedOffset);
                    var deltaSwitched = lastSwitched.minus(fd.fd2.flowDuration);
                    // scale the number of bytes of a flow depending on the application, srcAddr, and dstAddr
                    // -> use an exponential model for the scale factor
                    // -> the bigger the numbers for application, srcAddr, dstAdr are the smaller the scale factor is
                    var numBytesScaleFactor = 0.5 * Math.exp(-0.5 * (fd.fd1.application + fd.fd1.srcAddr + fd.fd1.dstAddr));

                    String address, largerAddress;
                    if (fd1.srcAddr < fd1.dstAddr) {
                        address = ipAddress(fd1.srcAddr);
                        largerAddress = ipAddress(fd1.dstAddr);
                    } else {
                        address = ipAddress(fd1.dstAddr);
                        largerAddress = ipAddress(fd1.srcAddr);
                    }
                    StringBuilder sb = new StringBuilder();
                    sb.append('[')
                            .append("\"Default\"")
                            .append(',')
                            .append(fd1.protocol)
                            .append(',')
                            .append("\"" + address + "\"")
                            .append(',')
                            .append("\"" + largerAddress + "\"")
                            .append(',')
                            .append("\"app" + fd1.application + "\"")
                            .append(']');
                    var convoKey = sb.toString();

                    var f = new FlowDocument();
                    var nodeExporter = new NodeDocument();
                    nodeExporter.setNodeId(fd1.nodeId);
                    nodeExporter.setForeignId("fId" + fd1.nodeId);
                    nodeExporter.setForeignSource("fSource" + fd1.nodeId);
                    f.setTimestamp(lastSwitched.getMillis());
                    f.setNodeExporter(nodeExporter);
                    f.setLocation("Default");
                    f.setProtocol(fd1.protocol);
                    f.setInputSnmp(fd1.inputInterfaceIdx);
                    f.setOutputSnmp(fd1.outputInterfaceIdx);
                    f.setApplication("app" + fd1.application);
                    f.setConvoKey(convoKey);
                    f.setSrcAddr(ipAddress(fd1.srcAddr));
                    f.setSrcAddrHostname("host" + fd1.srcAddr);
                    f.setDstAddr(ipAddress(fd1.dstAddr));
                    f.setDstAddrHostname("host" + fd1.dstAddr);
                    f.setDirection(fd2.ingressNotEgress ? Direction.INGRESS : Direction.EGRESS);
                    f.setFirstSwitched(deltaSwitched.getMillis());
                    f.setLastSwitched(lastSwitched.getMillis());
                    f.setDeltaSwitched(deltaSwitched.getMillis());
                    f.setBytes((long)(fd2.bytes * numBytesScaleFactor));
                    f.setTos(fd2.ecn + 4 * fd2.dscp);
                    f.setEcn(fd2.ecn);
                    f.setDscp(fd2.dscp);
                    return f;
                })
                .map(flowDocument -> GSON.toJson(flowDocument))
                .flatMap(s -> Stream.of(indexAction, s))
                ;

        Files.write(Path.of("es-bulk-request.json"), (Iterable<String>)flows::iterator, StandardCharsets.UTF_8);
    }

    private static final Gson GSON = new Gson();

    public enum Direction {
        @SerializedName("ingress")
        INGRESS,
        @SerializedName("egress")
        EGRESS;
    }

    public enum Locality {
        @SerializedName("public")
        PUBLIC("public"),
        @SerializedName("private")
        PRIVATE("private");

        private final String value;

        private Locality(String value) {
            this.value = Objects.requireNonNull(value);
        }

        public String getValue() {
            return value;
        }
    }

    public enum SamplingAlgorithm {
        @SerializedName("Unassigned")
        Unassigned,
        @SerializedName("SystematicCountBasedSampling")
        SystematicCountBasedSampling,
        @SerializedName("SystematicTimeBasedSampling")
        SystematicTimeBasedSampling,
        @SerializedName("RandomNoutOfNSampling")
        RandomNoutOfNSampling,
        @SerializedName("UniformProbabilisticSampling")
        UniformProbabilisticSampling,
        @SerializedName("PropertyMatchFiltering")
        PropertyMatchFiltering,
        @SerializedName("HashBasedFiltering")
        HashBasedFiltering,
        @SerializedName("FlowStateDependentIntermediateFlowSelectionProcess")
        FlowStateDependentIntermediateFlowSelectionProcess;
    }

    public enum NetflowVersion {
        @SerializedName("Netflow v5")
        V5,
        @SerializedName("Netflow v9")
        V9,
        @SerializedName("IPFIX")
        IPFIX,
        @SerializedName("SFLOW")
        SFLOW;
    }

    public static class FlowDocument {
        private static final int DOCUMENT_VERSION = 1;

        public FlowDocument() {
        }

        /**
         * Flow timestamp in milliseconds.
         */
        @SerializedName("@timestamp")
        private long timestamp;

        /**
         * Applied clock correction im milliseconds.
         */
        @SerializedName("@clock_correction")
        private long clockCorrection;

        /**
         * Schema version.
         */
        @SerializedName("@version")
        private Integer version = DOCUMENT_VERSION;

        /**
         * Exporter IP address.
         */
        @SerializedName("host")
        private String host;

        /**
         * The set of all hosts that are involved in this flow. This should include at a minimum the src and dst IP
         * addresses and may also include host names for those IPs.
         */
        @SerializedName("hosts")
        private Set<String> hosts = new LinkedHashSet<>();

        /**
         * Exported location.
         */
        @SerializedName("location")
        private String location;

        /**
         * Application name as determined by the
         * classification engine.
         */
        @SerializedName("netflow.application")
        private String application;

        /**
         * Number of bytes transferred in the flow.
         */
        @SerializedName("netflow.bytes")
        private Long bytes;

        /**
         * Key used to group and identify conversations
         */
        @SerializedName("netflow.convo_key")
        private String convoKey;

        /**
         * Direction of the flow (egress vs ingress)
         */
        @SerializedName("netflow.direction")
        private Direction direction;

        /**
         * Destination address.
         */
        @SerializedName("netflow.dst_addr")
        private String dstAddr;

        /**
         * Destination address hostname.
         */
        @SerializedName("netflow.dst_addr_hostname")
        private String dstAddrHostname;

        /**
         * Destination autonomous system (AS).
         */
        @SerializedName("netflow.dst_as")
        private Long dstAs;

        /**
         * Locality of the destination address (i.e. private vs public address)
         */
        @SerializedName("netflow.dst_locality")
        private Locality dstLocality;

        /**
         * The number of contiguous bits in the source address subnet mask.
         */
        @SerializedName("netflow.dst_mask_len")
        private Integer dstMaskLen;

        /**
         * Destination port.
         */
        @SerializedName("netflow.dst_port")
        private Integer dstPort;

        /**
         * Slot number of the flow-switching engine.
         */
        @SerializedName("netflow.engine_id")
        private Integer engineId;

        /**
         * Type of flow-switching engine.
         */
        @SerializedName("netflow.engine_type")
        private Integer engineType;

        /**
         * Unix timestamp in ms at which the first packet
         * associated with this flow was switched.
         */
        @SerializedName("netflow.first_switched")
        private Long firstSwitched;

        /**
         * Locality of the flow:
         * private if both the source and destination localities are private,
         * and public otherwise.
         */
        @SerializedName("netflow.flow_locality")
        private Locality flowLocality;

        /**
         * Number of flow records in the associated packet.
         */
        @SerializedName("netflow.flow_records")
        private int flowRecords;

        /**
         * Flow packet sequence number.
         */
        @SerializedName("netflow.flow_seq_num")
        private long flowSeqNum;

        /**
         * SNMP ifIndex
         */
        @SerializedName("netflow.input_snmp")
        private Integer inputSnmp;

        /**
         * IPv4 vs IPv6
         */
        @SerializedName("netflow.ip_protocol_version")
        private Integer ipProtocolVersion;

        /**
         * Unix timestamp in ms at which the last packet
         * associated with this flow was switched.
         */
        @SerializedName("netflow.last_switched")
        private Long lastSwitched;

        /**
         * Next hop
         */
        @SerializedName("netflow.next_hop")
        private String nextHop;

        /**
         * Next hop hostname
         */
        @SerializedName("netflow.next_hop_hostname")
        private String nextHopHostname;

        /**
         * SNMP ifIndex
         */
        @SerializedName("netflow.output_snmp")
        private Integer outputSnmp;

        /**
         * Number of packets in the flow
         */
        @SerializedName("netflow.packets")
        private Long packets;

        /**
         * IP protocol number i.e 6 for TCP, 17 for UDP
         */
        @SerializedName("netflow.protocol")
        private Integer protocol;

        /**
         * Sampling algorithm ID
         */
        @SerializedName("netflow.sampling_algorithm")
        private SamplingAlgorithm samplingAlgorithm;

        /**
         * Sampling interval
         */
        @SerializedName("netflow.sampling_interval")
        private Double samplingInterval;

        /**
         * Source address.
         */
        @SerializedName("netflow.src_addr")
        private String srcAddr;

        /**
         * Source address hostname.
         */
        @SerializedName("netflow.src_addr_hostname")
        private String srcAddrHostname;

        /**
         * Source autonomous system (AS).
         */
        @SerializedName("netflow.src_as")
        private Long srcAs;

        /**
         * Locality of the source address (i.e. private vs public address)
         */
        @SerializedName("netflow.src_locality")
        private Locality srcLocality;

        /**
         * The number of contiguous bits in the destination address subnet mask.
         */
        @SerializedName("netflow.src_mask_len")
        private Integer srcMaskLen;

        /**
         * Source port.
         */
        @SerializedName("netflow.src_port")
        private Integer srcPort;

        /**
         * TCP Flags.
         */
        @SerializedName("netflow.tcp_flags")
        private Integer tcpFlags;

        /**
         * Unix timestamp in ms at which the previous exported packet
         * associated with this flow was switched.
         */
        @SerializedName("netflow.delta_switched")
        private Long deltaSwitched;

        /**
         * TOS.
         */
        @SerializedName("netflow.tos")
        private Integer tos;

        @SerializedName("netflow.ecn")
        private Integer ecn;

        @SerializedName("netflow.dscp")
        private Integer dscp;

        /**
         * Netfow version
         */
        @SerializedName("netflow.version")
        private NetflowVersion netflowVersion;

        /**
         * VLAN Name.
         */
        @SerializedName("netflow.vlan")
        private String vlan;

        /**
         * Destination node details.
         */
        @SerializedName("node_dst")
        private NodeDocument nodeDst;

        /**
         * Exported node details.
         */
        @SerializedName("node_exporter")
        private NodeDocument nodeExporter;

        /**
         * Source node details.
         */
        @SerializedName("node_src")
        private NodeDocument nodeSrc;

        public void addHost(String host) {
            Objects.requireNonNull(host);
            hosts.add(host);
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getClockCorrection() {
            return this.clockCorrection;
        }

        public void setClockCorrection(final long clockCorrection) {
            this.clockCorrection = clockCorrection;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public Set<String> getHosts() {
            return hosts;
        }

        public void setHosts(Set<String> hosts) {
            this.hosts = hosts;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public String getApplication() {
            return application;
        }

        public void setApplication(String application) {
            this.application = application;
        }

        public Long getBytes() {
            return bytes;
        }

        public void setBytes(Long bytes) {
            this.bytes = bytes;
        }

        public String getConvoKey() {
            return convoKey;
        }

        public void setConvoKey(String convoKey) {
            this.convoKey = convoKey;
        }

        public Direction getDirection() {
            return direction;
        }

        public void setDirection(Direction direction) {
            this.direction = direction;
        }

        public String getDstAddr() {
            return dstAddr;
        }

        public void setDstAddr(String dstAddr) {
            addHost(dstAddr);
            this.dstAddr = dstAddr;
        }

        public String getDstAddrHostname() {
            return dstAddrHostname;
        }

        public void setDstAddrHostname(String dstAddrHostname) {
            this.dstAddrHostname = dstAddrHostname;
        }

        public Long getDstAs() {
            return dstAs;
        }

        public void setDstAs(Long dstAs) {
            this.dstAs = dstAs;
        }

        public Locality getDstLocality() {
            return dstLocality;
        }

        public void setDstLocality(Locality dstLocality) {
            this.dstLocality = dstLocality;
        }

        public Integer getDstMaskLen() {
            return dstMaskLen;
        }

        public void setDstMaskLen(Integer dstMaskLen) {
            this.dstMaskLen = dstMaskLen;
        }

        public Integer getDstPort() {
            return dstPort;
        }

        public void setDstPort(Integer dstPort) {
            this.dstPort = dstPort;
        }

        public Integer getEngineId() {
            return engineId;
        }

        public void setEngineId(Integer engineId) {
            this.engineId = engineId;
        }

        public Integer getEngineType() {
            return engineType;
        }

        public void setEngineType(Integer engineType) {
            this.engineType = engineType;
        }

        public Long getFirstSwitched() {
            return firstSwitched;
        }

        public void setFirstSwitched(Long firstSwitched) {
            this.firstSwitched = firstSwitched;
        }

        public Locality getFlowLocality() {
            return flowLocality;
        }

        public void setFlowLocality(Locality flowLocality) {
            this.flowLocality = flowLocality;
        }

        public int getFlowRecords() {
            return flowRecords;
        }

        public void setFlowRecords(int flowRecords) {
            this.flowRecords = flowRecords;
        }

        public long getFlowSeqNum() {
            return flowSeqNum;
        }

        public void setFlowSeqNum(long flowSeqNum) {
            this.flowSeqNum = flowSeqNum;
        }

        public Integer getInputSnmp() {
            return inputSnmp;
        }

        public void setInputSnmp(Integer inputSnmp) {
            this.inputSnmp = inputSnmp;
        }

        public Integer getIpProtocolVersion() {
            return ipProtocolVersion;
        }

        public void setIpProtocolVersion(Integer ipProtocolVersion) {
            this.ipProtocolVersion = ipProtocolVersion;
        }

        public Long getLastSwitched() {
            return lastSwitched;
        }

        public void setLastSwitched(Long lastSwitched) {
            this.lastSwitched = lastSwitched;
        }

        public String getNextHop() {
            return nextHop;
        }

        public void setNextHop(String nextHop) {
            this.nextHop = nextHop;
        }

        public String getNextHopHostname() {
            return nextHopHostname;
        }

        public void setNextHopHostname(String nextHopHostname) {
            this.nextHopHostname = nextHopHostname;
        }

        public Integer getOutputSnmp() {
            return outputSnmp;
        }

        public void setOutputSnmp(Integer outputSnmp) {
            this.outputSnmp = outputSnmp;
        }

        public Long getPackets() {
            return packets;
        }

        public void setPackets(Long packets) {
            this.packets = packets;
        }

        public Integer getProtocol() {
            return protocol;
        }

        public void setProtocol(Integer protocol) {
            this.protocol = protocol;
        }

        public SamplingAlgorithm getSamplingAlgorithm() {
            return samplingAlgorithm;
        }

        public void setSamplingAlgorithm(SamplingAlgorithm samplingAlgorithm) {
            this.samplingAlgorithm = samplingAlgorithm;
        }

        public Double getSamplingInterval() {
            return samplingInterval;
        }

        public void setSamplingInterval(Double samplingInterval) {
            this.samplingInterval = samplingInterval;
        }

        public String getSrcAddr() {
            return srcAddr;
        }

        public void setSrcAddr(String srcAddr) {
            addHost(srcAddr);
            this.srcAddr = srcAddr;
        }

        public String getSrcAddrHostname() {
            return srcAddrHostname;
        }

        public void setSrcAddrHostname(String srcAddrHostname) {
            this.srcAddrHostname = srcAddrHostname;
        }

        public Long getSrcAs() {
            return srcAs;
        }

        public void setSrcAs(Long srcAs) {
            this.srcAs = srcAs;
        }

        public Locality getSrcLocality() {
            return srcLocality;
        }

        public void setSrcLocality(Locality srcLocality) {
            this.srcLocality = srcLocality;
        }

        public Integer getSrcMaskLen() {
            return srcMaskLen;
        }

        public void setSrcMaskLen(Integer srcMaskLen) {
            this.srcMaskLen = srcMaskLen;
        }

        public Integer getSrcPort() {
            return srcPort;
        }

        public void setSrcPort(Integer srcPort) {
            this.srcPort = srcPort;
        }

        public Integer getTcpFlags() {
            return tcpFlags;
        }

        public void setTcpFlags(Integer tcpFlags) {
            this.tcpFlags = tcpFlags;
        }

        public Long getDeltaSwitched() {
            return deltaSwitched;
        }

        public void setDeltaSwitched(Long deltaSwitched) {
            this.deltaSwitched = deltaSwitched;
        }

        public Integer getTos() {
            return tos;
        }

        public void setTos(final Integer tos) {
            if (tos != null) {
                setDscp((tos & 0b11111100) >> 2);
                setEcn(tos & 0b00000011);
            } else {
                setDscp(null);
                setEcn(null);
            }
            this.tos = tos;
        }

        private void setEcn(final Integer ecn) {
            this.ecn = ecn;
        }

        private void setDscp(final Integer dscp) {
            this.dscp = dscp;
        }

        public Integer getEcn() {
            return ecn;
        }

        public Integer getDscp() {
            return dscp;
        }

        public NetflowVersion getNetflowVersion() {
            return netflowVersion;
        }

        public void setNetflowVersion(NetflowVersion netflowVersion) {
            this.netflowVersion = netflowVersion;
        }

        public String getVlan() {
            return vlan;
        }

        public void setVlan(String vlan) {
            this.vlan = vlan;
        }

        public NodeDocument getNodeDst() {
            return nodeDst;
        }

        public void setNodeDst(NodeDocument nodeDst) {
            this.nodeDst = nodeDst;
        }

        public NodeDocument getNodeExporter() {
            return nodeExporter;
        }

        public void setNodeExporter(NodeDocument nodeExporter) {
            this.nodeExporter = nodeExporter;
        }

        public NodeDocument getNodeSrc() {
            return nodeSrc;
        }

        public void setNodeSrc(NodeDocument nodeSrc) {
            this.nodeSrc = nodeSrc;
        }

    }

    public static class NodeDocument {
        @SerializedName("foreign_source")
        private String foreignSource;

        @SerializedName("foreign_id")
        private String foreignId;

        @SerializedName("node_id")
        private Integer nodeId;

        @SerializedName("categories")
        private List<String> categories = new LinkedList<>();

        public void setForeignSource(String foreignSource) {
            this.foreignSource = foreignSource;
        }

        public String getForeignSource() {
            return foreignSource;
        }

        public void setForeignId(String foreignId) {
            this.foreignId = foreignId;
        }

        public String getForeignId() {
            return foreignId;
        }

        public Integer getNodeId() {
            return nodeId;
        }

        public void setNodeId(Integer nodeId) {
            this.nodeId = nodeId;
        }

        public List<String> getCategories() {
            return categories;
        }

        public void setCategories(List<String> categories) {
            this.categories = categories;
        }
    }

}
