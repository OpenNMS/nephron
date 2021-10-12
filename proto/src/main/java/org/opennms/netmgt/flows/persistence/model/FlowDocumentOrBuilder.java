// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: flowdocument.proto

package org.opennms.netmgt.flows.persistence.model;

public interface FlowDocumentOrBuilder extends
    // @@protoc_insertion_point(interface_extends:FlowDocument)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   * Flow timestamp in milliseconds.
   * </pre>
   *
   * <code>uint64 timestamp = 1;</code>
   */
  long getTimestamp();

  /**
   * <pre>
   * Number of bytes transferred in the flow
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_bytes = 2;</code>
   */
  boolean hasNumBytes();
  /**
   * <pre>
   * Number of bytes transferred in the flow
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_bytes = 2;</code>
   */
  com.google.protobuf.UInt64Value getNumBytes();
  /**
   * <pre>
   * Number of bytes transferred in the flow
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_bytes = 2;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getNumBytesOrBuilder();

  /**
   * <pre>
   * Direction of the flow (egress vs ingress)
   * </pre>
   *
   * <code>.Direction direction = 3;</code>
   */
  int getDirectionValue();
  /**
   * <pre>
   * Direction of the flow (egress vs ingress)
   * </pre>
   *
   * <code>.Direction direction = 3;</code>
   */
  org.opennms.netmgt.flows.persistence.model.Direction getDirection();

  /**
   * <pre>
   *  Destination address.
   * </pre>
   *
   * <code>string dst_address = 4;</code>
   */
  java.lang.String getDstAddress();
  /**
   * <pre>
   *  Destination address.
   * </pre>
   *
   * <code>string dst_address = 4;</code>
   */
  com.google.protobuf.ByteString
      getDstAddressBytes();

  /**
   * <pre>
   * Destination address hostname.
   * </pre>
   *
   * <code>string dst_hostname = 5;</code>
   */
  java.lang.String getDstHostname();
  /**
   * <pre>
   * Destination address hostname.
   * </pre>
   *
   * <code>string dst_hostname = 5;</code>
   */
  com.google.protobuf.ByteString
      getDstHostnameBytes();

  /**
   * <pre>
   * Destination autonomous system (AS).
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value dst_as = 6;</code>
   */
  boolean hasDstAs();
  /**
   * <pre>
   * Destination autonomous system (AS).
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value dst_as = 6;</code>
   */
  com.google.protobuf.UInt64Value getDstAs();
  /**
   * <pre>
   * Destination autonomous system (AS).
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value dst_as = 6;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getDstAsOrBuilder();

  /**
   * <pre>
   * The number of contiguous bits in the source address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_mask_len = 7;</code>
   */
  boolean hasDstMaskLen();
  /**
   * <pre>
   * The number of contiguous bits in the source address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_mask_len = 7;</code>
   */
  com.google.protobuf.UInt32Value getDstMaskLen();
  /**
   * <pre>
   * The number of contiguous bits in the source address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_mask_len = 7;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getDstMaskLenOrBuilder();

  /**
   * <pre>
   * Destination port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_port = 8;</code>
   */
  boolean hasDstPort();
  /**
   * <pre>
   * Destination port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_port = 8;</code>
   */
  com.google.protobuf.UInt32Value getDstPort();
  /**
   * <pre>
   * Destination port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dst_port = 8;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getDstPortOrBuilder();

  /**
   * <pre>
   * Slot number of the flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_id = 9;</code>
   */
  boolean hasEngineId();
  /**
   * <pre>
   * Slot number of the flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_id = 9;</code>
   */
  com.google.protobuf.UInt32Value getEngineId();
  /**
   * <pre>
   * Slot number of the flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_id = 9;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getEngineIdOrBuilder();

  /**
   * <pre>
   * Type of flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_type = 10;</code>
   */
  boolean hasEngineType();
  /**
   * <pre>
   * Type of flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_type = 10;</code>
   */
  com.google.protobuf.UInt32Value getEngineType();
  /**
   * <pre>
   * Type of flow-switching engine.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value engine_type = 10;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getEngineTypeOrBuilder();

  /**
   * <pre>
   * Unix timestamp in ms at which the previous exported packet-
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value delta_switched = 11;</code>
   */
  boolean hasDeltaSwitched();
  /**
   * <pre>
   * Unix timestamp in ms at which the previous exported packet-
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value delta_switched = 11;</code>
   */
  com.google.protobuf.UInt64Value getDeltaSwitched();
  /**
   * <pre>
   * Unix timestamp in ms at which the previous exported packet-
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value delta_switched = 11;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getDeltaSwitchedOrBuilder();

  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value first_switched = 12;</code>
   */
  boolean hasFirstSwitched();
  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value first_switched = 12;</code>
   */
  com.google.protobuf.UInt64Value getFirstSwitched();
  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value first_switched = 12;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getFirstSwitchedOrBuilder();

  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value last_switched = 13;</code>
   */
  boolean hasLastSwitched();
  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value last_switched = 13;</code>
   */
  com.google.protobuf.UInt64Value getLastSwitched();
  /**
   * <pre>
   * -associated with this flow was switched.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value last_switched = 13;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getLastSwitchedOrBuilder();

  /**
   * <pre>
   * Number of flow records in the associated packet.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value num_flow_records = 14;</code>
   */
  boolean hasNumFlowRecords();
  /**
   * <pre>
   * Number of flow records in the associated packet.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value num_flow_records = 14;</code>
   */
  com.google.protobuf.UInt32Value getNumFlowRecords();
  /**
   * <pre>
   * Number of flow records in the associated packet.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value num_flow_records = 14;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getNumFlowRecordsOrBuilder();

  /**
   * <pre>
   * Number of packets in the flow.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_packets = 15;</code>
   */
  boolean hasNumPackets();
  /**
   * <pre>
   * Number of packets in the flow.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_packets = 15;</code>
   */
  com.google.protobuf.UInt64Value getNumPackets();
  /**
   * <pre>
   * Number of packets in the flow.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value num_packets = 15;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getNumPacketsOrBuilder();

  /**
   * <pre>
   * Flow packet sequence number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value flow_seq_num = 16;</code>
   */
  boolean hasFlowSeqNum();
  /**
   * <pre>
   * Flow packet sequence number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value flow_seq_num = 16;</code>
   */
  com.google.protobuf.UInt64Value getFlowSeqNum();
  /**
   * <pre>
   * Flow packet sequence number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value flow_seq_num = 16;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getFlowSeqNumOrBuilder();

  /**
   * <pre>
   * Input SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value input_snmp_ifindex = 17;</code>
   */
  boolean hasInputSnmpIfindex();
  /**
   * <pre>
   * Input SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value input_snmp_ifindex = 17;</code>
   */
  com.google.protobuf.UInt32Value getInputSnmpIfindex();
  /**
   * <pre>
   * Input SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value input_snmp_ifindex = 17;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getInputSnmpIfindexOrBuilder();

  /**
   * <pre>
   * Output SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value output_snmp_ifindex = 18;</code>
   */
  boolean hasOutputSnmpIfindex();
  /**
   * <pre>
   * Output SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value output_snmp_ifindex = 18;</code>
   */
  com.google.protobuf.UInt32Value getOutputSnmpIfindex();
  /**
   * <pre>
   * Output SNMP ifIndex.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value output_snmp_ifindex = 18;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getOutputSnmpIfindexOrBuilder();

  /**
   * <pre>
   * IPv4 vs IPv6.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ip_protocol_version = 19;</code>
   */
  boolean hasIpProtocolVersion();
  /**
   * <pre>
   * IPv4 vs IPv6.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ip_protocol_version = 19;</code>
   */
  com.google.protobuf.UInt32Value getIpProtocolVersion();
  /**
   * <pre>
   * IPv4 vs IPv6.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ip_protocol_version = 19;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getIpProtocolVersionOrBuilder();

  /**
   * <pre>
   * Next hop IpAddress.
   * </pre>
   *
   * <code>string next_hop_address = 20;</code>
   */
  java.lang.String getNextHopAddress();
  /**
   * <pre>
   * Next hop IpAddress.
   * </pre>
   *
   * <code>string next_hop_address = 20;</code>
   */
  com.google.protobuf.ByteString
      getNextHopAddressBytes();

  /**
   * <pre>
   * Next hop hostname.
   * </pre>
   *
   * <code>string next_hop_hostname = 21;</code>
   */
  java.lang.String getNextHopHostname();
  /**
   * <pre>
   * Next hop hostname.
   * </pre>
   *
   * <code>string next_hop_hostname = 21;</code>
   */
  com.google.protobuf.ByteString
      getNextHopHostnameBytes();

  /**
   * <pre>
   * IP protocol number i.e 6 for TCP, 17 for UDP
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value protocol = 22;</code>
   */
  boolean hasProtocol();
  /**
   * <pre>
   * IP protocol number i.e 6 for TCP, 17 for UDP
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value protocol = 22;</code>
   */
  com.google.protobuf.UInt32Value getProtocol();
  /**
   * <pre>
   * IP protocol number i.e 6 for TCP, 17 for UDP
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value protocol = 22;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getProtocolOrBuilder();

  /**
   * <pre>
   * Sampling algorithm ID.
   * </pre>
   *
   * <code>.SamplingAlgorithm sampling_algorithm = 23;</code>
   */
  int getSamplingAlgorithmValue();
  /**
   * <pre>
   * Sampling algorithm ID.
   * </pre>
   *
   * <code>.SamplingAlgorithm sampling_algorithm = 23;</code>
   */
  org.opennms.netmgt.flows.persistence.model.SamplingAlgorithm getSamplingAlgorithm();

  /**
   * <pre>
   * Sampling interval.
   * </pre>
   *
   * <code>.google.protobuf.DoubleValue sampling_interval = 24;</code>
   */
  boolean hasSamplingInterval();
  /**
   * <pre>
   * Sampling interval.
   * </pre>
   *
   * <code>.google.protobuf.DoubleValue sampling_interval = 24;</code>
   */
  com.google.protobuf.DoubleValue getSamplingInterval();
  /**
   * <pre>
   * Sampling interval.
   * </pre>
   *
   * <code>.google.protobuf.DoubleValue sampling_interval = 24;</code>
   */
  com.google.protobuf.DoubleValueOrBuilder getSamplingIntervalOrBuilder();

  /**
   * <pre>
   * Source address.
   * </pre>
   *
   * <code>string src_address = 26;</code>
   */
  java.lang.String getSrcAddress();
  /**
   * <pre>
   * Source address.
   * </pre>
   *
   * <code>string src_address = 26;</code>
   */
  com.google.protobuf.ByteString
      getSrcAddressBytes();

  /**
   * <pre>
   * Source hostname.
   * </pre>
   *
   * <code>string src_hostname = 27;</code>
   */
  java.lang.String getSrcHostname();
  /**
   * <pre>
   * Source hostname.
   * </pre>
   *
   * <code>string src_hostname = 27;</code>
   */
  com.google.protobuf.ByteString
      getSrcHostnameBytes();

  /**
   * <pre>
   * Source AS number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value src_as = 28;</code>
   */
  boolean hasSrcAs();
  /**
   * <pre>
   * Source AS number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value src_as = 28;</code>
   */
  com.google.protobuf.UInt64Value getSrcAs();
  /**
   * <pre>
   * Source AS number.
   * </pre>
   *
   * <code>.google.protobuf.UInt64Value src_as = 28;</code>
   */
  com.google.protobuf.UInt64ValueOrBuilder getSrcAsOrBuilder();

  /**
   * <pre>
   * The number of contiguous bits in the destination address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_mask_len = 29;</code>
   */
  boolean hasSrcMaskLen();
  /**
   * <pre>
   * The number of contiguous bits in the destination address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_mask_len = 29;</code>
   */
  com.google.protobuf.UInt32Value getSrcMaskLen();
  /**
   * <pre>
   * The number of contiguous bits in the destination address subnet mask.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_mask_len = 29;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getSrcMaskLenOrBuilder();

  /**
   * <pre>
   * Source port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_port = 30;</code>
   */
  boolean hasSrcPort();
  /**
   * <pre>
   * Source port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_port = 30;</code>
   */
  com.google.protobuf.UInt32Value getSrcPort();
  /**
   * <pre>
   * Source port.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value src_port = 30;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getSrcPortOrBuilder();

  /**
   * <pre>
   * TCP Flags.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tcp_flags = 31;</code>
   */
  boolean hasTcpFlags();
  /**
   * <pre>
   * TCP Flags.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tcp_flags = 31;</code>
   */
  com.google.protobuf.UInt32Value getTcpFlags();
  /**
   * <pre>
   * TCP Flags.
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tcp_flags = 31;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getTcpFlagsOrBuilder();

  /**
   * <pre>
   * TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tos = 32;</code>
   */
  boolean hasTos();
  /**
   * <pre>
   * TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tos = 32;</code>
   */
  com.google.protobuf.UInt32Value getTos();
  /**
   * <pre>
   * TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value tos = 32;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getTosOrBuilder();

  /**
   * <pre>
   * Netflow version
   * </pre>
   *
   * <code>.NetflowVersion netflow_version = 33;</code>
   */
  int getNetflowVersionValue();
  /**
   * <pre>
   * Netflow version
   * </pre>
   *
   * <code>.NetflowVersion netflow_version = 33;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NetflowVersion getNetflowVersion();

  /**
   * <pre>
   * VLAN ID.
   * </pre>
   *
   * <code>string vlan = 34;</code>
   */
  java.lang.String getVlan();
  /**
   * <pre>
   * VLAN ID.
   * </pre>
   *
   * <code>string vlan = 34;</code>
   */
  com.google.protobuf.ByteString
      getVlanBytes();

  /**
   * <code>.NodeInfo src_node = 35;</code>
   */
  boolean hasSrcNode();
  /**
   * <code>.NodeInfo src_node = 35;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfo getSrcNode();
  /**
   * <code>.NodeInfo src_node = 35;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfoOrBuilder getSrcNodeOrBuilder();

  /**
   * <code>.NodeInfo exporter_node = 36;</code>
   */
  boolean hasExporterNode();
  /**
   * <code>.NodeInfo exporter_node = 36;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfo getExporterNode();
  /**
   * <code>.NodeInfo exporter_node = 36;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfoOrBuilder getExporterNodeOrBuilder();

  /**
   * <code>.NodeInfo dest_node = 37;</code>
   */
  boolean hasDestNode();
  /**
   * <code>.NodeInfo dest_node = 37;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfo getDestNode();
  /**
   * <code>.NodeInfo dest_node = 37;</code>
   */
  org.opennms.netmgt.flows.persistence.model.NodeInfoOrBuilder getDestNodeOrBuilder();

  /**
   * <code>string application = 38;</code>
   */
  java.lang.String getApplication();
  /**
   * <code>string application = 38;</code>
   */
  com.google.protobuf.ByteString
      getApplicationBytes();

  /**
   * <code>string host = 39;</code>
   */
  java.lang.String getHost();
  /**
   * <code>string host = 39;</code>
   */
  com.google.protobuf.ByteString
      getHostBytes();

  /**
   * <code>string location = 40;</code>
   */
  java.lang.String getLocation();
  /**
   * <code>string location = 40;</code>
   */
  com.google.protobuf.ByteString
      getLocationBytes();

  /**
   * <code>.Locality src_locality = 41;</code>
   */
  int getSrcLocalityValue();
  /**
   * <code>.Locality src_locality = 41;</code>
   */
  org.opennms.netmgt.flows.persistence.model.Locality getSrcLocality();

  /**
   * <code>.Locality dst_locality = 42;</code>
   */
  int getDstLocalityValue();
  /**
   * <code>.Locality dst_locality = 42;</code>
   */
  org.opennms.netmgt.flows.persistence.model.Locality getDstLocality();

  /**
   * <code>.Locality flow_locality = 43;</code>
   */
  int getFlowLocalityValue();
  /**
   * <code>.Locality flow_locality = 43;</code>
   */
  org.opennms.netmgt.flows.persistence.model.Locality getFlowLocality();

  /**
   * <pre>
   * Applied clock correction im milliseconds.
   * </pre>
   *
   * <code>uint64 clock_correction = 45;</code>
   */
  long getClockCorrection();

  /**
   * <pre>
   * DSCP; upper 6 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dscp = 46;</code>
   */
  boolean hasDscp();
  /**
   * <pre>
   * DSCP; upper 6 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dscp = 46;</code>
   */
  com.google.protobuf.UInt32Value getDscp();
  /**
   * <pre>
   * DSCP; upper 6 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value dscp = 46;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getDscpOrBuilder();

  /**
   * <pre>
   * ECN; lower 2 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ecn = 47;</code>
   */
  boolean hasEcn();
  /**
   * <pre>
   * ECN; lower 2 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ecn = 47;</code>
   */
  com.google.protobuf.UInt32Value getEcn();
  /**
   * <pre>
   * ECN; lower 2 bits of TOS
   * </pre>
   *
   * <code>.google.protobuf.UInt32Value ecn = 47;</code>
   */
  com.google.protobuf.UInt32ValueOrBuilder getEcnOrBuilder();
}
