// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: flowdocument.proto

package org.opennms.netmgt.flows.persistence.model;

public final class EnrichedFlowProtos {
  private EnrichedFlowProtos() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_NodeInfo_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_NodeInfo_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_FlowDocument_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_FlowDocument_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\022flowdocument.proto\032\036google/protobuf/wr" +
      "appers.proto\"[\n\010NodeInfo\022\026\n\016foreign_sour" +
      "ce\030\001 \001(\t\022\022\n\nforegin_id\030\002 \001(\t\022\017\n\007node_id\030" +
      "\003 \001(\r\022\022\n\ncategories\030\004 \003(\t\"\216\016\n\014FlowDocume" +
      "nt\022\021\n\ttimestamp\030\001 \001(\004\022/\n\tnum_bytes\030\002 \001(\013" +
      "2\034.google.protobuf.UInt64Value\022\035\n\tdirect" +
      "ion\030\003 \001(\0162\n.Direction\022\023\n\013dst_address\030\004 \001" +
      "(\t\022\024\n\014dst_hostname\030\005 \001(\t\022,\n\006dst_as\030\006 \001(\013" +
      "2\034.google.protobuf.UInt64Value\0222\n\014dst_ma" +
      "sk_len\030\007 \001(\0132\034.google.protobuf.UInt32Val" +
      "ue\022.\n\010dst_port\030\010 \001(\0132\034.google.protobuf.U" +
      "Int32Value\022/\n\tengine_id\030\t \001(\0132\034.google.p" +
      "rotobuf.UInt32Value\0221\n\013engine_type\030\n \001(\013" +
      "2\034.google.protobuf.UInt32Value\0224\n\016delta_" +
      "switched\030\013 \001(\0132\034.google.protobuf.UInt64V" +
      "alue\0224\n\016first_switched\030\014 \001(\0132\034.google.pr" +
      "otobuf.UInt64Value\0223\n\rlast_switched\030\r \001(" +
      "\0132\034.google.protobuf.UInt64Value\0226\n\020num_f" +
      "low_records\030\016 \001(\0132\034.google.protobuf.UInt" +
      "32Value\0221\n\013num_packets\030\017 \001(\0132\034.google.pr" +
      "otobuf.UInt64Value\0222\n\014flow_seq_num\030\020 \001(\013" +
      "2\034.google.protobuf.UInt64Value\0228\n\022input_" +
      "snmp_ifindex\030\021 \001(\0132\034.google.protobuf.UIn" +
      "t32Value\0229\n\023output_snmp_ifindex\030\022 \001(\0132\034." +
      "google.protobuf.UInt32Value\0229\n\023ip_protoc" +
      "ol_version\030\023 \001(\0132\034.google.protobuf.UInt3" +
      "2Value\022\030\n\020next_hop_address\030\024 \001(\t\022\031\n\021next" +
      "_hop_hostname\030\025 \001(\t\022.\n\010protocol\030\026 \001(\0132\034." +
      "google.protobuf.UInt32Value\022.\n\022sampling_" +
      "algorithm\030\027 \001(\0162\022.SamplingAlgorithm\0227\n\021s" +
      "ampling_interval\030\030 \001(\0132\034.google.protobuf" +
      ".DoubleValue\022\023\n\013src_address\030\032 \001(\t\022\024\n\014src" +
      "_hostname\030\033 \001(\t\022,\n\006src_as\030\034 \001(\0132\034.google" +
      ".protobuf.UInt64Value\0222\n\014src_mask_len\030\035 " +
      "\001(\0132\034.google.protobuf.UInt32Value\022.\n\010src" +
      "_port\030\036 \001(\0132\034.google.protobuf.UInt32Valu" +
      "e\022/\n\ttcp_flags\030\037 \001(\0132\034.google.protobuf.U" +
      "Int32Value\022)\n\003tos\030  \001(\0132\034.google.protobu" +
      "f.UInt32Value\022(\n\017netflow_version\030! \001(\0162\017" +
      ".NetflowVersion\022\014\n\004vlan\030\" \001(\t\022\033\n\010src_nod" +
      "e\030# \001(\0132\t.NodeInfo\022 \n\rexporter_node\030$ \001(" +
      "\0132\t.NodeInfo\022\034\n\tdest_node\030% \001(\0132\t.NodeIn" +
      "fo\022\023\n\013application\030& \001(\t\022\014\n\004host\030\' \001(\t\022\020\n" +
      "\010location\030( \001(\t\022\037\n\014src_locality\030) \001(\0162\t." +
      "Locality\022\037\n\014dst_locality\030* \001(\0162\t.Localit" +
      "y\022 \n\rflow_locality\030+ \001(\0162\t.Locality\022\030\n\020c" +
      "lock_correction\030- \001(\004\022*\n\004dscp\030. \001(\0132\034.go" +
      "ogle.protobuf.UInt32Value\022)\n\003ecn\030/ \001(\0132\034" +
      ".google.protobuf.UInt32ValueJ\004\010,\020-*$\n\tDi" +
      "rection\022\013\n\007INGRESS\020\000\022\n\n\006EGRESS\020\001*\246\002\n\021Sam" +
      "plingAlgorithm\022\016\n\nUNASSIGNED\020\000\022#\n\037SYSTEM" +
      "ATIC_COUNT_BASED_SAMPLING\020\001\022\"\n\036SYSTEMATI" +
      "C_TIME_BASED_SAMPLING\020\002\022\036\n\032RANDOM_N_OUT_" +
      "OF_N_SAMPLING\020\003\022\"\n\036UNIFORM_PROBABILISTIC" +
      "_SAMPLING\020\004\022\034\n\030PROPERTY_MATCH_FILTERING\020" +
      "\005\022\030\n\024HASH_BASED_FILTERING\020\006\022<\n8FLOW_STAT" +
      "E_DEPENDENT_INTERMEDIATE_FLOW_SELECTION_" +
      "PROCESS\020\007*6\n\016NetflowVersion\022\006\n\002V5\020\000\022\006\n\002V" +
      "9\020\001\022\t\n\005IPFIX\020\002\022\t\n\005SFLOW\020\003*#\n\010Locality\022\n\n" +
      "\006PUBLIC\020\000\022\013\n\007PRIVATE\020\001BB\n*org.opennms.ne" +
      "tmgt.flows.persistence.modelB\022EnrichedFl" +
      "owProtosP\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.WrappersProto.getDescriptor(),
        });
    internal_static_NodeInfo_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_NodeInfo_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_NodeInfo_descriptor,
        new java.lang.String[] { "ForeignSource", "ForeginId", "NodeId", "Categories", });
    internal_static_FlowDocument_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_FlowDocument_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_FlowDocument_descriptor,
        new java.lang.String[] { "Timestamp", "NumBytes", "Direction", "DstAddress", "DstHostname", "DstAs", "DstMaskLen", "DstPort", "EngineId", "EngineType", "DeltaSwitched", "FirstSwitched", "LastSwitched", "NumFlowRecords", "NumPackets", "FlowSeqNum", "InputSnmpIfindex", "OutputSnmpIfindex", "IpProtocolVersion", "NextHopAddress", "NextHopHostname", "Protocol", "SamplingAlgorithm", "SamplingInterval", "SrcAddress", "SrcHostname", "SrcAs", "SrcMaskLen", "SrcPort", "TcpFlags", "Tos", "NetflowVersion", "Vlan", "SrcNode", "ExporterNode", "DestNode", "Application", "Host", "Location", "SrcLocality", "DstLocality", "FlowLocality", "ClockCorrection", "Dscp", "Ecn", });
    com.google.protobuf.WrappersProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
