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

import java.util.Iterator;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.codepoetics.protonpack.StreamUtils;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;

/**
 * Provides static methods for generating flows.
 */
public class FlowDocuments {


    // uniform distribution [0.001, 1]
    // -> use 0.001 in order to avoid very large logarithms when deriving normal and exponential distributions
    private static Arbitrary<Double> UNIFORM = Arbitraries.doubles().between(0.001, true, 1, true).ofScale(3);

    // normal distribution (using Box-Muller transform)
    private static Arbitrary<Double> NORMAL = Combinators
            .combine(UNIFORM, UNIFORM)
            .as((d1, d2) -> Math.sqrt(-2*Math.log(d1)) * Math.cos(2 * Math.PI * d2));

    // exponential distribution
    private static Arbitrary<Double> EXPONENTIAL = UNIFORM.map(d -> -Math.log(d));

    private static Arbitrary<Long> BYTES = Arbitraries.longs().between(0, 1024 * 1024 * 1024);

    /**
     * Creates a length limited stream of flows.
     *
     * The length is calculated by {@code maxIdx/idxInc} of the given {@link SourceConfig}.
     */
    public static Stream<FlowDocument> stream(
            SourceConfig sourceConfig
    ) {
        return stream(sourceConfig.flowConfig, sourceConfig.seed, sourceConfig.maxIdx, sourceConfig.idxInc, sourceConfig.idxOffset);
    }

    /**
     * Creates a length limited stream of flows.
     *
     * The {@code minSplits} and {@code maxSplits} properties of the given {@code SourceConfig} must be equal.
     *
     * The stream is implemented by first splitting the SourceConfig according to the minimum/maximum number of splits
     * and then returning flows from each split in a round-robing fashion.
     *
     * This tries to mimic how a source is processed by Flink. Flink also tries to split its input. However, Flink may
     * process splits in parallel and the order of processed elements may not be the same. Yet, the collection of all
     * returned flows should be the same.
     */
    public static Stream<FlowDocument> splittedStream(
            SourceConfig sourceConfig
    ) {
        if (sourceConfig.minSplits != sourceConfig.maxSplits) {
            throw new RuntimeException("minimum and maximum number of splits must be equals - minSplits: " + sourceConfig.minSplits + "; maxSplits: " + sourceConfig.maxSplits);
        }
        var iters = sourceConfig.split(sourceConfig.minSplits).stream().map(FlowDocuments::stream).map(s -> s.iterator()).toArray(l -> new Iterator[l]);
        var iter = new Iterator<FlowDocument>() {
            int idx = 0;
            @Override
            public boolean hasNext() {
                var cnt = iters.length;
                while (cnt-- > 0) {
                    if (iters[idx].hasNext()) return true;
                    idx = (idx + 1) % iters.length;
                }
                return false;
            }

            @Override
            public FlowDocument next() {
                return (FlowDocument)iters[idx].next();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
    }

    public static Stream<FlowDocument> stream(
            FlowConfig cfg,
            long seed,
            long maxIdx,
            int idxInc,
            int idxOffset
    ) {

        Arbitrary<FlowData> flowData = getFlowDataArbitrary(cfg);

        return
                StreamUtils.zipWithIndex(flowData.generator(1000).stream(new Random(seed)))
                        .limit(maxIdx / idxInc)
                        .map(zipped -> {
                            long idx = zipped.getIndex() * idxInc + idxOffset;
                            FlowData fd = zipped.getValue().value();
                            FlowDocument f = getFlowDocument(cfg, idx, fd);
                            return f;
                        });
    }

    public static FlowDocument getFlowDocument(FlowConfig cfg, long idx, FlowData fd) {
        Instant lastSwitchedWithoutClockSkew = cfg.lastSwitched.apply(idx).plus(fd.fd2.lastSwitchedOffset);
        Duration clockSkew = cfg.clockSkew.apply(fd.fd1.nodeId);
        Instant lastSwitched = lastSwitchedWithoutClockSkew.plus(clockSkew);
        Instant deltaSwitched = lastSwitched.minus(fd.fd2.flowDuration);

        String srcAddress = ipAddress(fd.fd1.srcAddr);
        String dstAddress = ipAddress(fd.fd1.dstAddr);

        String minAddr, maxAddr;
        if (srcAddress.compareTo(dstAddress) < 0) {
            minAddr = srcAddress;
            maxAddr = dstAddress;
        } else {
            maxAddr = srcAddress;
            minAddr = dstAddress;
        }

        String application = "app" + fd.fd1.application;

        String convoKey = new StringBuilder()
                .append('[')
                .append("\"Default\"")
                .append(',')
                .append(fd.fd1.protocol)
                .append(',')
                .append('"')
                .append(minAddr)
                .append('"')
                .append(',')
                .append('"')
                .append(maxAddr)
                .append('"')
                .append(",")
                .append('"')
                .append(application)
                .append('"')
                .append(']')
                .toString();

        FlowDocument f = FlowDocument
                .newBuilder()
                .setExporterNode(
                        NodeInfo.newBuilder()
                                .setNodeId(fd.fd1.nodeId)
                                .setForeginId("fId" + fd.fd1.nodeId)
                                .setForeignSource("fSource" + fd.fd1.nodeId)
                                .build()
                )
                .setLocation("Default")
                .setProtocol(UInt32Value.of(fd.fd1.protocol))
                .setInputSnmpIfindex(UInt32Value.of(fd.fd1.inputInterfaceIdx))
                .setOutputSnmpIfindex(UInt32Value.of(fd.fd1.outputInterfaceIdx))
                .setApplication(application)
                .setSrcAddress(srcAddress)
                .setSrcHostname("host" + fd.fd1.srcAddr)
                .setDstAddress(dstAddress)
                .setDstHostname("host" + fd.fd1.dstAddr)
                .setConvoKey(convoKey)
                .setDirection(fd.fd2.ingressNotEgress ? Direction.INGRESS : Direction.EGRESS)
                .setLastSwitched(UInt64Value.of(lastSwitched.getMillis()))
                .setDeltaSwitched(UInt64Value.of(deltaSwitched.getMillis()))
                .setNumBytes(UInt64Value.of(fd.fd2.bytes))
                .setTos(UInt32Value.of(fd.fd2.ecn + 4 * fd.fd2.dscp))
                .setEcn(UInt32Value.of(fd.fd2.ecn))
                .setDscp(UInt32Value.of(fd.fd2.dscp))
                .build();
        return f;
    }

    public static String ipAddress(int ip) {
        return String.format("%d.%d.%d.%d", (ip >> 24 & 0xff), (ip >> 16 & 0xff), (ip >> 8 & 0xff), (ip & 0xff));
    }

    public static Arbitrary<FlowData> getFlowDataArbitrary(FlowConfig cfg) {
        // the combine method does support up to 8 arguments
        // -> split the necessary information for generating flows into two pieces
        Arbitrary<FlowData1> flowData1 = Combinators.combine(
                Arbitraries.integers().between(cfg.minExporter, cfg.minExporter + cfg.numExporters - 1),
                Arbitraries.integers().between(cfg.minInterface, cfg.minInterface + cfg.numInterfaces - 1),
                Arbitraries.integers().between(cfg.minInterface, cfg.minInterface + cfg.numInterfaces - 1),
                Arbitraries.integers().between(0, cfg.numProtocols - 1),
                Arbitraries.integers().between(0, cfg.numHosts - 1),
                Arbitraries.integers().between(0, cfg.numHosts - 1),
                Arbitraries.integers().between(0, cfg.numApplications - 1)
        ).as(FlowData1::new);

        Arbitrary<FlowData2> flowData2 = Combinators.combine(
                Arbitraries.of(true, false),
                NORMAL.map(d -> Duration.millis((long)(d * cfg.lastSwitchedSigma.getMillis()))),
                EXPONENTIAL.map(d -> Duration.millis((long)(d * 1000 / cfg.flowDurationLambda))),
                BYTES,
                Arbitraries.integers().between(0, cfg.numEcns - 1),
                Arbitraries.integers().between(0, cfg.numDscps - 1)
        ).as(FlowData2::new);

        Arbitrary<FlowData> flowData = Combinators.combine(
                flowData1,
                flowData2
        ).as(FlowData::new);
        return flowData;
    }

    public static class FlowData1 {
        public final int nodeId;
        public final int inputInterfaceIdx;
        public final int outputInterfaceIdx;
        public final int protocol;
        public final int srcAddr;
        public final int dstAddr;
        public final int application;

        public FlowData1(int nodeId, int inputInterfaceIdx, int outputInterfaceIdx, int protocol, int srcAddr, int dstAddr, int application) {
            this.nodeId = nodeId;
            this.inputInterfaceIdx = inputInterfaceIdx;
            this.outputInterfaceIdx = outputInterfaceIdx;
            this.protocol = protocol;
            this.srcAddr = srcAddr;
            this.dstAddr = dstAddr;
            this.application = application;
        }

    }

    public static class FlowData2 {
        public final boolean ingressNotEgress;
        public final Duration lastSwitchedOffset;
        public final Duration flowDuration;
        public final long bytes;
        public final int ecn;
        public final int dscp;

        public FlowData2(boolean ingressNotEgress, Duration lastSwitchedOffset, Duration flowDuration, long bytes, int ecn, int dscp) {
            this.ingressNotEgress = ingressNotEgress;
            this.lastSwitchedOffset = lastSwitchedOffset;
            this.flowDuration = flowDuration;
            this.bytes = bytes;
            this.ecn = ecn;
            this.dscp = dscp;
        }
    }

    public static class FlowData {

        public final FlowData1 fd1;
        public final FlowData2 fd2;

        public FlowData(FlowData1 fd1, FlowData2 fd2) {
            this.fd1 = fd1;
            this.fd2 = fd2;
        }
    }
}
