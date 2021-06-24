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

package org.opennms.nephron.generator;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.FlowReport;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.opennms.netmgt.flows.persistence.model.Locality;
import org.opennms.netmgt.flows.persistence.model.NodeInfo;

import com.google.common.net.InetAddresses;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;

public class FlowDocGen {

    private final Random random;

    private final List<Integer> protocols;
    private final List<String> applications;
    private final List<String> hosts;
    private final List<AddrHost> addresses;

    public FlowDocGen(Random random) {
        this.random = random;
        this.protocols = Arrays.asList(6, 17);
        this.applications = generate(200, generateString(15));
        this.hosts = generate(5, generateString(10));
        this.addresses = generate(100, () -> new AddrHost(generateInetAddr().get(), generateString(10).get()));
    }

    public FlowDocument createFlowDocument(final Exporter exporter, final FlowReport report) {
        final int protocol = choose(this.protocols);
        final String application = choose(this.applications);

        final AddrHost srcAddr = choose(this.addresses);
        final AddrHost dstAddr = choose(this.addresses);

        final InetAddress[] convo = InetAddresses.coerceToInteger(srcAddr.address) < InetAddresses.coerceToInteger(dstAddr.address)
                                    ? new InetAddress[]{srcAddr.address, dstAddr.address}
                                    : new InetAddress[]{dstAddr.address, srcAddr.address};

        final String convoKey = "[\"" + exporter.getLocation() + "\",\"" + protocol + ",\"" + InetAddresses.toAddrString(convo[0]) + "\",\"" + InetAddresses.toAddrString(convo[1]) + "\",\"" + application + "\"]";

        final FlowDocument.Builder flowBuilder = FlowDocument.newBuilder();
        flowBuilder.setApplication(application);
        flowBuilder.setHost(choose(this.hosts));
        flowBuilder.setLocation(exporter.getLocation());
        flowBuilder.setDstLocality(Locality.PUBLIC);
        flowBuilder.setSrcLocality(Locality.PUBLIC);
        flowBuilder.setFlowLocality(Locality.PUBLIC);
        flowBuilder.setSrcAddress(InetAddresses.toAddrString(srcAddr.address));
        flowBuilder.setDstAddress(InetAddresses.toAddrString(dstAddr.address));
        flowBuilder.setSrcHostname(srcAddr.hostname);
        flowBuilder.setDstHostname(dstAddr.hostname);
        flowBuilder.setFirstSwitched(UInt64Value.of(report.getStart().plus(exporter.getClockOffset()).toEpochMilli()));
        flowBuilder.setDeltaSwitched(UInt64Value.of(report.getStart().plus(exporter.getClockOffset()).toEpochMilli()));
        flowBuilder.setLastSwitched(UInt64Value.of(report.getEnd().minusMillis(1).plus(exporter.getClockOffset()).toEpochMilli()));
        flowBuilder.setNumBytes(UInt64Value.of(report.getBytes()));
        flowBuilder.setConvoKey(convoKey);
        flowBuilder.setInputSnmpIfindex(UInt32Value.of(exporter.getInputSnmp()));
        flowBuilder.setOutputSnmpIfindex(UInt32Value.of(exporter.getOutputSnmp()));

        final NodeInfo.Builder exporterBuilder = NodeInfo.newBuilder();
        exporterBuilder.setNodeId(exporter.getNodeId());
        exporterBuilder.setForeignSource(exporter.getForeignSource());
        exporterBuilder.setForeginId(exporter.getForeignId());
        flowBuilder.setExporterNode(exporterBuilder);

        return flowBuilder.build();
    }

    private <T> T choose(final List<T> options) {
        return options.get(random.nextInt(options.size()));
    }

    private Supplier<String> generateString(final int length) {
        return () -> random.ints(97, 123)
                .limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    private <T> List<T> generate(final int count, final Supplier<T> f) {
        return IntStream.range(0, count)
                .mapToObj(i -> f.get())
                .collect(Collectors.toList());
    }

    private Supplier<Inet4Address> generateInetAddr() {
        return () -> InetAddresses.fromInteger(random.nextInt());
    }


    private static class AddrHost {
        public final InetAddress address;
        public final String hostname;

        private AddrHost(final InetAddress address, final String hostname) {
            this.address = Objects.requireNonNull(address);
            this.hostname = Objects.requireNonNull(hostname);
        }
    }

}
