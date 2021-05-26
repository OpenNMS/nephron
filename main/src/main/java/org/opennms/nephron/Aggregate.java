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
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

import com.google.common.base.Strings;

@DefaultCoder(Aggregate.AggregateCoder.class)
public class Aggregate {
    private final long bytesIn;
    private final long bytesOut;

    // Some aggregations track host name information for a host addresses.
    //
    // Aggregations by conversations track the host name for source and destination addresses
    // Aggregations by host addresses track the host name for the corresponding address
    //
    // In case of conversations `hostname` has the value of the "smaller" address and `hostname2` the value of the
    // larger address

    private final String hostname;
    private final String hostname2;

    private final boolean congestionEncountered;
    private final boolean nonEcnCapableTransport;

    public Aggregate(long bytesIn, long bytesOut, String hostname, String hostname2, boolean ce, boolean nonEct) {
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
        this.hostname = hostname;
        this.hostname2 = hostname2;
        this.congestionEncountered = ce;
        this.nonEcnCapableTransport = nonEct;
    }

    public Aggregate(long bytesIn, long bytesOut, String hostname, String hostname2, Integer ecn) {
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
        this.hostname = hostname;
        this.hostname2 = hostname2;
        if (ecn != null) {
            this.congestionEncountered = ecn == 3;
            this.nonEcnCapableTransport = ecn == 0;
        } else {
            this.congestionEncountered = false;
            this.nonEcnCapableTransport = true;
        }
    }

    /**
     * Returns a copy of this aggregate with {@code hostname} set to the given value and {@code hostname2} being {@code null}.
     */
    public Aggregate withHostname(String hostname) {
        return new Aggregate(bytesIn, bytesOut, hostname, null, congestionEncountered, nonEcnCapableTransport);
    }

    public static Aggregate merge(final Aggregate a, final Aggregate b) {
        return new Aggregate(a.bytesIn + b.bytesIn, a.bytesOut + b.bytesOut,
                // make "hostname merging" deterministic by picking the lexicographic smaller one in case
                // that both aggregations have hostname being set
                Strings.isNullOrEmpty(a.hostname) ? b.hostname : Strings.isNullOrEmpty(b.hostname) || a.hostname.compareTo(b.hostname) < 0 ? a.hostname : b.hostname,
                Strings.isNullOrEmpty(a.hostname2) ? b.hostname2 : Strings.isNullOrEmpty(b.hostname2) || a.hostname2.compareTo(b.hostname2) < 0 ? a.hostname2 : b.hostname2,
                a.congestionEncountered || b.congestionEncountered,
                a.nonEcnCapableTransport || b.nonEcnCapableTransport
        );
    }

    public long getBytesIn() {
        return bytesIn;
    }

    public long getBytesOut() {
        return bytesOut;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getHostname2() {
        return this.hostname2;
    }

    public long getBytes() {
        return this.bytesIn + this.bytesOut;
    }

    public boolean isCongestionEncountered() {
        return congestionEncountered;
    }

    public boolean isNonEcnCapableTransport() {
        return nonEcnCapableTransport;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Aggregate)) return false;
        Aggregate flowBytes = (Aggregate) o;
        return bytesIn == flowBytes.bytesIn &&
               bytesOut == flowBytes.bytesOut &&
               congestionEncountered == flowBytes.congestionEncountered &&
               nonEcnCapableTransport == flowBytes.nonEcnCapableTransport &&
               Objects.equals(hostname, flowBytes.hostname) &&
               Objects.equals(hostname2, flowBytes.hostname2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesIn, bytesOut, hostname, hostname2, congestionEncountered, nonEcnCapableTransport);
    }

    @Override
    public String toString() {
        return "Aggregate{" +
                "bytesIn=" + bytesIn +
                ", bytesOut=" + bytesOut +
               ", hostname='" + hostname + '\'' +
               ", congestionEncountered=" + congestionEncountered +
               ", nonEcnCapableTransport=" + nonEcnCapableTransport +
               '}';
    }

    public static class AggregateCoder extends AtomicCoder<Aggregate> {
        private final Coder<Long> LONG_CODER = VarLongCoder.of();
        private final Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
        private final Coder<Boolean> BOOLEAN_CODER = BooleanCoder.of();

        @Override
        public void encode(Aggregate value, OutputStream outStream) throws IOException {
            LONG_CODER.encode(value.bytesIn, outStream);
            LONG_CODER.encode(value.bytesOut, outStream);
            STRING_CODER.encode(value.hostname, outStream);
            STRING_CODER.encode(value.hostname2, outStream);
            BOOLEAN_CODER.encode(value.congestionEncountered, outStream);
            BOOLEAN_CODER.encode(value.nonEcnCapableTransport, outStream);
        }

        @Override
        public Aggregate decode(InputStream inStream) throws IOException {
            return new Aggregate(
                    LONG_CODER.decode(inStream),
                    LONG_CODER.decode(inStream),
                    STRING_CODER.decode(inStream),
                    STRING_CODER.decode(inStream),
                    BOOLEAN_CODER.decode(inStream),
                    BOOLEAN_CODER.decode(inStream)
            );
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }
}
