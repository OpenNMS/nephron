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

import static org.opennms.nephron.Pipeline.RATE_LIMITED_LOG;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;

import com.google.common.base.Strings;

@DefaultCoder(Aggregate.AggregateCoder.class)
public class Aggregate {
    private long bytesIn;
    private long bytesOut;

    private String hostname;

    public Aggregate(long bytesIn, long bytesOut,
                     final String hostname) {
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
        this.hostname = hostname;
    }

    public static Aggregate merge(final Aggregate a, final Aggregate b) {
        return new Aggregate(a.bytesIn + b.bytesIn, a.bytesOut + b.bytesOut,
                a.hostname != null ? a.hostname : b.hostname);
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

    public long getBytes() {
        return this.bytesIn + this.bytesOut;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Aggregate)) return false;
        Aggregate flowBytes = (Aggregate) o;
        return bytesIn == flowBytes.bytesIn &&
               bytesOut == flowBytes.bytesOut &&
               Objects.equals(hostname, flowBytes.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesIn, bytesOut);
    }

    @Override
    public String toString() {
        return "Aggregate{" +
               "bytesIn=" + bytesIn +
               ", bytesOut=" + bytesOut +
               ", hostname=" + hostname +
               '}';
    }

    public static class AggregateCoder extends AtomicCoder<Aggregate> {
        private final Coder<Long> LONG_CODER = VarLongCoder.of();
        private final Coder<String> STRING_CODER = StringUtf8Coder.of();

        @Override
        public void encode(Aggregate value, OutputStream outStream) throws IOException {
            // TODO: FIXME: Hack for NPEs
            // Caused by: java.lang.NullPointerException
            //	at org.opennms.nephron.FlowBytes$FlowBytesCoder.encode(FlowBytes.java:112)
            //	at org.opennms.nephron.FlowBytes$FlowBytesCoder.encode(FlowBytes.java:107)
            //	at org.apache.beam.sdk.coders.Coder.encode(Coder.java:136)
            //	at org.apache.beam.sdk.coders.KvCoder.encode(KvCoder.java:71)
            //	at org.apache.beam.sdk.coders.KvCoder.encode(KvCoder.java:36)
            //	at org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.encode(WindowedValue.java:588)
            //	at org.apache.beam.sdk.util.WindowedValue$FullWindowedValueCoder.encode(WindowedValue.java:539)
            if (value == null) {
                RATE_LIMITED_LOG.error("Got null FlowBytes value. Encoding as 0s.");
                LONG_CODER.encode(0L, outStream);
                LONG_CODER.encode(0L, outStream);
                STRING_CODER.encode("", outStream);
                return;
            }
            LONG_CODER.encode(value.bytesIn, outStream);
            LONG_CODER.encode(value.bytesOut, outStream);
            STRING_CODER.encode(Strings.nullToEmpty(value.hostname), outStream);
        }

        @Override
        public Aggregate decode(InputStream inStream) throws IOException {
            final long bytesIn = LONG_CODER.decode(inStream);
            final long bytesOut = LONG_CODER.decode(inStream);
            final String hostname = STRING_CODER.decode(inStream);
            return new Aggregate(bytesIn, bytesOut, Strings.emptyToNull(hostname));
        }
    }
}
