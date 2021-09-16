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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.opennms.nephron.elastic.AggregationType;

/**
 * Captures all data that is necessary to populate a {@link org.opennms.nephron.elastic.FlowSummary} instance.
 *
 * This class was added for efficient transmission of flow summary information in pipelines.
 */
@DefaultCoder(FlowSummaryData.FlowSummaryDataCoder.class)
public class FlowSummaryData {
    public final CompoundKey key;
    public final Aggregate aggregate;

    public FlowSummaryData(CompoundKey key, Aggregate aggregate) {
        this.key = Objects.requireNonNull(key);
        this.aggregate = Objects.requireNonNull(aggregate);
    }

    @Override
    public String toString() {
        return "FlowSummaryData{" +
               "key=" + key +
               ", aggregate=" + aggregate +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlowSummaryData that = (FlowSummaryData) o;
        return key.equals(that.key) && aggregate.equals(that.aggregate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, aggregate);
    }

    public static class FlowSummaryDataCoder extends AtomicCoder<FlowSummaryData> {

        private static Coder<CompoundKey> KEY_CODER = new CompoundKey.CompoundKeyCoder();
        private static Coder<Aggregate> AGG_CODER = new Aggregate.AggregateCoder();

        @Override
        public void encode(FlowSummaryData value, OutputStream outStream) throws IOException {
            KEY_CODER.encode(value.key, outStream);
            AGG_CODER.encode(value.aggregate, outStream);
        }

        @Override
        public FlowSummaryData decode(InputStream inStream) throws IOException {
            return new FlowSummaryData(
                    KEY_CODER.decode(inStream),
                    AGG_CODER.decode(inStream)
            );
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }
    }
}
