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
    public final AggregationType aggregationType;
    public final Groupings.CompoundKey key;
    public final Aggregate aggregate;
    public final long windowStart, windowEnd;
    public final int ranking;

    public FlowSummaryData(AggregationType aggregationType, Groupings.CompoundKey key, Aggregate aggregate, long windowStart, long windowEnd, int ranking) {
        this.aggregationType = aggregationType;
        this.key = key;
        this.aggregate = aggregate;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.ranking = ranking;
    }

    @Override
    public String toString() {
        return "FlowSummaryData{" +
               "aggregationType=" + aggregationType +
               ", key=" + key +
               ", aggregate=" + aggregate +
               ", windowStart=" + windowStart +
               ", windowEnd=" + windowEnd +
               ", ranking=" + ranking +
               '}';
    }

    public static class FlowSummaryDataCoder extends AtomicCoder<FlowSummaryData> {

        private static Coder<Integer> INT_CODER = VarIntCoder.of();
        private static Coder<Long> LONG_CODER = VarLongCoder.of();
        private static Coder<Groupings.CompoundKey> KEY_CODER = new Groupings.CompoundKeyCoder();
        private static Coder<Aggregate> AGG_CODER = new Aggregate.AggregateCoder();

        @Override
        public void encode(FlowSummaryData value, OutputStream outStream) throws IOException {
            INT_CODER.encode(value.aggregationType.ordinal(), outStream);
            KEY_CODER.encode(value.key, outStream);
            AGG_CODER.encode(value.aggregate, outStream);
            LONG_CODER.encode(value.windowStart, outStream);
            LONG_CODER.encode(value.windowEnd, outStream);
            INT_CODER.encode(value.ranking, outStream);
        }

        @Override
        public FlowSummaryData decode(InputStream inStream) throws IOException {
            return new FlowSummaryData(
                    AggregationType.values()[INT_CODER.decode(inStream)],
                    KEY_CODER.decode(inStream),
                    AGG_CODER.decode(inStream),
                    LONG_CODER.decode(inStream),
                    LONG_CODER.decode(inStream),
                    INT_CODER.decode(inStream)
            );
        }

    }
}
