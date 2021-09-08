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

package org.opennms.nephron.cortex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.joda.time.Instant;

/**
 * Assigns indexes to event timestamps in order satisfy Cortex's time ordering constraint for samples.
 * <p>
 * For each time series samples must be written ordered by time. This class assigns index numbers to event
 * timestamps that can be used as label values to differentiate different time series. Index numbers are guaranteed
 * to be increasing for each event timestamp and event timestamps are guaranteed to be increasing for each index number.
 */
public class EventTimestampIndexer {

    /**
     * An ordered list of event timestamps for which samples have been output. Newer
     * timestamps come before older timestamps. The list is updated by either setting
     * an existing timestamp to a newer value or by appending a new timestamp to the
     * end of the list.
     */
    private final List<Instant> flushedEventTimestamps;

    private EventTimestampIndexer(List<Instant> flushedEventTimestamps) {
        this.flushedEventTimestamps = flushedEventTimestamps;
    }


    public EventTimestampIndexer() {
        this(new ArrayList<>());
    }

    /**
     * Find an index for the given event timestamp that can be used as a label value for a sample of that event
     * timestamp.
     */
    public int findIndex(Instant eventTimestamp) {
        // search for an existing flushed event timestamp that is smaller than the given timestamp
        // -> if one is found it is increased to the given event timestamp and its index is returned
        // -> if none is found all flushed event time stamps are greater than or equal to the given event timestamp;
        //    in that case a new flushed event timestamp is added to the end of the list
        var idx = 0;
        while (idx < flushedEventTimestamps.size()) {
            if (flushedEventTimestamps.get(idx).compareTo(eventTimestamp) < 0) {
                flushedEventTimestamps.set(idx, eventTimestamp);
                return idx;
            }
            idx++;
        }
        flushedEventTimestamps.add(eventTimestamp);
        return idx;
    }

    public Instant newestEventTimestamp() {
        return flushedEventTimestamps.get(0);
    }

    public static class EventTimestampIndexerCoder extends AtomicCoder<EventTimestampIndexer> {

        public static final EventTimestampIndexerCoder of() {
            return new EventTimestampIndexerCoder();
        }

        private static final Coder<List<Instant>> INSTANTS_CODER = ListCoder.of(InstantCoder.of());

        @Override
        public void encode(EventTimestampIndexer value, OutputStream outStream) throws CoderException, IOException {
            INSTANTS_CODER.encode(value.flushedEventTimestamps, outStream);
        }

        @Override
        public EventTimestampIndexer decode(InputStream inStream) throws CoderException, IOException {
            return new EventTimestampIndexer(INSTANTS_CODER.decode(inStream));
        }

        @Override
        public boolean consistentWithEquals() {
            return INSTANTS_CODER.consistentWithEquals();
        }


    }

}
