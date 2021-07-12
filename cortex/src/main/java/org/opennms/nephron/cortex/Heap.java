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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Stores data that is meant to be output to Cortex.
 * <p>
 * Cortex requires that written data is ordered by time. Beam may output several panes of data for each window.
 * In addition, panes of later windows may arrive earlier than some panes of earlier windows.
 * <p>
 * This class accumulates data of different panes / windows and generates output that meets Cortex's ordering
 * constraint by assigning indexes. These indexes are used as labels to distinguish time series.
 */
public abstract class Heap<V> {

    /**
     * Represents a value that was flushed from the heap. It contains an index that must be used to distinguish
     * samples at the same timestamp.
     */
    public static class Flushed<V> {

        public final V value;
        public final Instant eventTimestamp;
        public final int index;

        public Flushed(V value, Instant eventTimestamp, int index) {
            this.value = value;
            this.eventTimestamp = eventTimestamp;
            this.index = index;
        }

    }

    /**
     * Adds a value to the heap for the specified event timestamp.
     */
    abstract void add(V value, Instant eventTimeTimestamp, Instant now, BiFunction<V, V, V> combiner);

    abstract boolean isEmpty();

    /**
     * Gets the processing timestamp of the oldest unchanged value in the heap.
     * <p>
     * The returned timestamp can be used to schedule a timer for checking for flushable values.
     */
    abstract Optional<Instant> oldestValueProcessingTimestamp();

    /**
     * Flushes all values that did not change during the specified duration.
     */
    abstract List<Flushed<V>> flush(Duration unchangedSince, Instant now);

    //
    //
    //

    static class HeapImpl<V> extends Heap<V> {

        private static class ValueAndLastUpdated<V> {

            private static Coder<Instant> INSTANT_CODER = InstantCoder.of();

            private V value;
            private Instant lastUpdated;

            public ValueAndLastUpdated(V value, Instant lastUpdated) {
                this.value = value;
                this.lastUpdated = lastUpdated;
            }

            public Instant getLastUpdated() {
                return lastUpdated;
            }

            private static class ValueAndLastUpdatedCoder<V> extends AtomicCoder<ValueAndLastUpdated<V>> {
                private final Coder<V> valueCoder;

                public ValueAndLastUpdatedCoder(Coder<V> valueCoder) {
                    this.valueCoder = valueCoder;
                }

                @Override
                public void encode(ValueAndLastUpdated<V> value, OutputStream outStream) throws CoderException, IOException {
                    valueCoder.encode(value.value, outStream);
                    INSTANT_CODER.encode(value.lastUpdated, outStream);
                }

                @Override
                public ValueAndLastUpdated<V> decode(InputStream inStream) throws CoderException, IOException {
                    return new ValueAndLastUpdated<>(
                            valueCoder.decode(inStream),
                            INSTANT_CODER.decode(inStream)
                    );
                }
            }
        }

        static class HeapImplCoder<V> extends StructuredCoder<HeapImpl<V>> {

            private final Coder<V> valueCoder;
            private final ListCoder<Instant> listCoder;
            private final MapCoder<Instant, ValueAndLastUpdated<V>> mapCoder;

            public HeapImplCoder(Coder<V> valueCoder) {
                this.valueCoder = valueCoder;
                listCoder = ListCoder.of(InstantCoder.of());
                mapCoder = MapCoder.of(InstantCoder.of(), new ValueAndLastUpdated.ValueAndLastUpdatedCoder(valueCoder));
            }

            @Override
            public void encode(HeapImpl<V> value, OutputStream outStream) throws CoderException, IOException {
                listCoder.encode(value.flushedEventTimestamps, outStream);
                mapCoder.encode(value.values, outStream);
            }

            @Override
            public HeapImpl<V> decode(InputStream inStream) throws CoderException, IOException {
                return new HeapImpl<>(
                        listCoder.decode(inStream),
                        mapCoder.decode(inStream)
                );
            }

            @Override
            public List<? extends Coder<?>> getCoderArguments() {
                return Collections.singletonList(valueCoder);
            }

            @Override
            public void verifyDeterministic() throws NonDeterministicException {
                throw new NonDeterministicException(
                        this, "Ordering of entries in the values Map may be non-deterministic.");
            }

            @Override
            public boolean consistentWithEquals() {
                return listCoder.consistentWithEquals() && mapCoder.consistentWithEquals();
            }

            // TODO: check other coder methods: structuralValue, registerByteSizeObserver, getEncodedElementByteSize, ...

            @Override
            public TypeDescriptor<HeapImpl<V>> getEncodedTypeDescriptor() {
                return new TypeDescriptor<HeapImpl<V>>() {}.where(
                        new TypeParameter<V>() {}, valueCoder.getEncodedTypeDescriptor());
            }

        }

        // latest event timestamps used in flushes
        // -> event timestamps are ordered: the newest event timestamp comes first
        private final List<Instant> flushedEventTimestamps;

        // eventTimestamp -> (value, lastUpdated)
        private final Map<Instant, ValueAndLastUpdated<V>> values;

        HeapImpl(List<Instant> flushedEventTimestamps, Map<Instant, ValueAndLastUpdated<V>> values) {
            this.flushedEventTimestamps = flushedEventTimestamps;
            this.values = values;
        }

        /**
         * Determine the minimum index that can be used for the given timestamp.
         */
        private int findIndex(Instant eventTimestamp) {
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

        @Override
        public void add(V value, Instant eventTimeTimestamp, Instant now, BiFunction<V, V, V> combiner) {
            var valu = values.get(eventTimeTimestamp);
            if (valu != null) {
                valu.value = combiner.apply(valu.value, value);
                valu.lastUpdated = now;
            } else {
                valu = new ValueAndLastUpdated<>(value, now);
                values.put(eventTimeTimestamp, valu);
            }
        }

        @Override
        boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        Optional<Instant> oldestValueProcessingTimestamp() {
            return values
                    .values()
                    .stream()
                    .map(ValueAndLastUpdated::getLastUpdated)
                    .reduce((i1, i2) -> i1.compareTo(i2) < 0 ? i1 : i2);
        }

        @Override
        public List<Flushed<V>> flush(Duration outputDelay, Instant now) {
            var threshold = now.minus(outputDelay);
            List<Map.Entry<Instant, ValueAndLastUpdated<V>>> expired = new ArrayList<>();
            for (var i = values.entrySet().iterator(); i.hasNext(); ) {
                var entry = i.next();
                if (entry.getValue().lastUpdated.compareTo(threshold) <= 0) {
                    expired.add(entry);
                    i.remove();
                }
            }
            return expired.stream()
                    .sorted(Comparator.comparing(e -> e.getValue().lastUpdated))
                    .map(entry -> new Flushed<>(entry.getValue().value, entry.getKey(), findIndex(entry.getKey())))
                    .collect(Collectors.toList());
        }
    }
}
