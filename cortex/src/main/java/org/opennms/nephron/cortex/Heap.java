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
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Stores data that is meant to be output to Cortex.
 * <p>
 * Cortex requires that data is written ordered by time. Beam may output several panes of data for each window.
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
     * Gets the oldest processing timestamp of all values on the heap.
     * <p>
     * The returned timestamp can be used to schedule a timer for checking for flushable values.
     */
    abstract Optional<Instant> oldestProcessingTimestamp();

    /**
     * Gets the newest event timestamp of all values on the heap.
     * <p>
     * The returned timestamp can be used to schedule a timer for garbage collecting the heap.
     */
    abstract Optional<Instant> newestEventTimestamp();

    /**
     * Flushes all values from the heap that are older then the specified duration.
     */
    abstract List<Flushed<V>> flush(Instant now, Duration outputDelay);

    //
    // implementation follows
    //

    static class HeapImpl<V> extends Heap<V> {

        /**
         * Bundles a potentially accumulated value and the processing timestamp when it was created.
         * <p>
         * Flushing logic is based on the created timestamp. Alternatively, a "last updated" timestamp could have
         * been tracked. However, relying on the created timestamp matches Beam's behavior for early and late
         * processing where the first value for an early / late pane determines when the pane gets flushed.
         */
        private static class HeapValue<V> {

            private static Coder<Instant> INSTANT_CODER = InstantCoder.of();

            private V value;
            // processing time when the heap value got created
            private Instant created;

            public HeapValue(V value, Instant created) {
                this.value = value;
                this.created = created;
            }

            public Instant getCreated() {
                return created;
            }

            private static class HeapValueCoder<V> extends AtomicCoder<HeapValue<V>> {
                private final Coder<V> valueCoder;

                public HeapValueCoder(Coder<V> valueCoder) {
                    this.valueCoder = valueCoder;
                }

                @Override
                public void encode(HeapValue<V> value, OutputStream outStream) throws CoderException, IOException {
                    valueCoder.encode(value.value, outStream);
                    INSTANT_CODER.encode(value.created, outStream);
                }

                @Override
                public HeapValue<V> decode(InputStream inStream) throws CoderException, IOException {
                    return new HeapValue<>(
                            valueCoder.decode(inStream),
                            INSTANT_CODER.decode(inStream)
                    );
                }
            }
        }

        static class HeapImplCoder<V> extends StructuredCoder<HeapImpl<V>> {

            private final Coder<V> valueCoder;
            private final EventTimestampIndexer.EventTimestampIndexerCoder eventTimestampIndexerCoder;
            private final MapCoder<Instant, HeapValue<V>> mapCoder;

            public HeapImplCoder(Coder<V> valueCoder) {
                this.valueCoder = valueCoder;
                eventTimestampIndexerCoder = EventTimestampIndexer.EventTimestampIndexerCoder.of();
                mapCoder = MapCoder.of(InstantCoder.of(), new HeapValue.HeapValueCoder(valueCoder));
            }

            @Override
            public void encode(HeapImpl<V> value, OutputStream outStream) throws CoderException, IOException {
                eventTimestampIndexerCoder.encode(value.eventTimestampIndexer, outStream);
                mapCoder.encode(value.values, outStream);
            }

            @Override
            public HeapImpl<V> decode(InputStream inStream) throws CoderException, IOException {
                return new HeapImpl<>(
                        eventTimestampIndexerCoder.decode(inStream),
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
                return eventTimestampIndexerCoder.consistentWithEquals() && mapCoder.consistentWithEquals();
            }

            // TODO: check other coder methods: structuralValue, registerByteSizeObserver, getEncodedElementByteSize, ...

            @Override
            public TypeDescriptor<HeapImpl<V>> getEncodedTypeDescriptor() {
                return new TypeDescriptor<HeapImpl<V>>() {}.where(
                        new TypeParameter<V>() {}, valueCoder.getEncodedTypeDescriptor());
            }

        }

        // latest event timestamps used in flushes
        private final EventTimestampIndexer eventTimestampIndexer;

        // eventTimestamp -> (value, created)
        private final Map<Instant, HeapValue<V>> values;

        HeapImpl(EventTimestampIndexer eventTimestampIndexer, Map<Instant, HeapValue<V>> values) {
            this.eventTimestampIndexer = eventTimestampIndexer;
            this.values = values;
        }

        /**
         * Determine the minimum index that can be used for the given timestamp.
         */
        private int findIndex(Instant eventTimestamp) {
            return eventTimestampIndexer.findIndex(eventTimestamp);
        }

        @Override
        public void add(V value, Instant eventTimeTimestamp, Instant now, BiFunction<V, V, V> combiner) {
            var heapValue = values.get(eventTimeTimestamp);
            if (heapValue != null) {
                heapValue.value = combiner.apply(heapValue.value, value);
            } else {
                heapValue = new HeapValue<>(value, now);
                values.put(eventTimeTimestamp, heapValue);
            }
        }

        @Override
        boolean isEmpty() {
            return values.isEmpty();
        }

        @Override
        Optional<Instant> oldestProcessingTimestamp() {
            return values
                    .values()
                    .stream()
                    .map(HeapValue::getCreated)
                    .min(Instant::compareTo);
        }

        @Override
        Optional<Instant> newestEventTimestamp() {
            return values.keySet().stream().max(Instant::compareTo);
        }

        @Override
        public List<Flushed<V>> flush(Instant now, Duration outputDelay) {
            var threshold = now.minus(outputDelay);
            List<Flushed<V>> expired = new ArrayList<>();
            for (var i = values.entrySet().iterator(); i.hasNext(); ) {
                var entry = i.next();
                if (entry.getValue().created.compareTo(threshold) <= 0) {
                    expired.add(new Flushed<>(entry.getValue().value, entry.getKey(), findIndex(entry.getKey())));
                    i.remove();
                }
            }
            if (expired.size() == 1) {
                return Collections.singletonList(expired.get(0));
            } else {
                return expired.stream()
                        .sorted(Comparator.comparing(e -> e.eventTimestamp))
                        .collect(Collectors.toList());
            }
        }

    }

}
