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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

/**
 * FIXME: The intent of this aggregation if to behave like the Top.of(k), but also combine
 * all of the other elements that fall outside of the Top K.
 */
public class MyTop {

    /**
     * {@code CombineFn} for {@code Top} transforms that combines a bunch of {@code T}s into a single
     * {@code count}-long {@code List<T>}, using {@code compareFn} to choose the largest {@code T}s.
     *
     * @param <T> type of element being compared
     */
    public static class TopCombineFn<T, ComparatorT extends Comparator<T> & Serializable>
            extends Combine.AccumulatingCombineFn<T, BoundedHeap<T, ComparatorT>, List<T>>
            implements NameUtils.NameOverride {

        private final int count;
        private final ComparatorT compareFn;
        private final BiFunction<T,T,T> combineFn;

        public TopCombineFn(int count, ComparatorT compareFn, BiFunction<T,T,T> combineFn) {
            checkArgument(count >= 0, "count must be >= 0 (not %s)", count);
            this.count = count;
            this.compareFn = compareFn;
            this.combineFn = combineFn;
        }

        @Override
        public String getNameOverride() {
            return String.format("Top(%s)", NameUtils.approximateSimpleName(compareFn));
        }

        @Override
        public BoundedHeap<T, ComparatorT> createAccumulator() {
            return new BoundedHeap<>(count, compareFn, combineFn, new ArrayList<>());
        }

        @Override
        public Coder<BoundedHeap<T, ComparatorT>> getAccumulatorCoder(
                CoderRegistry registry, Coder<T> inputCoder) {
            return new BoundedHeapCoder<>(count, compareFn, combineFn, inputCoder);
        }

        @Override
        public void populateDisplayData(DisplayData.Builder builder) {
            super.populateDisplayData(builder);
            builder
                    .add(DisplayData.item("count", count).withLabel("Top Count"))
                    .add(DisplayData.item("comparer", compareFn.getClass()).withLabel("Record Comparer"))
                    .add(DisplayData.item("combineFn", combineFn.getClass()).withLabel("Record Combiner"));

        }

        @Override
        public String getIncompatibleGlobalWindowErrorMessage() {
            return "Default values are not supported in Top.[of, smallest, largest]() if the input "
                    + "PCollection is not windowed by GlobalWindows. Instead, use "
                    + "Top.[of, smallest, largest]().withoutDefaults() to output an empty PCollection if the "
                    + "input PCollection is empty, or Top.[of, smallest, largest]().asSingletonView() to "
                    + "get a PCollection containing the empty list if the input PCollection is empty.";
        }
    }

    /**
     * A heap that stores only a finite number of top elements according to its provided {@code
     * Comparator}. Implemented as an {@link Combine.AccumulatingCombineFn.Accumulator} to facilitate implementation of {@link Top}.
     *
     * <p>This class is <i>not</i> safe for multithreaded use, except read-only.
     */
    static class BoundedHeap<T, ComparatorT extends Comparator<T> & Serializable>
            implements Combine.AccumulatingCombineFn.Accumulator<T, BoundedHeap<T, ComparatorT>, List<T>> {

        /**
         * A queue with smallest at the head, for quick adds.
         *
         * <p>Only one of asList and asQueue may be non-null.
         */
        @Nullable
        private PriorityQueue<T> asQueue;

        /**
         * A list in with largest first, the form of extractOutput().
         *
         * <p>Only one of asList and asQueue may be non-null.
         */
        @Nullable private List<T> asList;

        private T other;

        /** The user-provided Comparator. */
        private final ComparatorT compareFn;

        /** The user-provided Combiner. */
        private final BiFunction<T,T,T> combineFn;

        /** The maximum size of the heap. */
        private final int maximumSize;

        /** Creates a new heap with the provided size, comparator, and initial elements. */
        private BoundedHeap(int maximumSize, ComparatorT compareFn, BiFunction<T,T,T> combineFn, List<T> asList) {
            this.maximumSize = maximumSize;
            this.asList = asList;
            this.compareFn = compareFn;
            this.combineFn = combineFn;
        }

        @Override
        public void addInput(T value) {
            if(!maybeAddInput(value)) {
                accumulateInOther(value);
            }
        }

        private void accumulateInOther(T value) {
            other = combineFn.apply(other, value);
        }

        /**
         * Adds {@code value} to this heap if it is larger than any of the current elements. Returns
         * {@code true} if {@code value} was added.
         */
        private boolean maybeAddInput(T value) {
            if (maximumSize == 0) {
                // Don't add anything.
                return false;
            }

            // If asQueue == null, then this is the first add after the latest call to the
            // constructor or asList().
            if (asQueue == null) {
                asQueue = new PriorityQueue<>(maximumSize, compareFn);
                for (T item : asList) {
                    asQueue.add(item);
                }
                asList = null;
            }

            if (asQueue.size() < maximumSize) {
                asQueue.add(value);
                return true;
            } else if (compareFn.compare(value, asQueue.peek()) > 0) {
                asQueue.poll();
                asQueue.add(value);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void mergeAccumulator(BoundedHeap<T, ComparatorT> accumulator) {
            for (T value : accumulator.asList()) {
                if (!maybeAddInput(value)) {
                    accumulateInOther(value);
                }
            }
        }

        @Override
        public List<T> extractOutput() {
            List<T> list = new ArrayList<>(asList());
            if (other != null) {
                list.add(other);
            }
            return list;
        }

        /** Returns the contents of this Heap as a List sorted largest-to-smallest. */
        private List<T> asList() {
            if (asList == null) {
                List<T> smallestFirstList = Lists.newArrayListWithCapacity(asQueue.size());
                while (!asQueue.isEmpty()) {
                    smallestFirstList.add(asQueue.poll());
                }
                asList = Lists.reverse(smallestFirstList);
                asQueue = null;
            }
            return asList;
        }
    }

    /** A {@link Coder} for {@link BoundedHeap}, using Java serialization via {@link CustomCoder}. */
    private static class BoundedHeapCoder<T, ComparatorT extends Comparator<T> & Serializable>
            extends CustomCoder<BoundedHeap<T, ComparatorT>> {
        private final Coder<List<T>> listCoder;
        private final ComparatorT compareFn;
        private final BiFunction<T,T,T> combineFn;
        private final int maximumSize;

        public BoundedHeapCoder(int maximumSize, ComparatorT compareFn, BiFunction<T,T,T> combineFn, Coder<T> elementCoder) {
            listCoder = ListCoder.of(elementCoder);
            this.compareFn = compareFn;
            this.combineFn = combineFn;
            this.maximumSize = maximumSize;
        }

        @Override
        public void encode(BoundedHeap<T, ComparatorT> value, OutputStream outStream)
                throws CoderException, IOException {
            listCoder.encode(value.asList(), outStream);
        }

        @Override
        public BoundedHeap<T, ComparatorT> decode(InputStream inStream)
                throws CoderException, IOException {
            return new BoundedHeap<>(maximumSize, compareFn, combineFn, listCoder.decode(inStream));
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            verifyDeterministic(this, "HeapCoder requires a deterministic list coder", listCoder);
        }

        @Override
        public boolean isRegisterByteSizeObserverCheap(BoundedHeap<T, ComparatorT> value) {
            return listCoder.isRegisterByteSizeObserverCheap(value.asList());
        }

        @Override
        public void registerByteSizeObserver(
                BoundedHeap<T, ComparatorT> value, ElementByteSizeObserver observer) throws Exception {
            listCoder.registerByteSizeObserver(value.asList(), observer);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof BoundedHeapCoder)) {
                return false;
            }
            BoundedHeapCoder<?, ?> that = (BoundedHeapCoder<?, ?>) other;
            return Objects.equals(this.compareFn, that.compareFn)
                    && Objects.equals(this.combineFn, that.combineFn)
                    && Objects.equals(this.listCoder, that.listCoder)
                    && this.maximumSize == that.maximumSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(compareFn, combineFn, listCoder, maximumSize);
        }
    }
}
