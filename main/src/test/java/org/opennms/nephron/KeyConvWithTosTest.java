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

import java.util.stream.LongStream;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

public class KeyConvWithTosTest {

    @Property
    public boolean test(
            @ForAll("flowData") FlowData fd,
            @ForAll("windowSize") long windowSize,
            @ForAll("multiplier") double multiplier
    ) {
        // - determine the overlapping window numbers
        // - calculate the number of bytes that falls into each of these windows
        // - sum it up
        long sampled = LongStream
                .range(fd.deltaSwitched / windowSize, fd.lastSwitched / windowSize + 1)
                .map(i ->
                        Pipeline.KeyByConvWithTos.bytesInWindow(
                                fd.deltaSwitched,
                                fd.lastSwitched,
                                fd.bytes * multiplier,
                                i * windowSize,
                                (i + 1) * windowSize - 1
                        )
                ).sum();
        // System.out.println("flow: " + fd + "; windowSize: " + windowSize + "; sampled: " + sampled + "; multiplier: " + multiplier);
        // the sum of sampled values must be equal to the number of bytes in the flow (considering the multiplier)
        return sampled == (long)(fd.bytes * multiplier);
    }

    // an arbitrary long in the range [0, 1000]
    public static Arbitrary<Long> ARB_LONG = Arbitraries.longs().between(0, 1000);

    @Provide
    public Arbitrary<Double> multiplier() {
        // generate arbitrary multipliers with a strong bias for 1.0
        return Arbitraries.oneOf(Arbitraries.just(1.0), Arbitraries.doubles().between(0.1, 100));
    }

    @Provide
    public Arbitrary<Long> windowSize() {
        // window size must not be zero -> increment by one
        return ARB_LONG.map(l -> l + 1);
    }

    @Provide
    public Arbitrary<FlowData> flowData() {
        return ARB_LONG.flatMap(
                deltaSwitched -> ARB_LONG.flatMap(
                        lastSwitchedIncrement -> ARB_LONG.map(
                                bytes -> new FlowData(deltaSwitched, deltaSwitched + lastSwitchedIncrement, bytes)
                        )
                )
        );
    }

    public static class FlowData {
        public final long deltaSwitched, lastSwitched, bytes;

        public FlowData(long deltaSwitched, long lastSwitched, long bytes) {
            this.deltaSwitched = deltaSwitched;
            this.lastSwitched = lastSwitched;
            this.bytes = bytes;
        }

        @Override
        public String toString() {
            return "FlowData{" +
                   "deltaSwitched=" + deltaSwitched +
                   ", lastSwitched=" + lastSwitched +
                   ", bytes=" + bytes +
                   '}';
        }
    }
}
