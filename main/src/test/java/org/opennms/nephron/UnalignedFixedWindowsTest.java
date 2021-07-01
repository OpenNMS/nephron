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

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

public class UnalignedFixedWindowsTest {

    @Provide
    public Arbitrary<Integer> nodeId() {
        return Arbitraries.integers().between(0, 10);
    }

    @Provide
    public Arbitrary<Integer> itfIdx() {
        return Arbitraries.integers().between(0, 2);
    }

    @Provide
    public Arbitrary<Long> windowSize() {
        return Arbitraries.longs().between(1, 10);
    }

    @Provide
    public Arbitrary<Long> timestamp() {
        return Arbitraries.longs().between(100, 1000);
    }

    @Property
    public boolean canCalculateStartOfShiftedWindowForFlowTimestamp(
            @ForAll("nodeId") int nodeId,
            @ForAll("itfIdx") int itfIdx,
            @ForAll("windowSize") long windowSize,
            @ForAll("timestamp") long timestamp
    ) {
        long start = UnalignedFixedWindows.windowStartForTimestamp(nodeId, itfIdx, windowSize, timestamp);
        long shift = UnalignedFixedWindows.perNodeShift(nodeId, itfIdx, windowSize);
        // check that
        // - the start of the unshifted window (i.e. start - shift) is a multiple of the window size
        // - the timestamp is included in the shifted window
        return (start - shift) % windowSize == 0 && start <= timestamp && start + windowSize > timestamp;
    }

    @Property
    public boolean startAndWindowNumberCalculationsAreConsistent(
            @ForAll("nodeId") int nodeId,
            @ForAll("itfIdx") int itfIdx,
            @ForAll("windowSize") long windowSize,
            @ForAll("timestamp") long timestamp
    ) {
        long startForTimestamp = UnalignedFixedWindows.windowStartForTimestamp(nodeId, itfIdx, windowSize, timestamp);
        long windowNumber = UnalignedFixedWindows.windowNumber(nodeId, itfIdx, windowSize, timestamp);
        long startForWindowNumber = UnalignedFixedWindows.windowStartForWindowNumber(nodeId, itfIdx, windowSize, windowNumber);
        return startForTimestamp == startForWindowNumber;
    }

}
