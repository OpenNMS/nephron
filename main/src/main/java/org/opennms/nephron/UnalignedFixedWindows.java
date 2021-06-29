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

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import it.unimi.dsi.fastutil.HashCommon;

/**
 * When flows are assigned to windows the nodeId of the exporter and either the input snmp interface index or output
 * snmp interface is used to calculate window shifts.
 * <p>
 * Different shifts for windows for different exporter/interfaces can be used because no aggregations are done across
 * different exporter/interfaces. Using shifted windows for different exporter/interfaces spreads window completion
 * load more evenly across time, thereby reducing peak loads.
 * <p>
 * In case of a flow with {@code flow.direction == INGRESS}) its input snmp interface index is used and otherwise
 * its output interface index for calculating window shifts.
 */
public class UnalignedFixedWindows extends NonMergingWindowFn<FlowDocument, IntervalWindow> {

    public static UnalignedFixedWindows of(Duration size) {
        return new UnalignedFixedWindows(size);
    }

    public static long perNodeShift(int nodeId, int itfIdx, long windowSize) {
        return Math.abs(31 * HashCommon.mix(nodeId) + HashCommon.mix(itfIdx)) % windowSize;
    }

    /**
     * Returns the start of a shifted window that includes the given timestamp.
     *
     * Shifted windows start at: shift + windowNumber(nodeId, itfIdx, windowSize, timestamp) * windowSize,
     */
    public static long windowStartForTimestamp(
            int nodeId,
            int itfIdx,
            long windowSize,
            long timestamp) {
        long shift = perNodeShift(nodeId, itfIdx, windowSize);
        return timestamp - (timestamp - shift) % windowSize;
    }

    /**
     * Return the number of the shifted window the given timestamp falls into
     */
    public static long windowNumber(
            int nodeId,
            int itfIdx,
            long windowSize,
            long timestamp
    ) {
        long shift = perNodeShift(nodeId, itfIdx, windowSize);
        return (timestamp - shift) / windowSize;
    }

    public static long windowStartForWindowNumber(
            int nodeId,
            int itfIdx,
            long windowSize,
            long windowNumber
    ) {
        long shift = perNodeShift(nodeId, itfIdx, windowSize);
        return shift + windowNumber * windowSize;

    }

    public static int getItfIdx(FlowDocument flow) {
        return flow.getDirection() == Direction.INGRESS ? flow.getInputSnmpIfindex().getValue() : flow.getOutputSnmpIfindex().getValue();
    }

    private final long size;

    private UnalignedFixedWindows(Duration size) {
        this.size = Objects.requireNonNull(size).getMillis();
    }

    @Override
    public Collection<IntervalWindow> assignWindows(final AssignContext c) throws Exception {
        final FlowDocument flow = c.element();
        long timestamp = c.timestamp().getMillis();
        long startMs = windowStartForTimestamp(flow.getExporterNode().getNodeId(), getItfIdx(flow), size, timestamp);
        Instant start = Instant.ofEpochMilli(startMs);
        IntervalWindow window = new IntervalWindow(start, start.plus(this.size));
        return Collections.singleton(window);
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
        return Objects.equals(this, other);
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnalignedFixedWindows that = (UnalignedFixedWindows) o;
        return size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(size);
    }
}
