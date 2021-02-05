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

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.google.common.collect.Lists;

/**
 * Window function to assign flows to all windows they belong to.
 *
 * A flow spanning more than one window is assigned to multiple windows covering the whole timespan of the flow while
 * keeping the flow itself unchanged.
 *
 * For each exporter node an offset is used to unalign the windows of different exporters.
 * Cf. "Streaming Systems"; O'Reilly 2018; chapter 4; section on "Unaligned fixed windows":
 * "In circumstances for which comparing across windows is unnecessary, it’s often more
 * desirable to spread window completion load out evenly across time."
 *
 * Note: The timestamps of the flow itself are discrete points in time and therefore considered start (inclusive) and
 * end (exclusive).
 */
public class FlowWindows extends NonMergingWindowFn<FlowDocument, IntervalWindow> {

    private final long size;

    public static FlowWindows of(final Duration size) {
        return new FlowWindows(size);
    }

    private FlowWindows(final Duration size) {
        this.size = Objects.requireNonNull(size).getMillis();
    }

    public static long windowOffsetForNode(int nodeId, long windowSize) {
        return Math.abs(Integer.hashCode(nodeId)) % windowSize;
    }

    @Override
    public Collection<IntervalWindow> assignWindows(final AssignContext c) throws Exception {

        // We want to dispatch the flow to all the windows it may be a part of
        // The flow ranges from [delta_switched, last_switched)
        final FlowDocument flow = c.element();

        long deltaSwitchedMillis = flow.getDeltaSwitched().getValue();
        long lastSwitchedMillis = flow.getLastSwitched().getValue();

        long offset = windowOffsetForNode(flow.getExporterNode().getNodeId(), size);

        // Calculate the first and last window since EPOCH (both inclusive) the flow falls into
        long firstWindow = (deltaSwitchedMillis + offset) / size;
        long lastWindow = (lastSwitchedMillis + offset) / size;

        // Iterate window numbers for flow duration and build windows
        final List<IntervalWindow> windows = Lists.newArrayList();
        for (long window = firstWindow; window <= lastWindow; window++) {
            final Instant start = new Instant(window * size - offset);
            windows.add(new IntervalWindow(start, start.plus(this.size)));
        }

        return windows;
    }

    @Override
    public boolean isCompatible(final WindowFn<?, ?> that) {
        return Objects.equals(this, that);
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
        return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
        throw null;
    }
}
