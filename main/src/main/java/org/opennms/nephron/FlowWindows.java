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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

/**
 * Window function to assign flows to all windows they belong to.
 *
 * A flow spanning more than one window is assigned to multiple windows covering the whole timespan of the flow while
 * keeping the flow itself unchanged.
 *
 * The assigned Windows are aware of the exporter of the flows making them non-equal if the the exporter differs.
 *
 * Note: The timestamps of the flow itself are discrete points in time and therefore considered start (inclusive) and
 * end (exclusive).
 */
public class FlowWindows extends NonMergingWindowFn<FlowDocument, FlowWindows.FlowWindow> {

    private final Duration size;

    public static FlowWindows of(final Duration size) {
        return new FlowWindows(size);
    }

    private FlowWindows(final Duration size) {
        this.size = Objects.requireNonNull(size);
    }

    @Override
    public Collection<FlowWindow> assignWindows(final AssignContext c) throws Exception {
        final long windowSize = this.size.getMillis();

        // We want to dispatch the flow to all the windows it may be a part of
        // The flow ranges from [delta_switched, last_switched)
        final FlowDocument flow = c.element();

        long deltaSwitchedMillis = flow.getDeltaSwitched().getValue();
        long lastSwitchedMillis = flow.getLastSwitched().getValue();

        // Calculate the first and last window since EPOCH (both inclusive) the flow falls into
        long firstWindow = deltaSwitchedMillis / windowSize;
        long lastWindow = lastSwitchedMillis / windowSize;

        // Iterate window numbers for flow duration and build windows
        final List<FlowWindow> windows = Lists.newArrayList();
        for (long window = firstWindow; window <= lastWindow; window++) {
            final Instant start = new Instant(window * windowSize);

            windows.add(new FlowWindow(
                    start,
                    start.plus(this.size),
                    c.element().getExporterNode().getNodeId()
            ));
        }

        return windows;
    }

    @Override
    public boolean isCompatible(final WindowFn<?, ?> that) {
        return Objects.equals(this, that);
    }

    @Override
    public Coder<FlowWindow> windowCoder() {
        return FlowWindow.getCoder();
    }

    @Override
    public WindowMappingFn<FlowWindow> getDefaultWindowMappingFn() {
        throw null;
    }

    /**
     * An implementation of {@link BoundedWindow} that represents an interval from {@link #start}
     * (inclusive) to {@link #end} (exclusive) and tracks the exporters node ID to ensure non-equality for flows from
     * distinct exporters.
     */
    public static class FlowWindow extends BoundedWindow {
        /** Start of the interval, inclusive. */
        private final Instant start;

        /** End of the interval, exclusive. */
        private final Instant end;

        /** ID of the node exporting the flow */
        private final int nodeId;

        public FlowWindow(final Instant start,
                          final Instant end,
                          final int nodeId) {
            this.start = Objects.requireNonNull(start);
            this.end = Objects.requireNonNull(end);
            this.nodeId = nodeId;
        }

        public Instant start() {
            return this.start;
        }

        public Instant end() {
            return this.end;
        }

        public int getNodeId() {
            return this.nodeId;
        }

        @Override
        public Instant maxTimestamp() {
            // End not inclusive
            return this.end.minus(1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FlowWindow)) {
                return false;
            }
            final FlowWindow that = (FlowWindow) o;
            return Objects.equals(this.nodeId, that.nodeId) &&
                   Objects.equals(this.start, that.start) &&
                   Objects.equals(this.end, that.end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.start, this.end, this.nodeId);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                              .add("start", start)
                              .add("end", end)
                              .add("nodeId", nodeId)
                              .toString();
        }

        public static Coder<FlowWindow> getCoder() {
            return FlowWindowCoder.of();
        }

        public static class FlowWindowCoder extends AtomicCoder<FlowWindow> {
            private static final FlowWindowCoder INSTANCE = new FlowWindowCoder();

            private static final Coder<Instant> INSTANT_CODER = InstantCoder.of();
            private static final Coder<Integer> INT_CODER = VarIntCoder.of();

            public static FlowWindowCoder of() {
                return INSTANCE;
            }

            @Override
            public void encode(final FlowWindow value, final OutputStream out) throws CoderException, IOException {
                INSTANT_CODER.encode(value.start, out);
                INSTANT_CODER.encode(value.end, out);
                INT_CODER.encode(value.nodeId, out);
            }

            @Override
            public FlowWindow decode(final InputStream in) throws CoderException, IOException {
                final Instant start = INSTANT_CODER.decode(in);
                final Instant end = INSTANT_CODER.decode(in);
                final int nodeId = INT_CODER.decode(in);
                return new FlowWindow(start, end, nodeId);
            }
        }
    }
}
