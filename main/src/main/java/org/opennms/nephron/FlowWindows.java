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

import com.google.common.collect.Lists;

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
        // The flow ranges from [delta_switched, last_switched]
        final FlowDocument flow = c.element();

        final long flowStart = flow.getDeltaSwitched().getValue();

        // Make the the flow duration at least spanning a milliseconds (smallest time unit)
        final long flowEnd = Long.max(flow.getLastSwitched().getValue(), flowStart + 1);

        final long lastBucket = (flowEnd + windowSize - 1) / windowSize * windowSize;

        final List<FlowWindow> windows = Lists.newArrayList();

        long timestamp = flowStart;
        while (timestamp < lastBucket) {
//            if (timestamp <= c.timestamp().minus(maxFlowDuration).getMillis()) {
//                // Caused by: java.lang.IllegalArgumentException: Cannot output with timestamp 1970-01-01T00:00:00.000Z. Output timestamps must be no earlier than the timestamp of the current input (2020-
//                //                            04-14T15:33:11.302Z) minus the allowed skew (30 minutes). See the DoFn#getAllowedTimestampSkew() Javadoc for details on changing the allowed skew.
//                //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.checkTimestamp(SimpleDoFnRunner.java:607)
//                //                    at org.apache.beam.runners.core.SimpleDoFnRunner$DoFnProcessContext.outputWithTimestamp(SimpleDoFnRunner.java:573)
//                //                    at org.opennms.nephron.FlowAnalyzer$1.processElement(FlowAnalyzer.java:96)
//                RATE_LIMITED_LOG.warn("Skipping output for flow w/ start: {}, end: {}, target timestamp: {}, current input timestamp: {}. Full flow: {}",
//                                      Instant.ofEpochMilli(flowStart), Instant.ofEpochMilli(flow.getLastSwitched().getValue()), Instant.ofEpochMilli(timestamp), c.timestamp(),
//                                      flow);
//                timestamp += windowSize;
//                continue;
//            }

            final Instant start = new Instant(timestamp - (timestamp % this.size.getMillis()));

            windows.add(new FlowWindow(start, start.plus(this.size), c.element().getExporterNode().getNodeId()));

            timestamp += windowSize;
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

    public static class FlowWindow extends BoundedWindow {
        private final Instant start;
        private final Instant end;
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
