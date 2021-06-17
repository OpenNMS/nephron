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

package org.opennms.nephron.testing.flowgen;


import java.util.Optional;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public abstract class SyntheticFlowTimestampPolicy {

    /**
     * Side effecting method that returns the timestamp for a flow document.
     *
     * A timestamp policy may track the observed flow documents in order to update its watermark estimation.
     */
    public abstract Instant getTimestampForFlow(FlowDocument fd);

    /**
     * Returns the estimation of the current watermark.
     */
    public abstract Instant getWatermark();

    /**
     * Returns the time instant that is stored in {@link FlowReader.CheckpointMark} instances.
     *
     * Different timestamp policies may return different time instances, e.g. the current
     * watermark or the maximum observed flow timestamp.
     */
    public abstract Instant getCheckpointInstant();

    /**
     * Inspired by {@link org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay}
     */
    public static class WithLimitedDelay extends SyntheticFlowTimestampPolicy {

        private final Duration maxDelay;
        private final SerializableFunction<FlowDocument, Instant> timestampFunction;
        private Instant maxEventTimestamp;

        public WithLimitedDelay(
                Duration maxDelay,
                SerializableFunction<FlowDocument, Instant> timestampFunction,
                Optional<Instant> previousMaxEventTimestamp
        ) {
            this.maxDelay = maxDelay;
            this.timestampFunction = timestampFunction;
            this.maxEventTimestamp = previousMaxEventTimestamp.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE);
        }

        @Override
        public Instant getTimestampForFlow(FlowDocument fd) {
            Instant ts = timestampFunction.apply(fd);
            if (ts.isAfter(maxEventTimestamp)) {
                maxEventTimestamp = ts;
            }
            return ts;
        }

        @Override
        public Instant getWatermark() {
            Instant now = Instant.now();
            Instant wm;
            if (maxEventTimestamp.isAfter(now)) {
                wm = now.minus(maxDelay);
            } else {
                wm = maxEventTimestamp.minus(maxDelay);
            }
            return wm;
        }

        @Override
        public Instant getCheckpointInstant() {
            return maxEventTimestamp;
        }
    }

}
