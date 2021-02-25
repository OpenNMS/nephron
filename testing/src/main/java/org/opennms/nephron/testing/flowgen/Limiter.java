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

/**
 * Determines if a flow is emitted or not.
 */
public abstract class Limiter {

    /**
     * Checks if the given number of flows may be emitted.
     */
    public abstract boolean check(long incr);

    public static Limiter of(long flowsPerSecond) {
        if (flowsPerSecond <= 0 || flowsPerSecond == Long.MAX_VALUE) {
            return Limiter.OFF;
        } else {
            return new Limiter.FlowsPerSecond(flowsPerSecond);
        }
    }

    public static Limiter OFF = new Limiter() {
        @Override
        public boolean check(long incr) {
            return true;
        }
    };

    public static class FlowsPerSecond extends Limiter {
        private final long flowsPerSeconds;

        private long allowance;
        private long lastCheck = System.currentTimeMillis();

        public FlowsPerSecond(long flowsPerSeconds) {
            this.flowsPerSeconds = flowsPerSeconds;
            allowance = flowsPerSeconds * 1000;
        }

        public boolean check(long incr) {
            long current = System.currentTimeMillis();
            long timePassed = current - lastCheck;
            lastCheck = current;
            allowance += timePassed * flowsPerSeconds;
            if (allowance > flowsPerSeconds * 1000) {
                allowance = flowsPerSeconds * 1000;
            }
            if (allowance > incr * 1000) {
                allowance -= incr * 1000;
                return true;
            } else {
                return false;
            }
        }
    }
}
