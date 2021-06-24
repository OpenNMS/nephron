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

import java.util.function.Supplier;

/**
 * Determines if a flow is emitted or not.
 */
public abstract class Limiter {

    /**
     * Checks if the given number of flows may be emitted.
     */
    public abstract boolean check(long incr);

    /**
     * The current limiter state.
     *
     * Stored in checkmarks.
     */
    public abstract long state();

    public static Limiter of(long flowsPerSecond) {
        return of(flowsPerSecond, System::currentTimeMillis);
    }

    public static Limiter restore(long flowsPerSecond, long limiterState) {
        return restore(flowsPerSecond, limiterState, System::currentTimeMillis);
    }

    public static Limiter of(long flowsPerSecond, Supplier<Long> currentTimeMillis) {
        if (flowsPerSecond <= 0) {
            return Limiter.OFF;
        } else {
            return new Limiter.FlowsPerSecond(currentTimeMillis, flowsPerSecond);
        }
    }

    public static Limiter restore(long flowsPerSecond, long limiterState, Supplier<Long> currentTimeMillis) {
        if (flowsPerSecond <= 0) {
            return Limiter.OFF;
        } else {
            return new Limiter.FlowsPerSecond(currentTimeMillis, flowsPerSecond, limiterState);
        }
    }

    public static Limiter OFF = new Limiter() {
        @Override
        public boolean check(long incr) {
            return true;
        }

        @Override
        public long state() {
            return 0;
        }
    };

    public static class FlowsPerSecond extends Limiter {

        private final Supplier<Long> currentTimeMillis;
        private final long flowsPerSecond;

        // tracks how many permits can be given during the next second
        // -> allowance represents fractional permits (1/1000 of a permit)
        // -> with each elapsed millisecond the allowance increases by flowsPerSecond
        private long allowance;
        private long lastCheck;

        public FlowsPerSecond(Supplier<Long> currentTimeMillis, long flowsPerSecond) {
            this.currentTimeMillis = currentTimeMillis;
            this.flowsPerSecond = flowsPerSecond;
            allowance = flowsPerSecond * 1000;
            lastCheck = currentTimeMillis.get();
        }

        public FlowsPerSecond(Supplier<Long> currentTimeMillis, long flowsPerSecond, long state) {
            this.currentTimeMillis = currentTimeMillis;
            this.flowsPerSecond = flowsPerSecond;
            allowance = state % flowsPerSecond;
            lastCheck = state / flowsPerSecond;
        }

        public boolean check(long incr) {
            long current = currentTimeMillis.get();
            long timePassed = current - lastCheck;
            lastCheck = current;
            allowance += timePassed * flowsPerSecond;
            if (allowance > flowsPerSecond * 1000) {
                allowance = flowsPerSecond * 1000;
            }
            if (allowance >= incr * 1000) {
                allowance -= incr * 1000;
                return true;
            } else {
                return false;
            }
        }

        @Override
        public long state() {
            // encode lastCheck and allowance into a single state
            // -> normalize lastCheck and allowance
            //    * ensure allowance < flowsPerSecond
            //    * shift lastChecked into the past accordingly
            var l = lastCheck - allowance / flowsPerSecond;
            var a = allowance % flowsPerSecond;
            // encode l and a into a single long avoiding rounding errors
            return l * flowsPerSecond + a;
        }
    }
}
