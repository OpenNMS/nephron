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

import java.io.Serializable;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Parameterizes the generation of flows.
 */
public class FlowConfig implements Serializable {

    /**
     * Calculates the {@code lastSwitched} timestamp linearly starting at {@code start} and increasing by {@code step}
     * for each index.
     */
    public static SerializableFunction<Long, Instant> linearIncreasingLastSwitchedPolicy(Instant start, Duration step) {
        return idx -> start.plus(step.multipliedBy(idx));
    }

    /**
     * Calculates the {@code lastSwitched} timestamp uniformly distributed according to the configured
     * start, window size, and number of flows per window.
     */
    public static SerializableFunction<Long, Instant> uniformInWindowLastSwitchedPolicy(FlowGenOptions opts) {
        Instant start = Instant.ofEpochMilli(opts.getStartMs());
        Duration step = Duration.millis((long)((double)opts.getFixedWindowSizeMs() / opts.getFlowsPerWindow()));
        return linearIncreasingLastSwitchedPolicy(start, step);
    }

    /**
     * Returns a function that always return the current time instant.
     */
    public static SerializableFunction<Long, Instant> CURRENT_TIME_LAST_SWITCHED_POLICY = idx -> Instant.now();

    /**
     * Exporter numbers are generated uniformly starting at minExporter.
     */
    public final int minExporter;
    public final int numExporters;

    /**
     * Interface numbers are generated uniformly starting at minInterface.
     */
    public final int minInterface;
    public final int numInterfaces;

    public final int numApplications;
    public final int numHosts;

    public final int numEcns;
    public final int numDscps;

    /**
     * A function that given the index of a generated flow returns its lastSwitched timestamp.
     * The returned lastSwitched time instant is additionally randomized by a normal distribution.
     *
     * Note: Function implementations must not be referentially transparent. Function implementations may ignore
     * the function parameter and simply return the current time instant.
     */
    public final SerializableFunction<Long, Instant> lastSwitched;

    /**
     * LastSwitched timestamps are randomized by a normal distribution with the given sigma.
     */
    public final Duration lastSwitchedSigma;

    /**
     * Flow durations are calculated using an exponential distribution.
     * The random value returned by the exponential distribution is the flow length in seconds.
     */
    public final double flowDurationLambda;

    public FlowConfig(int minExporter, int numExporters, int minInterface, int numInterfaces, int numApplications, int numHosts, int numEcns, int numDscps, SerializableFunction<Long, Instant> lastSwitched, Duration lastSwitchedSigma, double flowDurationLambda) {
        this.minExporter = minExporter;
        this.numExporters = numExporters;
        this.minInterface = minInterface;
        this.numInterfaces = numInterfaces;
        this.numApplications = numApplications;
        this.numHosts = numHosts;
        this.numEcns = numEcns;
        this.numDscps = numDscps;
        this.lastSwitched = lastSwitched;
        this.lastSwitchedSigma = lastSwitchedSigma;
        this.flowDurationLambda = flowDurationLambda;
    }

    public FlowConfig(FlowGenOptions opts, SerializableFunction<Long, Instant> lastSwitched) {
        this(
                opts.getMinExporter(),
                opts.getNumExporters(),
                opts.getMinInterface(),
                opts.getNumInterfaces(),
                opts.getNumApplications(),
                opts.getNumHosts(),
                opts.getNumEcns(),
                opts.getNumDscps(),
                lastSwitched,
                Duration.millis(opts.getLastSwitchedSigmaMs()),
                opts.getFlowDurationLambda()
        );
    }

}
