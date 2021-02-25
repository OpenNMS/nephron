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
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * Configures a {@link SyntheticFlowSource}.
 */
public class SourceConfig implements Serializable {

    public static SourceConfig of(
            FlowGenOptions options,
            SyntheticFlowTimestampPolicyFactory timestampPolicyFactory
    ) {
        SerializableFunction<Long, Instant> lastSwitchedPolicy;
        if (options.getPlaybackMode()) {
            lastSwitchedPolicy = FlowConfig.uniformInWindowLastSwitchedPolicy(options);
        } else {
            lastSwitchedPolicy = FlowConfig.CURRENT_TIME_LAST_SWITCHED_POLICY;
            if (options.getFlowsPerSecond() != 0 && options.getFlowsPerSecond() != Long.MAX_VALUE) {
                options.setFlowsPerWindow(options.getFlowsPerSecond() * options.getFixedWindowSizeMs() / 1000);
            } else {
                options.setFlowsPerSecond(options.getFlowsPerWindow() * 1000 / options.getFixedWindowSizeMs());
            }
        }
        return new SourceConfig(
                new FlowConfig(options, lastSwitchedPolicy),
                timestampPolicyFactory,
                options.getSeed(),
                options.getNumFlowGenerators(),
                options.getNumWindows() * options.getFlowsPerWindow(),
                1,
                0,
                options.getFlowsPerSecond()
        );
    }

    public final FlowConfig flowConfig;
    public final SyntheticFlowTimestampPolicyFactory timestampPolicyFactory;
    public final long seed;
    public final int numGenerators;
    public final long maxIdx;
    public final int idxInc;
    public final int idxOffset;
    public final long flowsPerSecond;

    public SourceConfig(FlowConfig flowConfig, SyntheticFlowTimestampPolicyFactory timestampPolicyFactory, long seed, int numGenerators, long maxIdx, int idxInc, int idxOffset, long flowsPerSecond) {
        this.flowConfig = flowConfig;
        this.timestampPolicyFactory = timestampPolicyFactory;
        this.seed = seed;
        this.numGenerators = numGenerators;

        this.maxIdx = maxIdx;
        this.idxInc = idxInc;
        this.idxOffset = idxOffset;
        this.flowsPerSecond = flowsPerSecond;
    }

    public List<SourceConfig> split() {
        List<SourceConfig> res = new ArrayList<>(numGenerators);
        long seed = this.seed;
        for (int i = 0; i < numGenerators; i++) {
            res.add(
                    new SourceConfig(
                            flowConfig,
                            timestampPolicyFactory,
                            seed,
                            1,
                            maxIdx,
                            numGenerators,
                            i,
                            flowsPerSecond
                    )
            );
            seed *= seed;
        }
        return res;
    }
}
