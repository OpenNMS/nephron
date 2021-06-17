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
import java.util.Optional;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.NephronOptions;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public interface SyntheticFlowTimestampPolicyFactory extends Serializable {

    /**
     * Creates a SyntheticFlowTimestampPolicy.
     *
     * @param previous an optional time instant that a timestamp policy can use when
     *                 initialized based on a checkpoint mark
     */
    SyntheticFlowTimestampPolicy create(Optional<Instant> previous);

    static SyntheticFlowTimestampPolicyFactory withLimitedDelay(
            Duration maxDelay,
            SerializableFunction<FlowDocument, Instant> timestampFunction
    ) {
        return new SyntheticFlowTimestampPolicyFactory() {
            @Override
            public SyntheticFlowTimestampPolicy create(Optional<Instant> previous) {
                return new SyntheticFlowTimestampPolicy.WithLimitedDelay(maxDelay, timestampFunction, previous);
            }
        };
    }

    static SyntheticFlowTimestampPolicyFactory withLimitedDelay(
            NephronOptions options,
            SerializableFunction<FlowDocument, Instant> timestampFunction
    ) {
        return withLimitedDelay(Duration.millis(options.getDefaultMaxInputDelayMs()), timestampFunction);
    }
}
