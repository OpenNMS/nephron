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

import java.io.Serializable;
import java.util.Objects;

import org.opennms.netmgt.flows.persistence.model.Direction;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FlowBytes implements Serializable, Comparable<FlowBytes> {
    final long bytesIn;
    final long bytesOut;

    @JsonCreator
    public FlowBytes(@JsonProperty("bytesIn") long bytesIn,@JsonProperty("bytesOut") long bytesOut) {
        this.bytesIn = bytesIn;
        this.bytesOut = bytesOut;
    }

    public FlowBytes(FlowDocument flow, double multiplier) {
        // TODO: FIXME: Add test for sampling interval
        if (Direction.INGRESS.equals(flow.getDirection())) {
            bytesIn =  (long)(flow.getNumBytes().getValue() * flow.getSamplingInterval().getValue() * multiplier);
            bytesOut = 0;
        } else {
            bytesIn = 0;
            bytesOut = (long)(flow.getNumBytes().getValue() * flow.getSamplingInterval().getValue() * multiplier);
        }
    }

    public FlowBytes(FlowDocument flow) {
        this(flow, 1.0d);
    }

    public static FlowBytes sum(FlowBytes a, FlowBytes b) {
        return new FlowBytes(a.bytesIn + b.bytesIn, a.bytesOut + b.bytesOut);
    }

    public long getBytesIn() {
        return bytesIn;
    }

    public long getBytesOut() {
        return bytesOut;
    }

    @Override
    public int compareTo(FlowBytes other) {
        return Long.compare(bytesIn + bytesOut, other.bytesIn + other.bytesOut);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FlowBytes)) return false;
        FlowBytes flowBytes = (FlowBytes) o;
        return bytesIn == flowBytes.bytesIn &&
                bytesOut == flowBytes.bytesOut;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytesIn, bytesOut);
    }

    @Override
    public String toString() {
        return "FlowBytes{" +
                "bytesIn=" + bytesIn +
                ", bytesOut=" + bytesOut +
                '}';
    }
}
