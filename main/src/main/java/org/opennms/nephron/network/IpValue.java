/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2020-2020 The OpenNMS Group, Inc.
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

package org.opennms.nephron.network;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copied from OpenNMS.
 */
public class IpValue implements Serializable {

    public static IpValue of(final String input) {
        return of(new StringValue(input));
    }

    public static IpValue of(final StringValue input) {
        Objects.requireNonNull(input);
        if (input.isNullOrEmpty()) {
            throw new IllegalArgumentException("input may not be null or empty");
        }
        final List<StringValue> actualValues = input.splitBy(",");
        List<IPAddressRange> ranges = new ArrayList<>();
        for (StringValue eachValue : actualValues) {
            // In case it is ranged, verify the range
            if (eachValue.isRanged()) {
                final List<StringValue> rangedValues = eachValue.splitBy("-");
                // either a-, or a-b-c, etc.
                if (rangedValues.size() != 2) {
                    LOG.warn("Received multiple ranges {}. Will only use {}", rangedValues, rangedValues.subList(0, 2));
                }
                // Ensure each range is an ip address
                for (StringValue rangedValue : rangedValues) {
                    if (rangedValue.contains("/")) {
                        throw new IllegalArgumentException("Ranged value may not contain a CIDR expression");
                    }
                }
                ranges.add(new IPAddressRange(rangedValues.get(0).getValue(), rangedValues.get(1).getValue()));
            } else if (eachValue.getValue().contains("/")) {
                // Value may be a CIDR address - build range for it
                ranges.add(parseCIDR(eachValue.getValue()));
            } else {
                ranges.add(new IPAddressRange(eachValue.getValue()));
            }
        }
        return new IpValue(ranges);
    }

    private static final Logger LOG = LoggerFactory.getLogger(IpValue.class);

    private final List<IPAddressRange> ranges;

    public IpValue(List<IPAddressRange> ranges) {
        this.ranges = ranges;
    }

    public boolean isInRange(final String address) {
        return ranges.stream().anyMatch(r -> r.contains(address));
    }

    public List<IPAddressRange> getIpAddressRanges() {
        return ranges;
    }

    public static IPAddressRange parseCIDR(final String cidr) {
        final int slashIndex = cidr.indexOf('/');
        if (slashIndex == -1) {
            throw new IllegalArgumentException("Value is not a CIDR expression");
        }

        final byte[] address = toIpAddrBytes(cidr.substring(0, slashIndex));
        final int mask = Integer.parseInt(cidr.substring(slashIndex + 1));

        // Mask the lower bound with all zero
        final byte[] lower = Arrays.copyOf(address, address.length);
        for (int i = lower.length - 1; i >= mask / 8; i--) {
            if (i*8 >= mask) {
                lower[i] = (byte) 0x00;
            } else {
                lower[i] &= 0xFF << (8 - (mask - i * 8));
            }
        }

        // Mask the upper bound with all ones
        final byte[] upper = Arrays.copyOf(address, address.length);
        for (int i = upper.length - 1; i >= mask / 8; i--) {
            if (i*8 >= mask) {
                upper[i] = (byte) 0xFF;
            } else {
                upper[i] |= 0xFF >> (mask - i * 8);
            }
        }

        return new IPAddressRange(toIpAddrString(lower),
                                  toIpAddrString(upper));
    }

    private static byte[] toIpAddrBytes(final String dottedNotation) {
        return new IPAddress(dottedNotation).toOctets();
    }

    private static String toIpAddrString(final byte[] addr) {
        return new IPAddress(addr).toDbString();
    }

}
