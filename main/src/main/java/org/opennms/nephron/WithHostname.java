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

package org.opennms.nephron;

import org.opennms.netmgt.flows.persistence.model.FlowDocument;

/**
 * Pairs an arbitrary value with a host name.
 *
 * Used by {@link CompoundKeyType#create(FlowDocument)} to transport host names in a side channel.
 */
public class WithHostname<T> {
    public final T value;
    public final String hostname;

    public WithHostname(final T value, final String hostname) {
        this.value = value;
        this.hostname = hostname;
    }

    public static class Builder<T> {
        private final T value;

        private Builder(final T value) {
            this.value = value;
        }

        public WithHostname<T> withoutHostname() {
            return new WithHostname<>(this.value, null);
        }

        public WithHostname<T> andHostname(final String hostname) {
            return new WithHostname<>(this.value, hostname);
        }
    }

    public static <T> Builder<T> having(final T value) {
        return new Builder<>(value);
    }
}
