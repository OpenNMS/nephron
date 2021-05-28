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

import com.google.gson.Gson;

/**
 * Contains fields for all possible kinds of {@link CompoundKeyType}s.
 */
public class CompoundKeyData {

    private static Gson GSON = new Gson();

    public final String foreignSource;
    public final String foreignId;
    public final int nodeId;

    public final int ifIndex;

    public final int dscp;

    public final String application;

    public final String address;

    public final String location;
    public final Integer protocol;
    public final String largerAddress;

    public CompoundKeyData(Builder builder) {
        this.foreignSource = builder.foreignSource;
        this.foreignId = builder.foreignId;
        this.nodeId = builder.nodeId;
        this.ifIndex = builder.ifIndex;
        this.dscp = builder.dscp;
        this.application = builder.application;
        this.address = builder.address;
        this.location = builder.location;
        this.protocol = builder.protocol;
        this.largerAddress = builder.largerAddress;
    }

    public String getConversationKey() {
        StringBuilder sb = new StringBuilder();
        sb.append('[')
                .append(GSON.toJson(location))
                .append(',')
                .append(protocol)
                .append(',')
                .append(GSON.toJson(address))
                .append(',')
                .append(GSON.toJson(largerAddress))
                .append(',')
                .append(GSON.toJson(application))
                .append(']');
        return sb.toString();
    }

    public static class Builder {
        public String foreignSource;
        public String foreignId;
        public int nodeId;

        public int ifIndex;

        public int dscp;

        public String application;

        public String address;

        public String location;
        public Integer protocol;
        public String largerAddress;

        public Builder() {}

        public Builder(CompoundKeyData data) {
            this.foreignSource = data.foreignSource;
            this.foreignId = data.foreignId;
            this.nodeId = data.nodeId;
            this.ifIndex = data.ifIndex;
            this.dscp = data.dscp;
            this.application = data.application;
            this.address = data.address;
            this.location = data.location;
            this.protocol = data.protocol;
            this.largerAddress = data.largerAddress;
        }

        public Builder withAddress(String address) {
            this.address = address;
            return this;
        }

        public CompoundKeyData build() {
            return new CompoundKeyData(this);
        }

    }

}
