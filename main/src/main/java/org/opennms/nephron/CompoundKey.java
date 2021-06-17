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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.opennms.nephron.elastic.FlowSummary;

/**
 * Represents a compound key.
 *
 * A compound key is used to group flows into different dimensions. A compound key consists of a type that defines
 * the used dimensions and a data class that contains the fields that determine the value of the key in these dimensions.
 *
 * The {@link CompoundKeyData} class can store the fields for all dimensions in order to reduce memory churn. Instances
 * of the {@code CompoundKeyData} class can be shared between different compound key instances because the equality check
 * and hashCode calculation does only consider the fields that correspond to the dimension that are used in the
 * referencing key.
 */
@DefaultCoder(CompoundKey.CompoundKeyCoder.class)
public class CompoundKey {

    public final CompoundKeyType type;
    public final CompoundKeyData data;

    /**
     * Constructs a CompoundKey.
     */
    CompoundKey(CompoundKeyType type, CompoundKeyData data) {
        this.type = type;
        this.data = data;
    }

    public CompoundKeyType getType() {
        return type;
    }

    /**
     * Build the parent, or "outer" key for the current key.
     *
     * @return the outer key, or null if no such key exists
     */
    public CompoundKey getOuterKey() {
        return type.getParent() == null ? null : this.cast(type.getParent());
    }

    /**
     * Cast this key into a key of the given type.
     *
     * It is required that the {@link CompoundKeyData} instance of this key has all fields set
     * that are required by the target type.
     */
    public CompoundKey cast(CompoundKeyType targetType) {
        return new CompoundKey(targetType, data);
    }

    public String groupedByKey() {
        return type.groupedByKey(data);
    }

    public void populate(FlowSummary flow) {
        type.populate(data, flow);
    }

    /**
     * Checks if this key includes the conversation dimension as its last part and that all required fields for the
     * conversation dimension are set.
     *
     * This check is used to determine if a conversation is considered in the TopK aggregation for conversations.
     * If the key of a conversation is not complete then it is not considered there.
     */
    public boolean isCompleteConversationKey() {
        RefType[] parts = type.getParts();
        int l = parts.length;
        return parts[l - 1].isCompleteConversationRef(data);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundKey that = (CompoundKey) o;
        if (type != that.type) {
            return false;
        }
        // only the part of the data is considered that is related to one of the RefTypes of this key
        for (RefType refType: type.getParts()) {
            if (!refType.equals(data, that.data)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 17;
        hash = hash * 31 + type.hashCode();
        // only the part of the data is considered that is related to one of the RefTypes of this key
        for (RefType refType: type.getParts()) {
            hash = hash * 31 + refType.hashCode(data);
        }
        return hash;
    }

    private final static Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

    public static class CompoundKeyCoder extends AtomicCoder<CompoundKey> {
        @Override
        public void encode(CompoundKey value, OutputStream outStream) throws IOException {
            INT_CODER.encode(value.getType().ordinal(), outStream);
            value.type.encode(value.data, outStream);
        }

        @Override
        public CompoundKey decode(InputStream inStream) throws IOException {
            CompoundKeyType type = CompoundKeyType.values()[INT_CODER.decode(inStream)];
            return type.decode(inStream);
        }

        @Override
        public boolean consistentWithEquals() {
            return true;
        }

    }
}
