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
 * A compound key is used to group flows into different dimensions. A compound key is made up from a list of
 * {@link Ref} values.
 */
@DefaultCoder(CompoundKey.CompoundKeyCoder.class)
public class CompoundKey {

    private final CompoundKeyType type;
    private final List<Ref> refs;

    /**
     * Constructs a CompoundKey.
     * <p>
     * The {@link RefType}s of the given type must correspond to the given refs.
     */
    CompoundKey(CompoundKeyType type, List<Ref> refs) {
        if (type.getParts().length != refs.size()) {
            throw new RuntimeException("size of compound key type parts and given refs do not match - #parts: "
                                       + type.getParts().length + "; #refs: " + refs.size());
        }
        this.type = type;
        this.refs = refs;
    }

    public CompoundKeyType getType() {
        return type;
    }

    public List<Ref> getRefs() {
        return refs;
    }

    /**
     * Build the parent, or "outer" key for the current key.
     *
     * @return the outer key, or null if no such key exists
     */
    public CompoundKey getOuterKey() {
        return type.getParent() == null ? null :
               new CompoundKey(type.getParent(), refs.subList(0, type.getParent().getParts().length));
    }

    public String groupedByKey() {
        return refs.stream().map(Ref::idAsString).collect(Collectors.joining("-"));
    }

    public void populate(FlowSummary flow) {
        flow.setGroupedBy(type);
        flow.setGroupedByKey(groupedByKey());
        for (int i = 0; i < type.getParts().length; i++) {
            type.getParts()[i].populate(refs.get(i), flow);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundKey cKey = (CompoundKey) o;
        return type == cKey.type &&
               Objects.equals(refs, cKey.refs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, refs);
    }

    @Override
    public String toString() {
        return "CompoundKey{" +
               "type=" + type +
               ", refs=" + refs +
               '}';
    }

    private final static Coder<Integer> INT_CODER = NullableCoder.of(VarIntCoder.of());

    public static class CompoundKeyCoder extends AtomicCoder<CompoundKey> {
        @Override
        public void encode(CompoundKey value, OutputStream outStream) throws IOException {
            INT_CODER.encode(value.getType().ordinal(), outStream);
            value.getType().encode(value.getRefs(), outStream);
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
