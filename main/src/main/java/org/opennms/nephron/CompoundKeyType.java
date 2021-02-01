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

import static org.opennms.nephron.RefType.APPLICATION_PART;
import static org.opennms.nephron.RefType.CONVERSATION_PART;
import static org.opennms.nephron.RefType.DSCP_PART;
import static org.opennms.nephron.RefType.EXPORTER_PART;
import static org.opennms.nephron.RefType.HOST_PART;
import static org.opennms.nephron.RefType.INTERFACE_PART;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

/**
 * Describes compound keys.
 *
 * Contains a sequence of {@link RefType}s that describes the dimension flow data is grouped into.
 *
 * A compound key type may be derived from a parent type by adding additional grouping dimensions. Parent types are
 * used when calculating topK aggregations.
 */
public enum CompoundKeyType {

    EXPORTER(null, EXPORTER_PART),
    EXPORTER_INTERFACE(EXPORTER, INTERFACE_PART),

    EXPORTER_INTERFACE_APPLICATION(EXPORTER_INTERFACE, APPLICATION_PART),
    EXPORTER_INTERFACE_CONVERSATION(EXPORTER_INTERFACE, CONVERSATION_PART),
    EXPORTER_INTERFACE_HOST(EXPORTER_INTERFACE, HOST_PART),

    EXPORTER_INTERFACE_TOS(EXPORTER_INTERFACE, DSCP_PART),

    EXPORTER_INTERFACE_TOS_APPLICATION(EXPORTER_INTERFACE_TOS, APPLICATION_PART),
    EXPORTER_INTERFACE_TOS_CONVERSATION(EXPORTER_INTERFACE_TOS, CONVERSATION_PART),
    EXPORTER_INTERFACE_TOS_HOST(EXPORTER_INTERFACE_TOS, HOST_PART);

    private CompoundKeyType parent;
    private RefType<Ref>[] parts;

    CompoundKeyType(CompoundKeyType parent, RefType<? extends Ref>... parts) {
        this.parent = parent;
        this.parts = parent == null ? (RefType<Ref>[]) parts : ArrayUtils.addAll(parent.parts, (RefType<Ref>[]) parts);
    }

    public CompoundKeyType getParent() {
        return parent;
    }

    public RefType<Ref>[] getParts() {
        return parts;
    }

    CompoundKey decode(InputStream is) throws IOException {
        List<Ref> refs = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            refs.add(parts[i].decode(is));
        }
        return new CompoundKey(this, refs);
    }

    void encode(List<Ref> refs, OutputStream os) throws IOException {
        for (int i = 0; i < parts.length; i++) {
            parts[i].encode(refs.get(i), os);
        }
    }

    /**
     * Creates a compound key that may be accompanied by a host name from a flow document.
     * <p>
     * The key parts of the created key are created by delegating to the {@link RefType}s of this compound key type.
     */
    List<WithHostname<CompoundKey>> create(FlowDocument flow) throws MissingFieldsException {
        // the method returns a list of compound keys
        // -> a list of lists of the corresponding key parts must be determined
        // -> each key part type contributes a list of choices for that key part
        // -> the lists of choices is "exploded" into list of lists of key parts
        List<List<WithHostname<Ref>>> refss = null;
        for (RefType part : parts) {
            // each key part type yields a list of choices (refs)
            // -> all current lists in refss have to be extended by all choices
            List<WithHostname<Ref>> refs = part.create(flow);
            if (refss == null) {
                // first part
                // -> each choice yields a singleton list of key parts
                refss = refs.stream().map(whn -> Collections.singletonList(whn)).collect(Collectors.toList());
            } else {
                // append choices to current lists
                // -> determine the next refss list
                List<List<WithHostname<Ref>>> next = new ArrayList<>();
                // for each current list and each choice:
                // -> copy the current list, extends it by the choice and add it to the next refss
                for (List<WithHostname<Ref>> prefix : refss) {
                    for (WithHostname<Ref> suffix : refs) {
                        List<WithHostname<Ref>> l = new ArrayList<>();
                        l.addAll(prefix);
                        l.add(suffix);
                        next.add(l);
                    }
                }
                refss = next;
            }
        }
        // convert the list of part lists into a list of compound keys that may be accompanied by a host name
        return refss
                .stream()
                .map(refs -> {
                    // get the host name if it is available on the last part
                    // -> considering the host name on the last part only avoids duplicated storage of the
                    //    IP <-> hostname relation; (that relation is used in hostname resolution)
                    String hostname = refs.get(refs.size() - 1).hostname;
                    CompoundKey key = new CompoundKey(this, refs.stream().map(whn -> whn.value).collect(Collectors.toList()));
                    return WithHostname.having(key).andHostname(hostname);
                })
                .collect(Collectors.toList());
    }

}
