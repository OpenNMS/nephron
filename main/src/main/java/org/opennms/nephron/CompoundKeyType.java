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

import org.apache.beam.repackaged.core.org.apache.commons.lang3.ArrayUtils;
import org.opennms.nephron.elastic.FlowSummary;
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
    private RefType[] parts;

    CompoundKeyType(CompoundKeyType parent, RefType... parts) {
        this.parent = parent;
        this.parts = parent == null ? parts : ArrayUtils.addAll(parent.parts, parts);
    }

    public CompoundKeyType getParent() {
        return parent;
    }

    public RefType[] getParts() {
        return parts;
    }

    CompoundKey decode(InputStream is) throws IOException {
        CompoundKeyData.Builder builder = new CompoundKeyData.Builder();
        for (RefType refType: parts) {
            refType.decode(builder, is);
        }
        return new CompoundKey(this, builder.build());
    }

    void encode(CompoundKeyData data, OutputStream os) throws IOException {
        for (RefType refType: parts) {
            refType.encode(data, os);
        }
    }

    public CompoundKey create(FlowDocument flow) throws MissingFieldsException {
        CompoundKeyData.Builder builder = new CompoundKeyData.Builder();
        for (RefType refType: parts) {
            refType.create(builder, flow);
        }
        return new CompoundKey(this, builder.build());
    }

    void populate(CompoundKeyData data, FlowSummary flow) {
        flow.setGroupedBy(this);
        flow.setGroupedByKey(groupedByKey(data));
        for (RefType refType: parts) {
            refType.populate(data, flow);
        }
    }

    String groupedByKey(CompoundKeyData data) {
        StringBuilder sb = new StringBuilder();
        for (RefType refType: parts) {
            if (sb.length() > 0) sb.append('-');
            refType.groupedByKey(data, sb);
        }
        return sb.toString();
    }

}
