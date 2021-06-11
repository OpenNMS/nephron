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

package org.opennms.nephron.generator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.function.BiConsumer;

import org.opennms.nephron.catheter.Exporter;
import org.opennms.nephron.catheter.FlowReport;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public class Handler implements BiConsumer<Exporter, FlowReport>, Closeable {

    private final FlowDocGen flowDocGen;
    private final FlowDocSender flowDocSender;

    public Handler(final String bootstrapServers,
                   final String flowTopic,
                   final Random random) {
        this(bootstrapServers, flowTopic, random, null);
    }

    public Handler(final String bootstrapServers,
                   final String flowTopic,
                   final Random random,
                   final File propertiesFile) {
        this.flowDocGen = new FlowDocGen(random);
        this.flowDocSender = new FlowDocSender(bootstrapServers, flowTopic, propertiesFile);
    }

    @Override
    public void accept(final Exporter exporter, final FlowReport report) {
        final FlowDocument flowDocument = flowDocGen.createFlowDocument(exporter, report);
        flowDocSender.send(flowDocument);
    }

    @Override
    public void close() throws IOException {
        flowDocSender.close();
    }

}
