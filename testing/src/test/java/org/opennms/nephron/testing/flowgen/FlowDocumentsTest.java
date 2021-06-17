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

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.opennms.nephron.Pipeline;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

public class FlowDocumentsTest {

    @Test
    public void canGenerateFlows() {
        final FlowGenOptions options =
                // 10.000 * 100 flows
                PipelineOptionsFactory.fromArgs(
                        "--lastSwitchedSigmaMs=0",
                        "--fixedWindowSizeMs=100000",
                        "--numWindows=10000",
                        "--flowsPerWindow=100"
                ).withValidation().as(FlowGenOptions.class);

        // skip 100.000 flow documents and then take one
        long skipped = 100000;

        SourceConfig sourceConfig = SourceConfig.of(
                options,
                SyntheticFlowTimestampPolicyFactory.withLimitedDelay(options, Pipeline.ReadFromKafka::getTimestamp)
        );

        Stream<FlowDocument> stream = FlowDocuments.stream(sourceConfig);

        Iterator<FlowDocument> iter = stream.skip(skipped).limit(1).iterator();

        Assert.assertTrue(iter.hasNext());
        FlowDocument fd = iter.next();

        Instant start = Instant.ofEpochMilli(options.getStartMs());
        Assert.assertEquals(start.plus(Duration.standardSeconds(skipped)), Instant.ofEpochMilli(fd.getLastSwitched().getValue()));
    }

    @Test
    public void streamEnds() {
        final FlowGenOptions options =
                // 100 * 100 flows
                PipelineOptionsFactory.fromArgs(
                        "--numWindows=100",
                        "--flowsPerWindow=100"
                ).withValidation().as(FlowGenOptions.class);

        // skip 10.000 flow documents and then take one
        long skipped = 9999;

        SourceConfig sourceConfig = SourceConfig.of(
                options,
                SyntheticFlowTimestampPolicyFactory.withLimitedDelay(options, Pipeline.ReadFromKafka::getTimestamp)
        );

        Stream<FlowDocument> stream = FlowDocuments.stream(sourceConfig);

        Iterator<FlowDocument> iter = stream.skip(skipped).limit(1).iterator();

        Assert.assertTrue(iter.hasNext());
        iter.next();
        Assert.assertFalse(iter.hasNext());

    }

}
