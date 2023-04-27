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

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.opennms.nephron.v2.PipelineRunner;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import java.util.concurrent.BlockingQueue;

public class Nephron implements PipelineRunner {
    static void runNephron(NephronOptions options) {
        final org.apache.beam.sdk.Pipeline p = Pipeline.create(options);
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(NephronOptions.class);
        final NephronOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(NephronOptions.class);
        runNephron(options);
    }

    @Override
    public void run(String[] args, BlockingQueue<FlowDocument> queue) {
        PipelineOptionsFactory.register(NephronOptions.class);
        final NephronOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NephronOptions.class);
        org.apache.beam.sdk.Pipeline pipeline = Pipeline.createTheNewPipeline(options, queue);
        pipeline.run(options).waitUntilFinish();
    }
}
