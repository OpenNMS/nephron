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

package org.opennms.nephron.testing.benchmark;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.opennms.nephron.testing.flowgen.FlowGenOptions;

public interface BenchmarkOptions extends FlowGenOptions {

    @Description("Maximum time the input may be idle before it is \"closed\" by advancing its watermark to infinity.")
    @Default.Integer(15)
    Integer getMaxInputIdleSecs();

    void setMaxInputIdleSecs(Integer num);

    @Description("Maximum time a benchmark run may take.")
    @Default.Integer(120)
    Integer getMaxRunSecs();

    void setMaxRunSecs(Integer num);

    @Description("Determines how input is fed into the pipeline. Possible values: MEMORY (default), KAFKA")
    @Default.Enum("MEMORY")
    InputSetup.Seletion getInput();

    void setInput(InputSetup.Seletion value);

}
