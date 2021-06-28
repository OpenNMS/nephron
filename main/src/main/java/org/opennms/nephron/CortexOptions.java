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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CortexOptions extends PipelineOptions {

    @Description("Write URL for cortex (e.g. http://cortex:9009/api/v1/push). Setting this URL switches on Cortex persistence.")
    String getCortexWriteUrl();
    void setCortexWriteUrl(String value);

    @Description("OrgId that is set in the X-Scope-OrgID header.")
    String getCortexOrgId();
    void setCortexOrgId(String value);

    @Description("The maximum number of batched samples.")
    @Default.Integer(10000)
    int getCortexMaxBatchSize();
    void setCortexMaxBatchSize(int value);

    @Description("The maximum number of bytes sent in one batch. (The actual number of sent bytes may be slightly larger.)")
    @Default.Integer(512 * 1024)
    int getCortexMaxBatchBytes();
    void setCortexMaxBatchBytes(int value);

    @Description("Accumulation delay before samples are output to Cortex. Cortex accumulation is switched off if set to zero.")
    @Default.Long(0)
    long getCortexAccumulationDelayMs();
    void setCortexAccumulationDelayMs(long value);

    @Description("The addresses of those hosts that are included in the output of aggregations for hosts. The value is comma-separated list of addresses and address ranges. Host names and CIDR notation are supported.")
    @Default.String("")
    String getCortexConsideredHosts();
    void setCortexConsideredHosts(String value);

}
