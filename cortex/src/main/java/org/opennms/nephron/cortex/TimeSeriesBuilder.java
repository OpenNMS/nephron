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

package org.opennms.nephron.cortex;

/**
 * Allows to add labels and samples to an underlying protobuf builder.
 * <p>
 * The metric name is just a special kind of label. Therefore {@link #setMetricName(String)} should be called
 * only once.
 * <p>
 * Label names and metric names are sanitized according to Cortex requirements.
 */
public interface TimeSeriesBuilder {
    TimeSeriesBuilder setMetricName(String name);

    TimeSeriesBuilder addLabel(String name, String value);

    TimeSeriesBuilder addSample(long epochMillis, double value);

    /**
     * Starts another time series.
     * <p>
     * Clients of a time series builder may need to output samples for different time series.
     * In that case {@code nextSeries} must be called in-between.
     */
    TimeSeriesBuilder nextSeries();

    default TimeSeriesBuilder addLabel(String name, int value) {
        return addLabel(name, String.valueOf(value));
    }

    default TimeSeriesBuilder addLabel(String name, Integer value) {
        return addLabel(name, String.valueOf(value));
    }

}
