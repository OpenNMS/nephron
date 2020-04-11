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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.TimeUnit;

import org.joda.time.Duration;
import org.junit.Test;

public class DurationUtilsTest {

    @Test
    public void canConvertStringToMillis() {
        assertThat(DurationUtils.toMillis("1ms"), equalTo(TimeUnit.MILLISECONDS.toMillis(1)));
        assertThat(DurationUtils.toMillis("1s"), equalTo(TimeUnit.SECONDS.toMillis(1)));
        assertThat(DurationUtils.toMillis("1m "), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(DurationUtils.toMillis(" 1h"), equalTo(TimeUnit.HOURS.toMillis(1)));
        assertThat(DurationUtils.toMillis("1d"), equalTo(TimeUnit.DAYS.toMillis(1)));
    }

    @Test
    public void canConvertStringToDuration() {
        assertThat(DurationUtils.toDuration("1ms"), equalTo(Duration.millis(1)));
        assertThat(DurationUtils.toDuration("1m"), equalTo(Duration.millis(TimeUnit.MINUTES.toMillis(1))));
    }
}
