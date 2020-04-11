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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.joda.time.Duration;

public class DurationUtils {

    private static final Pattern DURATION_REGEX = Pattern.compile("^([0-9]+)([a-z]+)$");

    private static final LoadingCache<String, Long> durations = CacheBuilder.newBuilder()
            .maximumSize(100)
            .build(
                    new CacheLoader<String, Long>() {
                        @Override
                        public Long load(String key) {
                            final Matcher m = DURATION_REGEX.matcher(key.trim().toLowerCase());
                            if (!m.matches()) {
                                throw new IllegalArgumentException("Invalid duration: " + key);
                            }

                            long value = Long.parseLong(m.group(1));
                            String unit = m.group(2);

                            long multiplier;
                            switch(unit) {
                                case "ms":
                                    multiplier = TimeUnit.MILLISECONDS.toMillis(1);
                                    break;
                                case "s":
                                    multiplier = TimeUnit.SECONDS.toMillis(1);
                                    break;
                                case "m":
                                    multiplier = TimeUnit.MINUTES.toMillis(1);
                                    break;
                                case "h":
                                    multiplier = TimeUnit.HOURS.toMillis(1);
                                    break;
                                case "d":
                                    multiplier = TimeUnit.DAYS.toMillis(1);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unsupported unit: " + unit + " in duration: " + key);
                            }

                            return value * multiplier;
                        }
                    });

    public static long toMillis(String duration) {
        Objects.requireNonNull(duration, "duration cannot be null.");
        return durations.getUnchecked(duration);
    }

    public static Duration toDuration(String duration) {
        return Duration.millis(toMillis(duration));
    }
}
