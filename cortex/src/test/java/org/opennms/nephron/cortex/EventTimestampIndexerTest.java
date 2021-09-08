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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.joda.time.Instant;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

public class EventTimestampIndexerTest {

    @Property
    public void test(
        @ForAll("eventTimestamps") List<Instant> eventTimestamps
    ) {
        var flushedEventTimestamps = new EventTimestampIndexer();
        var indexes = new HashMap<Instant, Integer>();
        var timestamps = new HashMap<Integer, Instant>();

        var eventTimestampsWithIndexes = eventTimestamps
                .stream()
                .map(ets -> Tuple.of(ets, flushedEventTimestamps.findIndex(ets)))
                .collect(Collectors.toList());

        for (var t: eventTimestampsWithIndexes) {
            // check that indexes are increasing for each timestamp
            final var nextTimestamp = t.get1();
            final var nextIndex = t.get2();
            var lastIndexForTimestamp = indexes.get(nextTimestamp);
            if (lastIndexForTimestamp != null) {
                assertThat(nextIndex, Matchers.greaterThan(lastIndexForTimestamp));
            }
            // check that timestamps are increasing for each index
            var lastTimestampForIndex = timestamps.get(nextIndex);
            if (lastTimestampForIndex != null) {
                assertThat(nextTimestamp, Matchers.greaterThan(lastTimestampForIndex));
            }
            // check that no smaller index could be used
            for (int i = 0; i < nextIndex; i++) {
                var flushedTimestamp = timestamps.get(i);
                // assert that the smaller index has been used (was flushed) ...
                assertThat(flushedTimestamp, notNullValue());
                // ... and that the next timestamp is not after that timestamp
                assertThat(nextTimestamp, lessThanOrEqualTo(flushedTimestamp));
            }
            indexes.put(nextTimestamp, nextIndex);
            timestamps.put(nextIndex, nextTimestamp);
        }
    }

    private static Arbitrary<Instant> EVENT_TIMESTAMP = Arbitraries.integers().between(0, 5).map(s -> Instant.ofEpochSecond(s));

    @Provide
    private Arbitrary<List<Instant>> eventTimestamps() {
        return EVENT_TIMESTAMP.list().ofMinSize(1).ofMaxSize(10);
    }
}
