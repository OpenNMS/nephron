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

package org.opennms.nephron.flowgen;


import static org.joda.time.Duration.standardSeconds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.nephron.Pipeline;

public class AggregationModeTest {

//    TestFlinkRunner runner = TestFlinkRunner.create(false);
//
//    final PipelineOptions options = runner.getPipelineOptions();

    @Rule
    public TestPipeline p = TestPipeline
//            .create();
            .fromOptions(PipelineOptionsFactory.fromArgs("--runner=TestFlinkRunner").create());
//            .fromOptions(runner.getPipelineOptions());

    // use a static list in order to prevent it from being serialized
    private static List<Integer> additionResults = Collections.synchronizedList(new ArrayList<>());

    @Before
    public void setUp() {
        Pipeline.registerCoders(p);
    }

    @Test
    public void test() {
        Duration windowDur = standardSeconds(60);
        int allowedLatenessWindows = 60;
        Duration allowedLateness = windowDur.multipliedBy(allowedLatenessWindows);

        Instant start = Instant.ofEpochMilli(1500000000000L);

        class TestStreamBuilder<T> {

            private Instant watermark;
            private TestStream.Builder<T> builder;

            public TestStreamBuilder(Instant watermark, Coder<T> coder) {
                this.watermark = watermark;
                builder = TestStream.create(coder).advanceWatermarkTo(start);
            }

            protected Function<Instant, Instant> at(Instant t) {
                return x -> t;
            }

            protected Function<Instant, Instant> after(Duration d) {
                return x -> x.plus(d);
            }

            protected Function<Instant, Instant> atNextWindow(Duration windowDuration) {
                long millis = windowDuration.getMillis();
                return x -> Instant.ofEpochMilli((x.getMillis() / millis + 1) * millis);
            }

            public TestStreamBuilder repeat(T value, int num, Function<Instant, Instant> when) {
                for (int i = 0; i < num; i++) next(value, when);
                return this;
            }

            public TestStreamBuilder next(T value) {
                return next(value, t -> t);
            }

            public TestStreamBuilder next(T value, Function<Instant, Instant> when) {
                Instant t = when.apply(watermark);
                builder = builder.addElements(TimestampedValue.of(value, t));
                watermark(t);
                return this;
            }

            protected TestStreamBuilder watermark(Function<Instant, Instant> when) {
                return watermark(when.apply(watermark));
            }

            protected TestStreamBuilder watermark(Instant t) {
                if (t.isAfter(watermark)) {
                    builder = builder.advanceWatermarkTo(t);
                    watermark = t;
                }
                return this;
            }

            public TestStream<T> advanceWatermarkToInfinity() {
                return builder.advanceWatermarkToInfinity();
            }

        }

        TestStream<Integer> stream = new TestStreamBuilder<Integer>(start, NullableCoder.of(VarIntCoder.of())) {{
                // 1 and 10 arrive in time (for the first window)
                next(1);
                next(10, after(standardSeconds(1)));
                // set watermark to the start of next window
                watermark(atNextWindow(windowDur));
                // late data arrives for the first window
                next(100, at(start));
                // advance watermark after the allowed lateness for the first window has passed
                watermark(after(allowedLateness));
                // data for the first window that should be discarded
                next(1000, at(start));

        }}.advanceWatermarkToInfinity();

        Window<Integer> window = Window.<Integer>into(FixedWindows.of(windowDur))
                .triggering(
                        AfterWatermark
                                .pastEndOfWindow()
                                .withLateFirings(
                                        AfterProcessingTime
                                                .pastFirstElementInPane()
                                                .plusDelayOf(standardSeconds(180))
                                )
                )
                .withAllowedLateness(allowedLateness)
                .accumulatingFiredPanes();

        PCollection<Integer> sum1 = p
                .apply(stream)
                .apply(window)
                .apply(Combine.globally(ADD_INTS).withoutDefaults());

        PCollection<?> sum2 = sum1
                .apply(Combine.globally(ADD_INTS).withoutDefaults())
//                .apply(WithKeys.<Integer, Integer>of(i -> 1).withKeyType(TypeDescriptors.integers()))
//                .apply(Combine.perKey(ADD_INTS))
                ;

        IntervalWindow firstWindow = new IntervalWindow(start, windowDur);

        PAssert.that(sum1).inWindow(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum1-inWindow: " + i);
            }
            return null;

        });

        PAssert.that(sum1).inOnTimePane(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum1-onTime: " + i);
            }
            return null;
        });

        PAssert.that(sum1).inLatePane(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum1-late: " + i);
            }
            return null;
        });

        PAssert.that(sum2).inWindow(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum2-inWindow: " + i);
            }
            return null;

        });

        PAssert.that(sum2).inOnTimePane(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum2-onTime: " + i);
            }
            return null;
        });

        PAssert.that(sum2).inLatePane(firstWindow).satisfies(iter -> {
            for (Object i : iter) {
                System.out.println("res-sum2-late: " + i);
            }
            return null;
        });

        p.run();

        // 1 + 10: onTime for sum1
        // 11 + 100: latePane for sum1
        // 11 + 111: latePane for sum2
        Assert.assertEquals(1, additionResults.stream().filter(s -> s == 11).count());
        Assert.assertEquals(1, additionResults.stream().filter(s -> s == 111).count());
        Assert.assertEquals(1, additionResults.stream().filter(s -> s == 122).count());
    }

    private static SerializableBiFunction<Integer, Integer, Integer> ADD_INTS = (a, b) -> {
        int res = a + b;
        additionResults.add(res);
        return res;
    };

}
