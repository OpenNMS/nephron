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

package org.opennms.nephron.util;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stateful transform that accumulates values of different panes and flushes accumulated values after a given output
 * delay.
 */
public class PaneAccumulator<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {

    private static final Logger LOG = LoggerFactory.getLogger(PaneAccumulator.class);

    private final SerializableBiFunction<V, V, V> combiner;
    private final Duration accumulationDelay;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    public PaneAccumulator(
            SerializableBiFunction<V, V, V> combiner,
            Duration accumulationDelay,
            Coder<K> keyCoder,
            Coder<V> valueCoder
    ) {
        super("paneAcc");
        this.combiner = combiner;
        this.accumulationDelay = accumulationDelay;
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
        return input.apply(ParDo.of(new PaneCombinerFn())).setCoder(KvCoder.of(keyCoder, valueCoder));
    }

    private class PaneCombinerFn extends DoFn<KV<K, V>, KV<K, V>> {

        private static final String VALUE_STATE_NAME = "value";
        private static final String OUTPUT_TIMER_NAME = "output";

        @StateId(VALUE_STATE_NAME)
        private final StateSpec<ValueState<V>> valueStateSpec = StateSpecs.value(valueCoder);

        @TimerId(OUTPUT_TIMER_NAME)
        private final TimerSpec outputTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        @ProcessElement
        public void process(
                ProcessContext ctx,
                BoundedWindow window,
                @AlwaysFetched @StateId(VALUE_STATE_NAME) ValueState<V> valueState,
                @TimerId(OUTPUT_TIMER_NAME) Timer timer
        ) {
            var oldValue = valueState.read();
            V newValue;
            if (oldValue == null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("create state - key: {}; ctx.timestamp: {}; window.maxTimestamp: {}", ctx.element().getKey(), ctx.timestamp(), window.maxTimestamp());
                }
                newValue = ctx.element().getValue();
                timer.withOutputTimestamp(window.maxTimestamp()).offset(accumulationDelay).setRelative();
            } else {
                newValue = combiner.apply(oldValue, ctx.element().getValue());
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("write state - key: {}; ctx.timestamp: {}; window.maxTimestamp: {}; newValue: {}", ctx.element().getKey(), ctx.timestamp(), window.maxTimestamp(), newValue);
            }
            valueState.write(newValue);
        }

        @OnTimer(OUTPUT_TIMER_NAME)
        public void onOutput(
                OnTimerContext ctx,
                BoundedWindow window,
                @Key K key,
                @AlwaysFetched @StateId(VALUE_STATE_NAME) ValueState<V> valueState
                ) {
            var value = valueState.read();
            if (LOG.isTraceEnabled()) {
                LOG.trace("output state - key: {}; ctx.timestamp: {}, window.maxTimestamp: {}", key, ctx.timestamp(), window.maxTimestamp());
            }
            ctx.outputWithTimestamp(KV.of(key, value), window.maxTimestamp());
            valueState.clear();
        }

        @OnWindowExpiration
        public void onExpiration(
                OutputReceiver<KV<K, V>> ctx,
                BoundedWindow window,
                @Key K key,
                @AlwaysFetched @StateId(VALUE_STATE_NAME) ValueState<V> valueState
        ) {
            var value = valueState.read();
            if (value != null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("output expired state - key: {}; window.maxTimestamp: {}", key, window.maxTimestamp());
                }
                ctx.outputWithTimestamp(KV.of(key, value), window.maxTimestamp());
            }
        }

    }

}
