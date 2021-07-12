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

import org.apache.beam.sdk.coders.Coder;
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

import com.google.common.base.MoreObjects;

/**
 * A stateful transform that accumulates values of different panes, flushes accumulated values after a given output
 * delay, and assigns unique index numbers to the accumulated output.
 */
public class PaneAccumulator<K, V> extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, KV<Integer, V>>>> {

    private static final Logger LOG = LoggerFactory.getLogger(PaneAccumulator.class);

    private final SerializableBiFunction<V, V, V> combiner;
    private final Duration outputDelay;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;

    public PaneAccumulator(
            SerializableBiFunction<V, V, V> combiner,
            Duration outputDelay,
            Coder<K> keyCoder,
            Coder<V> valueCoder
    ) {
        super("paneCombiner");
        this.combiner = combiner;
        this.outputDelay = outputDelay;
        this.keyCoder = keyCoder;
        this.valueCoder = valueCoder;
    }

    @Override
    public PCollection<KV<K, KV<Integer, V>>> expand(PCollection<KV<K, V>> input) {
        return input.apply(ParDo.of(new PaneCombinerFn()));
    }

    private class PaneCombinerFn extends DoFn<KV<K, V>, KV<K, KV<Integer, V>>> {

        @StateId("key")
        private final StateSpec<ValueState<K>> keyStateSpec = StateSpecs.value(keyCoder);

        @StateId("value")
        private final StateSpec<ValueState<V>> valueStateSpec = StateSpecs.value(valueCoder);

        @StateId("index")
        private final StateSpec<ValueState<Integer>> indexStateSpec = StateSpecs.value();

        @TimerId("output")
        private final TimerSpec outputTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

        @ProcessElement
        public void process(
                ProcessContext ctx,
                BoundedWindow window,
                @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("value") ValueState<V> valueState,
                @TimerId("output") Timer timer
        ) {
            var oldValue = valueState.read();
            V newValue;
            if (oldValue == null) {
                LOG.debug("create state - key: {}; ctx.timestamp: {}; window.maxTimestamp: {}", ctx.element().getKey(), ctx.timestamp(), window.maxTimestamp());
                keyState.write(ctx.element().getKey());
                newValue = ctx.element().getValue();
            } else {
                newValue = combiner.apply(oldValue, ctx.element().getValue());
            }
            LOG.trace("write state - key: {}; ctx.timestamp: {}; window.maxTimestamp: {}; newValue: {}", ctx.element().getKey(), ctx.timestamp(), window.maxTimestamp(), newValue);
            valueState.write(newValue);
            timer/*.withOutputTimestamp(window.maxTimestamp())*/.offset(outputDelay).setRelative();
        }

        @OnTimer("output")
        public void onOutput(
                OnTimerContext ctx,
                BoundedWindow window,
                @AlwaysFetched @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("value") ValueState<V> valueState,
                @AlwaysFetched @StateId("index") ValueState<Integer> indexState
                ) {
            var key = keyState.read();
            var value = valueState.read();
            var idx = MoreObjects.firstNonNull(indexState.read(), 0);
            LOG.trace("output state - key: {}; ctx.timestamp: {}, window.maxTimestamp: {}; idx: {}", key, ctx.timestamp(), window.maxTimestamp(), idx);
            ctx.outputWithTimestamp(KV.of(key, KV.of(idx, value)), window.maxTimestamp());
            valueState.clear();
            indexState.write(++idx);
        }

        @OnWindowExpiration
        public void onExpiration(
                OutputReceiver<KV<K, KV<Integer, V>>> ctx,
                BoundedWindow window,
                @StateId("key") ValueState<K> keyState,
                @AlwaysFetched @StateId("value") ValueState<V> valueState,
                @StateId("index") ValueState<Integer> indexState
        ) {
            var value = valueState.read();
            if (value != null) {
                var kl = keyState.readLater();
                var il = indexState.readLater();
                var key = kl.read();
                var idx = MoreObjects.firstNonNull(il.read(), 0);
                LOG.trace("output expired state - key: {}; window.maxTimestamp: {}; idx: {}", key, window.maxTimestamp(), idx);
                ctx.outputWithTimestamp(KV.of(key, KV.of(idx, value)), window.maxTimestamp());
            }
        }

    }

}
