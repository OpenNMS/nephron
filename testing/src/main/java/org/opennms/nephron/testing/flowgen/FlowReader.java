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

package org.opennms.nephron.testing.flowgen;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.BiFunction;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows to repeatedly advance to the next flow and gives access to the current watermark.
 *
 * Created by a {@link SyntheticFlowSource} for reading flows.
 */
public class FlowReader extends UnboundedSource.UnboundedReader<FlowDocument> {

    private static Logger LOG = LoggerFactory.getLogger(FlowReader.class);

    private final UnboundedSource<FlowDocument, CheckpointMark> source;
    private final BiFunction<Random, Long, FlowDocument> nextFlowDocument;
    private final SyntheticFlowTimestampPolicy timestampPolicy;
    private final int idxIncr;
    private final long maxIdx;
    protected long index;
    private Random random;

    private FlowDocument current = null;

    private final Limiter limiter;

    public FlowReader(
            UnboundedSource<FlowDocument, CheckpointMark> source,
            BiFunction<Random, Long, FlowDocument> nextFlowDocument,
            SyntheticFlowTimestampPolicy timestampPolicy,
            int idxIncr,
            long maxIdx,
            long index,
            Random random,
            Limiter limiter
    ) {
        this.source = source;
        this.nextFlowDocument = nextFlowDocument;
        this.timestampPolicy = timestampPolicy;
        this.idxIncr = idxIncr;
        this.maxIdx = maxIdx;
        this.index = index;
        this.random = random;
        this.limiter = limiter;
    }

    @Override
    public boolean start() throws IOException {
        return advance();
    }

    @Override
    public boolean advance() throws IOException {
        getWatermark();
        if (index < maxIdx) {
            if (limiter.check(idxIncr)) {
                current = nextFlowDocument.apply(random, index);
                index += idxIncr;
                // may update the watermark contained in the timestamp policy
                timestampPolicy.getTimestampForFlow(current);
                return true;
            } else {
                return false;
            }
        } else {
            current = null;
            return false;
        }
    }

    @Override
    public Instant getWatermark() {
        Instant wm = index < maxIdx ? timestampPolicy.getWatermark() : BoundedWindow.TIMESTAMP_MAX_VALUE;
        return wm;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        return new CheckpointMark(timestampPolicy.getCheckpointInstant(), index, random, limiter.state());
    }

    @Override
    public UnboundedSource<FlowDocument, ?> getCurrentSource() {
        return source;
    }

    @Override
    public FlowDocument getCurrent() throws NoSuchElementException {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return timestampPolicy.getTimestampForFlow(current);
    }

    @Override
    public void close() throws IOException {

    }

    public static class CheckpointMark implements UnboundedSource.CheckpointMark {

        private static Coder<Instant> INSTANT_CODER = InstantCoder.of();
        private static Coder<Long> LONG_CODER = VarLongCoder.of();
        private static Coder<Random> RANDOM_CODER = SerializableCoder.of(Random.class);

        public static Coder<CheckpointMark> CHECKPOINT_CODER = new CustomCoder<CheckpointMark>() {

            @Override
            public void encode(CheckpointMark value, OutputStream outStream) throws CoderException, IOException {
                INSTANT_CODER.encode(value.previous, outStream);
                LONG_CODER.encode(value.index, outStream);
                RANDOM_CODER.encode(value.random, outStream);
                LONG_CODER.encode(value.limiterState, outStream);
            }

            @Override
            public CheckpointMark decode(InputStream inStream) throws CoderException, IOException {
                return new CheckpointMark(
                        INSTANT_CODER.decode(inStream),
                        LONG_CODER.decode(inStream),
                        RANDOM_CODER.decode(inStream),
                        LONG_CODER.decode(inStream)
                );
            }
        };

        // previous watermark or previous max event timestamp (depending on SyntheticFlowTimestampPolicy)
        public final Instant previous;
        public final long index;
        public final Random random;
        public final long limiterState;

        public CheckpointMark(Instant previous, long index, Random random, long limiterState) {
            this.previous = previous;
            this.index = index;
            this.random = random;
            this.limiterState = limiterState;
        }

        @Override
        public void finalizeCheckpoint() throws IOException {

        }
    }

}
