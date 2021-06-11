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
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.opennms.nephron.coders.FlowDocumentProtobufCoder;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jqwik.api.RandomGenerator;

/**
 * A source of synthetically generated flows.
 *
 * Flow generation can be parameterized by a {@link SourceConfig} instance. Flows are generated deterministically
 * base on the supplied seed value for a random number generator.
 */
public class SyntheticFlowSource extends UnboundedSource<FlowDocument, FlowReader.CheckpointMark> {

    private static final Logger LOG = LoggerFactory.getLogger(SyntheticFlowSource.class);

    /**
     * Creates a transformation that reads from a synthetic flow source.
     */
    public static PTransform<PBegin, PCollection<FlowDocument>> readFromSyntheticSource(SourceConfig sourceConfig) {
        return new PTransform<>() {
            private final Gauge inputDrift = Metrics.gauge("flows", "input_lag");

            @Override
            public PCollection<FlowDocument> expand(PBegin input) {
                return input
                        .apply(Read.from(new SyntheticFlowSource(sourceConfig)))
                        .apply(ParDo.of(
                                new DoFn<FlowDocument, FlowDocument>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {
                                        FlowDocument flow = c.element();
                                        inputDrift.set(System.currentTimeMillis() - flow.getLastSwitched().getValue());
                                        c.output(flow);
                                    }
                                }
                        ));
            }
        };
    }

    private final SourceConfig sourceConfig;

    public SyntheticFlowSource(SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
    }

    @Override
    public List<? extends UnboundedSource<FlowDocument, FlowReader.CheckpointMark>> split(int desiredNumSplits, PipelineOptions options) throws Exception {
        // Note: desiredNumSplits may be larget than the configured parallelism because a single task instance can
        // use several source instances.
        LOG.debug("desired number of splits: " + desiredNumSplits);
        return sourceConfig.split(desiredNumSplits).stream()
                .map(sc -> new SyntheticFlowSource(sc))
                .collect(Collectors.toList());
    }

    @Override
    public UnboundedReader<FlowDocument> createReader(PipelineOptions options, FlowReader.@Nullable CheckpointMark checkpointMark) throws IOException {
        RandomGenerator<FlowDocuments.FlowData> flowData = FlowDocuments.getFlowDataArbitrary(sourceConfig.flowConfig).generator(1000);
        Optional<Instant> previous;
        long startIdx;
        Random random;
        if (checkpointMark == null) {
            LOG.trace("creating initial unbounded reader for {}", options);
            previous = Optional.empty();
            startIdx = sourceConfig.idxOffset;
            random = new Random(sourceConfig.seed);
        } else {
            LOG.trace("resuming unbounded reader from {}", checkpointMark);
            previous = Optional.of(checkpointMark.previous);
            startIdx = checkpointMark.index;
            random = checkpointMark.random;
        }
        Limiter limiter = Limiter.of(sourceConfig.flowsPerSecond);
        return new FlowReader(
                this,
                (rnd, idx) -> FlowDocuments.getFlowDocument(sourceConfig.flowConfig, idx, flowData.next(rnd).value()),
                sourceConfig.timestampPolicyFactory.create(previous),
                sourceConfig.idxInc,
                sourceConfig.maxIdx,
                startIdx,
                random,
                limiter
        );

    }

    @Override
    public Coder<FlowReader.CheckpointMark> getCheckpointMarkCoder() {
        return FlowReader.CheckpointMark.CHECKPOINT_CODER;
    }

    @Override
    public Coder<FlowDocument> getOutputCoder() {
        // TODO swachter: why do we need this? the FlowDocumentProbufCoder was registered with the pipeline
        return new FlowDocumentProtobufCoder();
    }
}
