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

package org.opennms.nephron.testing.benchmark;

import static org.opennms.nephron.Pipeline.registerCoders;

import java.util.Objects;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.FlowSummaryData;
import org.opennms.nephron.NephronOptions;
import org.opennms.nephron.Pipeline;
import org.opennms.nephron.testing.flowgen.FlowGenOptions;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the pipeline on generated synthetic flows and drop the results.
 */
public class Benchmark {

    private static Logger LOG = LoggerFactory.getLogger(Benchmark.class);

    /**
     * How long to let streaming pipeline run after all events have been generated and we've seen no
     * activity.
     */
    private static final Duration DONE_DELAY = Duration.standardSeconds(15);

    /** Delay between perf samples. */
    private static final Duration PERF_DELAY = Duration.standardSeconds(15);

    // --runner=FlinkRunner --flinkMaster=localhost:8081
    // --flowsPerWindow=100
    // --flowsPerSecond=10000
    public static void main(String[] args) throws Exception {
        args = ensureArg("--blockOnRun=false", args);
        PipelineOptionsFactory.register(NephronOptions.class);
        PipelineOptionsFactory.register(FlowGenOptions.class);
        var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BenchmarkOptions.class);
        new Benchmark(options).run();
    }

    private static String[] ensureArg(String argAssignment, String[] args) {
        if (argValue(argAssignment, args) != null) {
            return args;
        } else {
            var newArgs = new String[args.length + 1];
            System.arraycopy(args, 0, newArgs, 0, args.length);
            newArgs[args.length] = argAssignment;
            return newArgs;
        }
    }

    private static String argValue(String arg, String[] args) {
        String leftHandSide = arg.substring(0, arg.indexOf('=') + 1);
        for (String a: args) {
            if (a.startsWith(leftHandSide)) {
                return a.substring(leftHandSide.length() + 1);
            }
        }
        return null;
    }

    private final BenchmarkOptions options;
    private final TestingProbe<FlowDocument> inTestingProbe = new TestingProbe<>("benchmark", "in");
    private final TestingProbe<FlowSummaryData> outTestingProbe = new TestingProbe<>("benchmark", "out");
    private final Instant start = Instant.now();

    private PipelineResult pipelineResult;
    private TerminationReason terminationReason;

    private Benchmark(BenchmarkOptions options) {
        this.options = options;
    }

    public void run() throws Exception {
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(pipeline);

        var inputSetup = options.getInput().createInputSetup(options);

        PCollection<FlowSummaryData> flowSummaries = pipeline
                .apply(inputSetup.source())
                .apply(inTestingProbe.getTransform())
                .apply(new Pipeline.CalculateFlowStatistics(options))
                .apply(outTestingProbe.getTransform());

        flowSummaries.apply(devNull("summaries"));

        if (FlinkRunner.class.isAssignableFrom(options.getRunner()) && options.getInput() == InputSetup.Seletion.KAFKA) {

            // Unfortunately FlinkRunner blocks pipeline.run()
            //
            // -> Input has to be generated in a separate thread
            // -> In addition, the kafka source must be tweaked to advance the watermark to infinity after the
            //    expected number of elements were read
            // -> this completes the pipeline and pipeline.run() returns
            //
            // -> TODO: https://issues.apache.org/jira/browse/BEAM-12477

            var thread = new Thread(() -> {
                try {
                    // wait some time until pipeline is ready
                    // -> Kafka records that are published too early are ignored
                    LOG.info("wait some time until pipeline is started...");
                    Thread.sleep(20000);
                    LOG.info("generate input...");
                    inputSetup.generate();
                } catch (Exception e) {
                    LOG.error("input generation failed", e);
                }
            });

            thread.start();

            pipelineResult = pipeline.run();
        } else {
            LOG.info("run pipeline...");
            pipelineResult = pipeline.run();
            LOG.info("generate input...");
            inputSetup.generate();
        }

        var benchmarkResult = waitUntilFinished(inputSetup.sourceConfig.maxIdx);

        LOG.info("benchmark finished");

        System.out.println("Benchmark result");
        System.out.println(String.format("input                : %s", options.getInput().name()));
        System.out.println(String.format("termination          : %s", terminationReason != null ? terminationReason.name() : "<unknown>"));
        System.out.println(String.format("input count          : %d", benchmarkResult.snapshot.in.count));
        System.out.println(String.format("expected input count : %d", inputSetup.sourceConfig.maxIdx));
        System.out.println(String.format("output count         : %d", benchmarkResult.snapshot.out.count));
        System.out.println(String.format("input  rate     : %.2f (1/s)", benchmarkResult.inRate));
        System.out.println(String.format("output rate     : %.2f (1/s)", benchmarkResult.outRate));
        System.out.println(String.format("processing time (first input until last output) : %.2f s", (double)benchmarkResult.processingTime.getMillis() / 1000));

    }

    private void cancel(TerminationReason tr) throws Exception {
        if (terminationReason == null) {
            this.terminationReason = tr;
            pipelineResult.cancel();
        }
    }
    private BenchmarkResult waitUntilFinished(long maxIdx) throws Exception {
        var lastActivityAt = Instant.now();
        InOutSnapshot lastSnapshot = null;
        Instant maxIdxReachedAt = null;
        running: while(terminationReason == null) {

            final Instant now = Instant.now();
            var elapsed = new Duration(start, now);

            var nextSnapshot = takeSnapshot();
            if (lastSnapshot == null || !lastSnapshot.equals(nextSnapshot)) {
                lastActivityAt = now;
                lastSnapshot = nextSnapshot;
            }

            if (elapsed.getStandardSeconds() >= options.getMaxRunSecs()) {
                cancel(TerminationReason.TIMEOUT);
            }

            if (maxIdxReachedAt == null && lastSnapshot.in.count >= maxIdx) {
                maxIdxReachedAt = now;
            } else {
                var maxIdxReachedFor = new Duration(maxIdxReachedAt, now);
                if (maxIdxReachedFor.isLongerThan(DONE_DELAY)) {
                    cancel(TerminationReason.EXPECTED_INPUT_PROCESSED);
                }
            }

            Duration quietFor = new Duration(lastActivityAt, now);
            if (quietFor.isLongerThan(DONE_DELAY)) {
                cancel(TerminationReason.STARVED);
            }

            var state = pipelineResult.getState();
            switch (state) {
                case UNKNOWN:
                case UNRECOGNIZED:
                case STOPPED:
                case RUNNING:
                    // Keep going.
                    break;
                case DONE:
                    terminationReason = TerminationReason.DONE;
                    break running;
                case CANCELLED:
                    if (terminationReason == null) {
                        LOG.error("Job was unexpectedly cancelled");
                        terminationReason = TerminationReason.UNEXPECTED;
                    }
                    break running;
                case FAILED:
                    terminationReason = TerminationReason.FAILED;
                    break running;
                case UPDATED:
                    terminationReason = TerminationReason.UPDATED;
                    LOG.error("Job was unexpectedly updated");
                    break running;
            }

            LOG.debug(String.format("input count : %d", lastSnapshot.in.count));
            LOG.debug(String.format("output count: %d", lastSnapshot.out.count));

            Thread.sleep(PERF_DELAY.getMillis());
        }

        return new BenchmarkResult(lastSnapshot);
    }

    private InOutSnapshot takeSnapshot() {
        return new InOutSnapshot(
                inTestingProbe.takeSnapshot(pipelineResult),
                outTestingProbe.takeSnapshot(pipelineResult)
        );
    }

    /** Return a transform to count and discard each element. */
    public static <T> ParDo.SingleOutput<T, Void> devNull(final String name) {
        return ParDo.of(
                new DoFn<T, Void>() {
                    final Counter discardedCounterMetric = Metrics.counter(name, "discarded");

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        discardedCounterMetric.inc();
                    }
                });
    }

    public static class InOutSnapshot {
        public final TestingProbe.Snapshot in, out;

        public InOutSnapshot(TestingProbe.Snapshot in, TestingProbe.Snapshot out) {
            this.in = in;
            this.out = out;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InOutSnapshot that = (InOutSnapshot) o;
            return Objects.equals(this.in, that.in) && Objects.equals(this.out, that.out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(in, out);
        }
    }

    enum TerminationReason {
        DONE,
        TIMEOUT,
        STARVED,
        EXPECTED_INPUT_PROCESSED,
        UNEXPECTED,
        FAILED,
        UPDATED
    }

}
