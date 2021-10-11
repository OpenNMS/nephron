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

import static org.opennms.nephron.Pipeline.accumulateSummariesIfNecessary;
import static org.opennms.nephron.Pipeline.attachWriteToCortex;
import static org.opennms.nephron.Pipeline.attachWriteToElastic;
import static org.opennms.nephron.Pipeline.registerCoders;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.nephron.Aggregate;
import org.opennms.nephron.CompoundKey;
import org.opennms.nephron.Pipeline;
import org.opennms.nephron.cortex.CortexIo;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the pipeline on generated synthetic flows and drop the results.
 * <p>
 * See the {@link CmdLineArgsProcessor} class for a details description of the possible command line arguments.
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

    // --runner=FlinkRunner
    // --flinkMaster=localhost:8081
    // --flowsPerWindow=100
    // --flowsPerSecond=10000
    // --cortexWriteUrl=http://localhost:9009/api/v1/push
    //
    // e.g.: --runner=FlinkRunner --flinkMaster=localhost:8081 --numWindows=20 --fixedWindowSizeMs=10000 --earlyProcessingDelayMs=5000 --lateProcessingDelayMs=5000 --playbackMode=false --elasticUrl=http://localhost:9200 --cortexWriteUrl=http://localhost:9009/api/v1/push --parallelism=4 --flowsPerSecond=8000 --lastSwitchedSigmaMs=5000 --numExporters=2 --numInterfaces=2 --numProtocols=1 --numApplications=2 --numHosts=2 --numEcns=1 --numDscps=1 --cortexMaxBatchSize=100
    //
    // start Cortex in container:
    // ====
    // docker run -d --name cortex -v /home/swachter/projects/opennms/nephron/cortex/src/test/resources/cortex.yaml:/etc/cortex/cortex.yaml -p 9009:9009 -p 9005:9005 cortexproject/cortex:v1.9.0 -config.file=/etc/cortex/cortex.yaml

    public static void main(String[] args) throws Exception {
        args = ensureArg("--blockOnRun=false", args);
        args = ensureArg("--runner=FlinkRunner", args);

        CmdLineArgsProcessor.CmdLineArgs cmdLineArgs = CmdLineArgsProcessor.process(args);
        var paramLists = cmdLineArgs.args.eval().expand();

        Consumer<String> resultConsumer;
        if (cmdLineArgs.out == null) {
            resultConsumer = msg -> System.out.println(msg);
        } else {
            var fileWriter = new PrintStream(cmdLineArgs.out, StandardCharsets.UTF_8);
            resultConsumer = msg -> {
                System.out.println(msg);
                fileWriter.println(msg);
            };
        }

        Set<String> commonParameters = commonParameters(paramLists);

        resultConsumer.accept(String.format("Started benchmark run at          : %s", Instant.now()));
        resultConsumer.accept(String.format("Number of pipeline argument lists : %d", paramLists.size()));
        resultConsumer.accept(String.format("common pipeline arguments         : %s", commonParameters.stream().sorted().collect(Collectors.joining(" "))));

        var counter = 1;

        var executor = Executors.newSingleThreadExecutor();

        try {
            for (var paramList : paramLists) {
                executeCommand(cmdLineArgs.before);
                var as = paramList.toArray(new String[0]);
                as = ensureArg("--blockOnRun=false", as);
                as = ensureArg("--runner=FlinkRunner", as);
                // the benchmark may write to ES; disabled by default (override default from NephronOptions)
                as = ensureArg("--elasticUrl=", as);
                var options = PipelineOptionsFactory.fromArgs(as).withValidation().as(BenchmarkOptions.class);
                var pl = new ArrayList(paramList);
                pl.removeAll(commonParameters);
                resultConsumer.accept("=".repeat(30));
                resultConsumer.accept(String.format("start pipeline run [%d/%d] at : %s", counter, paramLists.size(), Instant.now()));
                resultConsumer.accept(String.format("varying pipeline arguments : %s", pl.stream().sorted().collect(Collectors.joining(" "))));
                // The Apache beam flink runner automatically determines the jars / resources necessary for running the pipeline
                // (cf. org.apache.beam.runners.flink.FlinkPipelineExecutionEnvironment#prepareFilesToStageForRemoteClusterExecution)
                // -> this is done by considering involved class loaders
                // -> in particular, the call stack is traversed to consider the class loaders of all classes on the call stack
                //    (cf. nonapi.io.github.classgraph.classpath.ClassLoaderFinder)
                // -> when the benchmark is run by maven then the maven exec plugin is on the call stack
                // -> this would result in all maven classes being added as dependencies to the Flink job
                // -> run the pipeline in a separate thread (thereby removing maven from the call stack)
                var f = executor.submit(() -> {
                    try {
                        new Benchmark(options, resultConsumer).run();
                    } catch (Exception e) {
                        LOG.error("benchmark failed", e);
                        throw new RuntimeException(e);
                    }
                });
                f.get();
                executeCommand(cmdLineArgs.after);
                if (counter < paramLists.size() && options.getSleepBetweenRunsMs() > 0) {
                    Thread.sleep(options.getSleepBetweenRunsMs());
                }
                counter++;
            }
        } finally {
            executor.shutdown();
        }
    }

    public static Set<String> commonParameters(List<List<String>> paramLists) {
        // first collect all parameters
        var commonParameters = paramLists.stream().flatMap(l -> l.stream()).collect(Collectors.toSet());
        // then retain the parameters that are contained in all parameter lists
        paramLists.forEach(commonParameters::retainAll);
        return commonParameters;
    }

    private static void executeCommand(String command) throws Exception {
        if (command != null) {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            Process p = pb.start();
            var exitCode = p.waitFor();
            if (exitCode != 0) {
                throw new Exception("command returned non-zero exit code - exitCode: " + exitCode + "; command: " + command);
            }
        }
    }

    /**
     * Adds an argument assignment to the given args array if that argument is not yet set.
     */
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
                return a.substring(leftHandSide.length());
            }
        }
        return null;
    }

    private final BenchmarkOptions options;
    private final Consumer<String> resultConsumer;
    private final TestingProbe<FlowDocument> inTestingProbe = new TestingProbe<>("benchmark", "in");
    private final TestingProbe<KV<CompoundKey, Aggregate>> outTestingProbe = new TestingProbe<>("benchmark", "out");
    private final Instant start = Instant.now();

    private PipelineResult pipelineResult;
    private TerminationReason terminationReason;

    private Benchmark(BenchmarkOptions options, Consumer<String> resultConsumer) {
        this.options = options;
        this.resultConsumer = resultConsumer;
    }

    public void run() throws Exception {
        org.apache.beam.sdk.Pipeline pipeline = org.apache.beam.sdk.Pipeline.create(options);
        registerCoders(pipeline);

        var inputSetup = options.getInput().createInputSetup(options);

        var input = pipeline
                .apply(inputSetup.source())
                .apply(inTestingProbe.getTransform());

        var flowSummariesAndHostnames = Pipeline.calculateFlowStatistics(input, options);

        var flowSummaries = flowSummariesAndHostnames.getLeft();

        flowSummaries = accumulateSummariesIfNecessary(options, flowSummaries);

        flowSummaries = flowSummaries.apply(outTestingProbe.getTransform());

        attachWriteToElastic(options, flowSummaries, flowSummariesAndHostnames.getRight());
        // add an additional label that differentiates benchmark runs
        // -> ensures that samples of different runs do not conflict
        //    (Cortex's sample time ordering constraint could be violated because the EventTimestampIndexer logic is started anew for each run)
        // -> use the current time as a distinguishing value
        attachWriteToCortex(options, flowSummaries, cw -> cw.withFixedLabel("bmr", Instant.now().toString()));

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

        resultConsumer.accept("Benchmark result");
        resultConsumer.accept(String.format("termination          : %s", terminationReason != null ? terminationReason.name() : "<unknown>"));
        resultConsumer.accept(String.format("input count          : %d", benchmarkResult.snapshot.in.count));
        resultConsumer.accept(String.format("expected input count : %d", inputSetup.sourceConfig.maxIdx));
        resultConsumer.accept(String.format("output count         : %d", benchmarkResult.snapshot.out.count));
        resultConsumer.accept(String.format("cortex samples       : %d", benchmarkResult.snapshot.cortexSamples));
        resultConsumer.accept(String.format("input  rate     : %.2f (1/s)", benchmarkResult.inRate));
        resultConsumer.accept(String.format("output rate     : %.2f (1/s)", benchmarkResult.outRate));
        resultConsumer.accept(String.format("processing time (first input until last output) : %.2f s", (double)benchmarkResult.processingTime.getMillis() / 1000));

    }

    private void cancel(TerminationReason tr) throws Exception {
        if (terminationReason == null) {
            this.terminationReason = tr;
            pipelineResult.cancel();
        }
    }
    private BenchmarkResult waitUntilFinished(long maxIdx) throws Exception {
        var lastActivityAt = Instant.now();
        Snapshot lastSnapshot = null;
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

    private Snapshot takeSnapshot() {
        return new Snapshot(
                inTestingProbe.takeSnapshot(pipelineResult),
                outTestingProbe.takeSnapshot(pipelineResult),
                TestingProbe.getElementCount(CortexIo.CORTEX_METRIC_NAMESPACE, CortexIo.CORTEX_SAMPLE_METRIC_NAME, pipelineResult)
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

    public static class Snapshot {
        public final TestingProbe.Snapshot in, out;
        public final long cortexSamples;

        public Snapshot(TestingProbe.Snapshot in, TestingProbe.Snapshot out, long cortexSamples) {
            this.in = in;
            this.out = out;
            this.cortexSamples = cortexSamples;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Snapshot snapshot = (Snapshot) o;
            return cortexSamples == snapshot.cortexSamples && Objects.equals(in, snapshot.in) && Objects.equals(out, snapshot.out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(in, out, cortexSamples);
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
