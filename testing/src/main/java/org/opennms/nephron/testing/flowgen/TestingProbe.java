/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opennms.nephron.testing.flowgen;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/**
 * Allows to monitor a pipeline.
 *
 * Provides a
 *
 * <ul>
 *     <li>an identity transformation that must be inserted into a pipeline</li>
 *     <li>a counter metric that counts the number of elements that are processed by the transformation</li>
 *     <li>a distribution metrics that tracks the minimum and maximum time when processing took place in the transformation</li>
 * </ul>
 *
 * The metrics can be used to calculate the processing rate at the monitored location.
 *
 * <p>
 * Code copied from org.apache.beam.sdk.nexmark.Monitor and org.apache.beam.sdk.testutils.metrics.MetricsReader
 * with slight adaptions.
 */
public class TestingProbe<T> implements Serializable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestingProbe.class);

    private static final long ERRONEOUS_METRIC_VALUE = -1;

    private class MonitorDoFn extends DoFn<T, T> {
        final Counter elementCounter = Metrics.counter(namespace, elemenCounterName);
        final Distribution processingTime = Metrics.distribution(namespace, processingTimeDistributionName);

        @ProcessElement
        public void processElement(ProcessContext c) {
            elementCounter.inc();
            processingTime.update(System.currentTimeMillis());
            c.output(c.element());
        }
    }

    public final String namespace;
    public final String elemenCounterName;
    public final String processingTimeDistributionName;
    private final MonitorDoFn doFn;
    private final PTransform<PCollection<? extends T>, PCollection<T>> transform;

    public TestingProbe(String namespace, String prefix) {
        this.namespace = namespace;
        this.elemenCounterName = prefix + ".elements";
        this.processingTimeDistributionName = prefix + ".processingTime";
        doFn = new MonitorDoFn();
        transform = ParDo.of(doFn);
    }

    public PTransform<PCollection<? extends T>, PCollection<T>> getTransform() {
        return transform;
    }

    /**
     * Return the current value for the element counter, or -1 if can't be retrieved. Note this uses only
     * attempted metrics because some runners don't support committed metrics.
     */
    public long getElementCount(PipelineResult result) {
        MetricQueryResults metrics = result
                .metrics()
                .queryMetrics(
                        MetricsFilter.builder()
                                .addNameFilter(MetricNameFilter.named(namespace, elemenCounterName))
                                .build());
        Iterable<MetricResult<Long>> counters = metrics.getCounters();

        checkIfMetricResultIsUnique(elemenCounterName, counters);

        try {
            MetricResult<Long> metricResult = counters.iterator().next();
            return metricResult.getAttempted();
        } catch (NoSuchElementException e) {
            LOG.error("Failed to get metric {}, from namespace {}", elemenCounterName, namespace);
        }
        return ERRONEOUS_METRIC_VALUE;
    }

    private <T> void checkIfMetricResultIsUnique(String metricName, Iterable<MetricResult<T>> metricResult)
            throws IllegalStateException {

        int resultCount = Iterables.size(metricResult);
        Preconditions.checkState(
                resultCount <= 1,
                "More than one metric result matches name: %s in namespace %s. Metric results count: %s",
                metricName,
                namespace,
                resultCount);
    }

    /**
     * Return start time metric by counting the difference between "now" and min value from a
     * distribution metric.
     */
    public long getStartProcessingTime(PipelineResult result) {
        Iterable<MetricResult<DistributionResult>> timeDistributions = getDistributions(result);
        return getLowestMin(timeDistributions);
    }

    private Long getLowestMin(Iterable<MetricResult<DistributionResult>> distributions) {
        Optional<Long> lowestMin =
                StreamSupport.stream(distributions.spliterator(), true)
                        .map(element -> element.getAttempted().getMin())
                        .filter(TestingProbe::isCredible)
                        .min(Long::compareTo);

        return lowestMin.orElse(ERRONEOUS_METRIC_VALUE);
    }

    public long getEndProcessingTime(PipelineResult result) {
        Iterable<MetricResult<DistributionResult>> timeDistributions = getDistributions(result);
        return getGreatestMax(timeDistributions);
    }

    private Long getGreatestMax(Iterable<MetricResult<DistributionResult>> distributions) {
        Optional<Long> greatestMax =
                StreamSupport.stream(distributions.spliterator(), true)
                        .map(element -> element.getAttempted().getMax())
                        .filter(TestingProbe::isCredible)
                        .max(Long::compareTo);

        return greatestMax.orElse(ERRONEOUS_METRIC_VALUE);
    }


    private Iterable<MetricResult<DistributionResult>> getDistributions(PipelineResult result) {
        MetricQueryResults metrics =
                result
                        .metrics()
                        .queryMetrics(
                                MetricsFilter.builder()
                                        .addNameFilter(MetricNameFilter.named(namespace, processingTimeDistributionName))
                                        .build());
        return metrics.getDistributions();
    }

    public Snapshot takeSnapshot(PipelineResult result) {
        return new Snapshot(
                getElementCount(result),
                getStartProcessingTime(result),
                getEndProcessingTime(result)
        );
    }


    /**
     * timestamp metrics are used to monitor time of execution of transforms. If result timestamp
     * metric is too far from now, consider that metric is erroneous.
     */
    private static boolean isCredible(long value) {
        return (Math.abs(value - System.currentTimeMillis()) <= Duration.standardDays(10000).getMillis());
    }

    public static class Snapshot {
        public final long count;
        public final long start;
        public final long end;

        public Snapshot(long count, long start, long end) {
            this.count = count;
            this.start = start;
            this.end = end;
        }

        public double rate() {
            var ms = end - start;
            return ms == 0 ? 0 : (double)count / ms * 1000;
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
            return count == snapshot.count && start == snapshot.start && end == snapshot.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(count, start, end);
        }
    }

}
