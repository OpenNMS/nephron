package org.opennms.nephron;

import java.util.Optional;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Maintain the watermark at the (largest event time - acceptable delay) and ignore event times that are in the future
 * relative to the system clock.
 */
public class MyCustomTimestampPolicyWithLimitedDelay<K, V> extends TimestampPolicy<K, V> {
    private final Duration maxDelay;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFunction;
    private Instant maxEventTimestamp;

    public MyCustomTimestampPolicyWithLimitedDelay(SerializableFunction<KafkaRecord<K, V>, Instant> timestampFunction,
                                                   Duration maxDelay, Optional<Instant> previousWatermark) {
        this.maxDelay = maxDelay;
        this.timestampFunction = timestampFunction;
        this.maxEventTimestamp = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE).plus(maxDelay);
    }

    @Override
    public Instant getTimestampForRecord(final PartitionContext ctx, final KafkaRecord<K, V> record) {
        return getTimestampForRecord(ctx, record, Instant.now());
    }

    @VisibleForTesting
    public Instant getTimestampForRecord(final PartitionContext ctx, final KafkaRecord<K, V> record, final Instant now) {
        Instant ts = this.timestampFunction.apply(record);
        // Ignore future timestamps
        if (ts.isAfter(this.maxEventTimestamp) && ts.isBefore(now)) {
            this.maxEventTimestamp = ts;
        }
        return ts;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return maxEventTimestamp.minus(this.maxDelay);
    }
}
