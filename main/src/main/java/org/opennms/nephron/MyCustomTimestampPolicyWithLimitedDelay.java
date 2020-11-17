package org.opennms.nephron;

import java.util.Optional;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
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
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord<K, V> record) {
        Instant ts = this.timestampFunction.apply(record);
        // Ignore future timestamps
        if (ts.isAfter(this.maxEventTimestamp) && ts.isBeforeNow()) {
            this.maxEventTimestamp = ts;
        }
        return ts;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return maxEventTimestamp.minus(this.maxDelay);
    }
}
