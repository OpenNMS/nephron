package org.opennms.nephron;

import static org.opennms.nephron.Pipeline.ReadFromKafka.getTimestamp;

import java.util.Optional;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

/**
 * Maintain the watermark at the (largest event time - acceptable delay) and ignore event times that are in the future
 * relative to the system clock.
 */
public class FlowTimestampPolicy extends TimestampPolicy<String, FlowDocument> {
    private final Duration maxDelay;

    private Instant maxEventTimestamp;

    public FlowTimestampPolicy(final Duration maxDelay, final Optional<Instant> previousWatermark) {
        this.maxDelay = maxDelay;
        this.maxEventTimestamp = previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE).plus(maxDelay);
    }

    @Override
    public Instant getTimestampForRecord(final PartitionContext ctx, final KafkaRecord<String, FlowDocument> record) {
        return getTimestampForRecord(ctx, record, Instant.now());
    }

    @VisibleForTesting
    public Instant getTimestampForRecord(final PartitionContext ctx, final KafkaRecord<String, FlowDocument> record, final Instant now) {
        final Instant ts = getTimestamp(record.getKV().getValue());

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
