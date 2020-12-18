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
package org.apache.beam.sdk.io.kafka;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opennms.nephron.Pipeline.getKafkaInputTimestampPolicyFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.opennms.netmgt.flows.persistence.model.FlowDocument;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.UInt64Value;

/**
 * Copy of https://github.com/apache/beam/blob/v2.20.0/sdks/java/io/kafka/src/test/java/org/apache/beam/sdk/io/kafka/CustomTimestampPolicyWithLimitedDelayTest.java
 * adapted for our model and customization.
 */
public class KafkaInputTimestampPolicyFactoryTest {

    @Test
    public void testCustomTimestampPolicyWithLimitedDelay() {
        Duration maxDelay = Duration.standardMinutes(2);

        TimestampPolicyFactory<String, FlowDocument> factory = getKafkaInputTimestampPolicyFactory(maxDelay);
        CustomTimestampPolicyWithLimitedDelay<String, FlowDocument> policy =
                (CustomTimestampPolicyWithLimitedDelay)factory.createTimestampPolicy(new TopicPartition("flows", 0), Optional.empty());

        // Base condition, we're at the current timestamp, there's no watermark yet (min value) and there is a backlog in the topic
        Instant now = Instant.now();
        TimestampPolicy.PartitionContext ctx = mock(TimestampPolicy.PartitionContext.class);
        when(ctx.getMessageBacklog()).thenReturn(100L);
        when(ctx.getBacklogCheckTime()).thenReturn(now);
        assertThat(policy.getWatermark(ctx), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

        // (1) Test simple case : watermark == max_timestamp - max_delay

        List<Long> input =
                ImmutableList.of(
                        -200_000L, -150_000L, -120_000L, -140_000L, -100_000L, // <<< Max timestamp
                        -110_000L);
        assertThat(getTimestampsForMyRecords(policy, now, input), is(input));

        // Watermark should be max_timestamp - maxDelay
        assertThat(
                policy.getWatermark(ctx), is(now.minus(Duration.standardSeconds(100)).minus(maxDelay)));

        // (2) Verify future timestamps

        input =
                ImmutableList.of(
                        -200_000L, -150_000L, -120_000L, -140_000L, 100_000L, // <<< timestamp is in future
                        -100_000L, -110_000L);

        assertThat(getTimestampsForMyRecords(policy, now, input), is(input));

        // Watermark should be now - max_delay (backlog in context still non zero)
        assertThat(policy.getWatermark(ctx, now), is(now.minus(maxDelay)));

        // (3) Verify that Watermark advances when there is no backlog

        // advance current time by 5 minutes
        now = now.plus(Duration.standardMinutes(5));
        Instant backlogCheckTime = now.minus(Duration.standardSeconds(10));

        when(ctx.getMessageBacklog()).thenReturn(0L);
        when(ctx.getBacklogCheckTime()).thenReturn(backlogCheckTime);

        assertThat(policy.getWatermark(ctx, now), is(backlogCheckTime.minus(maxDelay)));
    }

    // Takes offsets of timestamps from now returns the results as offsets from 'now'.
    private static List<Long> getTimestampsForMyRecords(
            TimestampPolicy<String, FlowDocument> policy, Instant now, List<Long> timestampOffsets) {
        return timestampOffsets.stream()
                .map(
                        ts -> {
                            Instant result =
                                    policy.getTimestampForRecord(
                                            null,
                                            new KafkaRecord<>(
                                                    "topic",
                                                    0,
                                                    0,
                                                    0,
                                                    KafkaTimestampType.CREATE_TIME,
                                                    new RecordHeaders(),
                                                    "key",
                                                    FlowDocument.newBuilder()
                                                            .setDeltaSwitched(UInt64Value.of(now.getMillis() + ts))
                                                            .build()));
                            return result.getMillis() - now.getMillis();
                        })
                .collect(Collectors.toList());
    }
}
