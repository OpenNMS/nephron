/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
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

package org.opennms.nephron;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.opennms.features.kafka.producer.model.CollectionSetProtos;

public class TopKInterfacesTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void canProcessResponseTimeMetrics() {
        /*
        2020-04-12T04:34:34,996 | DEBUG | kafka-producer-network-thread | producer-1 | KafkaPersister                   | 435 - org.opennms.features.kafka.producer - 26.1.0.SNAPSHOT | persisted collection timestamp: 1586666074994
                resource {
                  generic {
                    node {
                    }
                    type: "latency"
                    instance: "127.0.0.1[Minion-RPC]"
                  }
                  numeric {
                    group: "minion-rpc"
                    name: "minion-rpc"
                    value: 92.0
                  }
}
         */
        CollectionSetProtos.CollectionSet cset = CollectionSetProtos.CollectionSet.newBuilder()
                .setTimestamp(1586666074984L)
                .addResource(CollectionSetProtos.CollectionSetResource.newBuilder()
                        .setResponse(CollectionSetProtos.ResponseTimeResource.newBuilder()
                                .setInstance("127.0.0.1[Minion-RPC]"))
                        .addNumeric(CollectionSetProtos.NumericAttribute.newBuilder()
                                .setGroup("minion-rpc")
                                .setName("minion-rpc")
                                .setValue(92d)
                                .build())).build();

        // What?
        //
        // The keys we want to track are:
        //    service
        //    location/service
        //    location
        // for each of these we want to track the top K services with the highest response times
        // value should be output in a rolling window - last 5 minutes - output every change.


        // How?
        // Service name - minion-rpc
        // Location - ?
        // IP address - ? first part of instance@#!

        // row is ((location,service,host,value), timestamp)
        // we want to track Top K larget values for different groups
        //  ("") -> Largest value on whole system
        // ("location") -> Largest values at various location
        // ("location", "service") -> Which hosts respond worst to this service
        // ("service") -> Which hosts at which locations have the worst response time

        // Various way to group the same data - tells us different things
        p.run();
    }

    public static ParDo.SingleOutput<CollectionSetProtos.CollectionSet, CollectionSetProtos.CollectionSet> attachTimestamp() {
        return ParDo.of(new DoFn<CollectionSetProtos.CollectionSet, CollectionSetProtos.CollectionSet>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                final CollectionSetProtos.CollectionSet cset = c.element();
                c.outputWithTimestamp(cset, Instant.ofEpochMilli(cset.getTimestamp()));
            }
        });
    }

    @Test
    public void canTrackTopKInterfaces() {

        // Generate Sinusoidal time series for interface traffic

        // Serialize to OpenNMS model

        // Send to Pipeline

        // Output: Protobuf

        // Forward to ES

        // Query from ES - single query - view directly from Grafana


    }
}
