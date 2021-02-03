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

package org.opennms.nephron;

import static org.opennms.nephron.Pipeline.toFlowSummary;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.opennms.nephron.elastic.FlowSummary;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A coder that uses Jackson for encoding / decoding.
 *
 * The JacksonJsonCoder is not very efficient because it is based on reflection and uses JSON as the encoded
 * representation. Test pipelines may use this coder when they want to check generated flow summary documents.
 */
public class JacksonJsonCoder<T> extends Coder<T> {

    /**
     * Transformation that can be applied to pipeline to transform FLowSummaryData instances into FlowSummary
     * instances.
     */
    public static ParDo.SingleOutput<FlowSummaryData, FlowSummary> TO_FLOW_SUMMARY =
            ParDo.of(new DoFn<FlowSummaryData, FlowSummary>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    c.output(toFlowSummary(c.element()));
                }
            });


    private static final ObjectMapper mapper = new ObjectMapper();
    private static final StringUtf8Coder delegate = StringUtf8Coder.of();
    private final Class<T> clazz;

    public JacksonJsonCoder(Class<T> clazz) {
        this.clazz = Objects.requireNonNull(clazz);
    }

    @Override
    public void encode(T value, OutputStream outStream) throws IOException {
        final String json = mapper.writeValueAsString(value);
        delegate.encode(json, outStream);
    }

    @Override
    public T decode(InputStream inStream) throws IOException {
        String json = delegate.decode(inStream);
        return mapper.readValue(json, clazz);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
        return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {
        // pass
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JacksonJsonCoder<?> that = (JacksonJsonCoder<?>) o;
        return Objects.equals(clazz, that.clazz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz);
    }
}
