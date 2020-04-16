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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class FlowsSerdesTestSummary {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void canSerdesTopKFlows() throws IOException {
        TopKFlows topk = new TopKFlows();
        topk.setContext("abc");
        topk.setWindowMinMs(1L);
        topk.setWindowMaxMs(10L);
        topk.setTimestamp(topk.getWindowMinMs());

        FlowBytes flowBytes = new FlowBytes(42, 43);
        topk.getFlows().put("asdf", flowBytes);

        final String json = objectMapper.writeValueAsString(topk);

        String expectedJson = "{\"@timestamp\":1,\"context\":\"abc\",\"windowMinMs\":1,\"windowMaxMs\":10,\"flows\":{\"asdf\":{\"bytesIn\":42,\"bytesOut\":43}}}";
        try {
            JSONAssert.assertEquals(expectedJson, json, false);
        } catch (Throwable t) {
            throw new RuntimeException("Actual JSON: " + json, t);
        }

        TopKFlows topkParsed = objectMapper.readValue(json, TopKFlows.class);

        assertThat(topkParsed, equalTo(topk));
    }

}
