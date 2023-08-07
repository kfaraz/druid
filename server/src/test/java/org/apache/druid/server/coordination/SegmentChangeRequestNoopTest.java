/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SegmentChangeRequestNoopTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    final SegmentChangeRequestNoop noopRequest = new SegmentChangeRequestNoop();
    final String json = MAPPER.writeValueAsString(noopRequest);

    Map<String, Object> objectMap = MAPPER.readValue(
        json,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertEquals(1, objectMap.size());
    Assert.assertEquals("noop", objectMap.get("action"));

    DataSegmentChangeRequest deserialized = MAPPER.readValue(json, DataSegmentChangeRequest.class);
    Assert.assertTrue(deserialized instanceof SegmentChangeRequestNoop);
  }

  @Test
  public void testGetSegmentThrowsUnsupportedException()
  {
    SegmentChangeRequestNoop noopRequest = new SegmentChangeRequestNoop();
    Assert.assertThrows(
        UnsupportedOperationException.class,
        noopRequest::getSegment
    );
  }
}
