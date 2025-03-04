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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class KafkaIOConfigTest
{
  private final ObjectMapper mapper;

  public KafkaIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules(new KafkaIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertNull("minimumMessageTime", config.getMinimumMessageTime());
    Assert.assertNull("maximumMessageTime", config.getMaximumMessageTime());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testSerdeWithDefaultsAndSequenceNumbers() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startSequenceNumbers\": {\"type\":\"start\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endSequenceNumbers\": {\"type\":\"end\", \"stream\":\"mytopic\", \"partitionSequenceNumberMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"}\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertTrue(config.isUseTransaction());
    Assert.assertNull("minimumMessageTime", config.getMinimumMessageTime());
    Assert.assertNull("maximumMessageTime", config.getMaximumMessageTime());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    KafkaIndexTaskIOConfig config = (KafkaIndexTaskIOConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                IOConfig.class
            )
        ), IOConfig.class
    );

    Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
    Assert.assertEquals("mytopic", config.getStartSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 1L, new KafkaTopicPartition(false, null, 1), 10L),
                        config.getStartSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals("mytopic", config.getEndSequenceNumbers().getStream());
    Assert.assertEquals(ImmutableMap.of(new KafkaTopicPartition(false, null, 0), 15L, new KafkaTopicPartition(false, null, 1),
                                        200L), config.getEndSequenceNumbers().getPartitionSequenceNumberMap());
    Assert.assertEquals(ImmutableMap.of("bootstrap.servers", "localhost:9092"), config.getConsumerProperties());
    Assert.assertFalse(config.isUseTransaction());
    Assert.assertEquals(DateTimes.of("2016-05-31T12:00Z"), config.getMinimumMessageTime());
    Assert.assertEquals(DateTimes.of("2016-05-31T14:00Z"), config.getMaximumMessageTime());
    Assert.assertEquals(Collections.emptySet(), config.getStartSequenceNumbers().getExclusivePartitions());
  }

  @Test
  public void testBaseSequenceNameRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("baseSequenceName"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("startPartitions"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testEndPartitionsRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("endSequenceNumbers"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testConsumerPropertiesRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("consumerProperties"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndTopicMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"other\", \"partitionOffsetMap\" : {\"0\":15, \"1\":200}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start topic/stream and end topic/stream must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testStartAndEndPartitionSetMatch() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("start partition set and end partition set must match"));
    mapper.readValue(jsonStr, IOConfig.class);
  }

  @Test
  public void testEndOffsetGreaterThanStart() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"kafka\",\n"
                     + "  \"taskGroupId\": 0,\n"
                     + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                     + "  \"startPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":1, \"1\":10}},\n"
                     + "  \"endPartitions\": {\"topic\":\"mytopic\", \"partitionOffsetMap\" : {\"0\":15, \"1\":2}},\n"
                     + "  \"consumerProperties\": {\"bootstrap.servers\":\"localhost:9092\"},\n"
                     + "  \"useTransaction\": false,\n"
                     + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                     + "  \"maximumMessageTime\": \"2016-05-31T14:00Z\"\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(IllegalArgumentException.class));
    exception.expectMessage(CoreMatchers.containsString("end offset must be >= start offset"));
    mapper.readValue(jsonStr, IOConfig.class);
  }
}
