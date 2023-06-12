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

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MetadataStorageConnectorConfigTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  @Test
  public void testEquals() throws IOException
  {
    final Map<String, String> dbcpProps = ImmutableMap.of("key", "value");
    MetadataStorageConnectorConfig metadataStorageConnectorConfig
        = MetadataStorageConnectorConfig.create("url", "user", "password", dbcpProps);
    MetadataStorageConnectorConfig metadataStorageConnectorConfig2
        = MetadataStorageConnectorConfig.create("url", "user", "password", dbcpProps);
    Assert.assertEquals(metadataStorageConnectorConfig, metadataStorageConnectorConfig2);
    Assert.assertEquals(metadataStorageConnectorConfig.hashCode(), metadataStorageConnectorConfig2.hashCode());
  }

  @Test
  public void testSerdeWithConnectUri() throws IOException
  {
    testConfigSerde(
        MetadataStorageConnectorConfig.create("jdbc:mysql://localhost:100/druid")
    );
  }

  @Test
  public void testSerdeWithDerbyHostPort() throws IOException
  {
    testConfigSerde(
        MetadataStorageConnectorConfig.create(null, "localhost", "1530", null)
    );
  }

  @Test
  public void testSerdeWithDefaultPasswordProvider() throws Exception
  {
    testConfigSerde(
        new MetadataStorageConnectorConfig(
            "jdbc:derby://localhost:100/druid;create=true",
            null,
            null,
            "admin",
            DefaultPasswordProvider.fromString("password"),
            null,
            null
        )
    );
  }

  @Test
  public void testSerdeWithDbcpProperties() throws Exception
  {
    final Properties dbcpProperties = new Properties();
    dbcpProperties.put("maxConnLifetimeMillis", "1200000");
    dbcpProperties.put("defaultQueryTimeout", "30000");

    testConfigSerde(
        new MetadataStorageConnectorConfig(
            "jdbc:derby://localhost:100/druid;create=true",
            null,
            null,
            "admin",
            DefaultPasswordProvider.fromString("password"),
            dbcpProperties,
            null
        )
    );
  }

  private void testConfigSerde(MetadataStorageConnectorConfig config) throws JsonProcessingException
  {
    String json = JSON_MAPPER.writeValueAsString(config);
    MetadataStorageConnectorConfig deserConfig =
        JSON_MAPPER.readValue(json, MetadataStorageConnectorConfig.class);
    Assert.assertEquals(config, deserConfig);
  }

  @Test
  public void testCreate()
  {
    Map<String, String> props = ImmutableMap.of("key", "value");
    MetadataStorageConnectorConfig config =
        MetadataStorageConnectorConfig.create("connectURI", "user", "pwd", props);
    Assert.assertEquals("connectURI", config.getConnectURI());
    Assert.assertEquals("user", config.getUser());
    Assert.assertEquals("pwd", config.getPassword());
    Assert.assertEquals(1, config.getDbcpProperties().size());
  }
}
