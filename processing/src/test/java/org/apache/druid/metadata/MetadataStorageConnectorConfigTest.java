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
  public void testSerde() throws IOException
  {
    MetadataStorageConnectorConfig config =
        MetadataStorageConnectorConfig.create("localhost:1000", "user", "pwd", null);

    String json = JSON_MAPPER.writeValueAsString(config);
    System.out.println("Json: " + json);

    MetadataStorageConnectorConfig deserConfig = JSON_MAPPER.readValue(json, MetadataStorageConnectorConfig.class);
    Assert.assertEquals(config, deserConfig);
  }

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
  public void testMetadataStorageConnectionConfigSimplePassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "\"nothing\"",
        "nothing"
    );
  }

  @Test
  public void testSerdeWithDefaultProviderPassword() throws Exception
  {
    testMetadataStorageConnectionConfig(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
  }

  private void testMetadataStorageConnectionConfig(
      boolean createTables,
      String host,
      int port,
      String connectURI,
      String user,
      String pwdString,
      String pwd
  ) throws Exception
  {
    MetadataStorageConnectorConfig config = JSON_MAPPER.readValue(
        "{" +
        "\"createTables\": \"" + createTables + "\"," +
        "\"host\": \"" + host + "\"," +
        "\"port\": \"" + port + "\"," +
        "\"connectURI\": \"" + connectURI + "\"," +
        "\"user\": \"" + user + "\"," +
        "\"password\": " + pwdString +
        "}",
        MetadataStorageConnectorConfig.class
    );

    Assert.assertEquals(host, config.getDerbyHost());
    Assert.assertEquals(port, config.getDerbyPort());
    Assert.assertEquals(connectURI, config.getConnectURI());
    Assert.assertEquals(user, config.getUser());
    Assert.assertEquals(pwd, config.getPassword());
    Assert.assertNull(config.getDbcpProperties());
  }

  @Test
  public void testDbcpProperties() throws Exception
  {
    testDbcpPropertiesFile(
        true,
        "host",
        1234,
        "connectURI",
        "user",
        "{\"type\":\"default\",\"password\":\"nothing\"}",
        "nothing"
    );
  }
  private void testDbcpPropertiesFile(
          boolean createTables,
          String host,
          int port,
          String connectURI,
          String user,
          String pwdString,
          String pwd
  ) throws Exception
  {
    MetadataStorageConnectorConfig config = JSON_MAPPER.readValue(
            "{" +
                    "\"createTables\": \"" + createTables + "\"," +
                    "\"host\": \"" + host + "\"," +
                    "\"port\": \"" + port + "\"," +
                    "\"connectURI\": \"" + connectURI + "\"," +
                    "\"user\": \"" + user + "\"," +
                    "\"password\": " + pwdString + "," +
                    "\"dbcp\": {\n" +
                    "  \"maxConnLifetimeMillis\" : 1200000,\n" +
                    "  \"defaultQueryTimeout\" : \"30000\"\n" +
                    "}" +
                    "}",
            MetadataStorageConnectorConfig.class
    );

    Assert.assertEquals(host, config.getDerbyHost());
    Assert.assertEquals(port, config.getDerbyPort());
    Assert.assertEquals(connectURI, config.getConnectURI());
    Assert.assertEquals(user, config.getUser());
    Assert.assertEquals(pwd, config.getPassword());
    Properties dbcpProperties = config.getDbcpProperties();
    Assert.assertEquals(dbcpProperties.getProperty("maxConnLifetimeMillis"), "1200000");
    Assert.assertEquals(dbcpProperties.getProperty("defaultQueryTimeout"), "30000");
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
