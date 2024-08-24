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

package org.apache.druid.testing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.HashMap;
import java.util.Map;

public class EmbeddedClusterConfigProvider extends ConfigFileConfigProvider
{
  private final EmbeddedDruidCluster embeddedDruidCluster;

  @JsonCreator
  public EmbeddedClusterConfigProvider(
      @JacksonInject EmbeddedDruidCluster embeddedDruidCluster
  )
  {
    super(buildProperties());
    this.embeddedDruidCluster = embeddedDruidCluster;

    embeddedDruidCluster.start();
  }

  @Override
  public String getZkConnectionString()
  {
    return embeddedDruidCluster.getZkConnectionString();
  }

  @Override
  public boolean isEmbeddedCluster()
  {
    return true;
  }

  private static Map<String, String> buildProperties()
  {
    final Map<String, String> props = new HashMap<>();
    props.put("router_url", "http://localhost:8082");
    props.put("router_tls_url", "http://localhost:8082");

    props.put("broker_url", "http://localhost:8082");
    props.put("broker_tls_url", "http://localhost:8082");

    props.put("historical_host", "localhost");
    props.put("historical_port", "8083");

    props.put("coordinator_host", "localhost");
    props.put("coordinator_port", "8081");

    props.put("indexer_host", "localhost");
    props.put("indexer_port", "8081");

    props.put("coordinator_two_host", "");
    props.put("coordinator_two_port", "");

    props.put("overlord_two_host", "");
    props.put("overlord_two_port", "");

    props.put("middlemanager_host", "localhost");
    props.put("middlemanager_port", "");

    return props;
  }
}
