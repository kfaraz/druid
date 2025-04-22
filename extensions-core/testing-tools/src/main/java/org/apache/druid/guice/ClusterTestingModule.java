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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.config.Configs;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.testing.cluster.ClusterTestingConfig;
import org.apache.druid.testing.cluster.overlord.FaultyCoordinatorClient;
import org.apache.druid.testing.cluster.overlord.FaultyOverlordClient;
import org.apache.druid.testing.cluster.overlord.FaultyRemoteTaskActionClientFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Module that injects faulty clients into the Peon process to simulate various
 * fault scenarios.
 */
public class ClusterTestingModule implements DruidModule
{
  private static final Logger log = new Logger(ClusterTestingModule.class);

  private boolean isPeon = false;
  private boolean isClusterTestingEnabled = false;

  @Inject
  public void configure(
      Properties props,
      @Self Set<NodeRole> roles
  )
  {
    this.isClusterTestingEnabled = Boolean.parseBoolean(
        props.getProperty("druid.unsafe.cluster.testing", "false")
    );
    this.isPeon = roles.contains(NodeRole.PEON);
  }

  @Override
  public void configure(Binder binder)
  {
    // Bind the faulty clients only if this is a peon and cluster testing is enabled
    if (isPeon && isClusterTestingEnabled) {
      binder.bind(ClusterTestingConfig.class)
            .toProvider(ClusterTestingConfigProvider.class)
            .in(LazySingleton.class);
      binder.bind(CoordinatorClient.class)
            .to(FaultyCoordinatorClient.class)
            .in(LazySingleton.class);
      binder.bind(OverlordClient.class)
            .to(FaultyOverlordClient.class)
            .in(LazySingleton.class);
      binder.bind(RemoteTaskActionClientFactory.class)
            .to(FaultyRemoteTaskActionClientFactory.class)
            .in(LazySingleton.class);
    }
  }

  private static class ClusterTestingConfigProvider implements Provider<ClusterTestingConfig>
  {
    private final Task task;
    private final ObjectMapper mapper;

    @Inject
    public ClusterTestingConfigProvider(Task task, ObjectMapper mapper)
    {
      this.task = task;
      this.mapper = mapper;
    }


    @Override
    public ClusterTestingConfig get()
    {
      try {
        final Map<String, Object> configAsMap = task.getContextValue("clusterTesting");
        final String json = mapper.writeValueAsString(configAsMap);
        final ClusterTestingConfig testingConfig = mapper.readValue(json, ClusterTestingConfig.class);
        log.info("Running peon in cluster testing mode with config[%s].", testingConfig);

        return Configs.valueOrDefault(testingConfig, new ClusterTestingConfig(null));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
