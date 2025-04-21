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

import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.testing.cluster.overlord.FaultyCoordinatorClient;
import org.apache.druid.testing.cluster.overlord.FaultyRemoteTaskActionClientFactory;

import java.util.Properties;
import java.util.Set;

public class ClusterTestingModule implements DruidModule
{
  private Properties props = null;
  private boolean isTask = false;

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Inject
  public void setNodeRoles(@Self Set<NodeRole> roles)
  {
    this.isTask = roles.contains(NodeRole.PEON);
  }

  @Override
  public void configure(Binder binder)
  {
    // Bind the testing stuff if this is a task and if cluster testing is enabled
    if (isTask && isClusterTestingEnabled()) {
      binder.bind(CoordinatorClient.class).to(FaultyCoordinatorClient.class).in(LazySingleton.class);
      binder.bind(RemoteTaskActionClientFactory.class).to(FaultyRemoteTaskActionClientFactory.class).in(LazySingleton.class);
    }
  }

  private boolean isClusterTestingEnabled()
  {
    return Boolean.parseBoolean(props.getProperty("druid.unsafe.cluster.testing", "false"));
  }
}
