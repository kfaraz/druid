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

package org.apache.druid.testing.simulate.embedded;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An embedded Druid server used in simulation tests.
 * This class and its methods (except a couple) are kept package protected as
 * they are used only by the specific server implementations.
 */
abstract class EmbeddedDruidServer
{
  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;

  EmbeddedDruidServer()
  {
    this.name = StringUtils.format(
        "%s-%2d",
        this.getClass().getSimpleName(),
        SERVER_ID.incrementAndGet()
    );
  }

  /**
   * @return Name of this server = type + 2-digit ID.
   */
  public String getName()
  {
    return name;
  }

  /**
   * Override properties configured on this server.
   */
  public Map<String, String> getProperties()
  {
    return Map.of();
  }

  /**
   * Creates a {@link ServerRunnable} corresponding to a specific Druid service.
   */
  abstract ServerRunnable createRunnable(
      LifecycleInitHandler handler
  );

  /**
   * {@link RuntimeInfo} to use for this server.
   */
  abstract RuntimeInfo getRuntimeInfo();

  /**
   * Creates a JUnit {@link ExternalResource} for this server that can be used
   * with {@code Rule}, {@code ClassRule} or in a {@code RuleChain}.
   */
  ExternalResource junitResource(
      TemporaryFolder tempDir,
      EmbeddedZookeeper zk,
      TestDerbyConnector.DerbyConnectorRule dbRule
  )
  {
    return new DruidServerJunitResource(this, tempDir, zk, dbRule);
  }

  /**
   * Handler used during initialization of the lifecycle of an embedded server.
   */
  interface LifecycleInitHandler
  {
    /**
     * @return Modules that should be used in {@link ServerRunnable#getModules()}.
     * This list contains modules that cannot be injected into the
     * {@code StartupInjectorBuilder} as they need dependencies that are only
     * bound later either in {@link ServerRunnable#getModules()} itself or via
     * the {@code CoreInjectorBuilder}.
     */
    List<? extends Module> getInitModules();

    /**
     * All implementations of {@link EmbeddedDruidServer} must call this method
     * from {@link ServerRunnable#initLifecycle(Injector)}.
     */
    void onLifecycleInit(Lifecycle lifecycle);
  }
}
