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

package org.apache.druid.simulate;

import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;

/**
 * An embedded Druid server used in simulation tests.
 */
public abstract class EmbeddedDruidServer
{
  private final String name;
  private final Map<String, String> properties;

  protected EmbeddedDruidServer(
      String name,
      Map<String, String> properties
  )
  {
    this.name = name;
    this.properties = properties;
  }

  public String getName()
  {
    return name;
  }

  public Map<String, String> getProperties()
  {
    return properties;
  }

  protected abstract ServerRunnable createRunnable(
      LifecycleInitHandler handler
  );

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
    return new DruidServerResource(this, tempDir, zk, dbRule);
  }

  public interface LifecycleInitHandler
  {
    List<? extends Module> getInitModules();
    void onLifecycleInit(Lifecycle lifecycle);
  }
}
