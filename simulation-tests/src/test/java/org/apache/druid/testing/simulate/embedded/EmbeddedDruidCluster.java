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

import com.google.common.base.Preconditions;
import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Builder for an embedded Druid cluster that can be used in simulation tests.
 * A cluster is initialized with the following:
 * <ul>
 * <li>One or more Druid servers</li>
 * <li>A single Zookeeper server used by all the Druid services</li>
 * <li>An optional in-memory Derby metadata store</li>
 * <li>Temporary folder for segment and task storage</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * private final EmbeddedOverlord overlord = EmbeddedOverlord.create();
 * private final EmbeddedIndexer indexer = EmbeddedIndexer.create();
 *
 * &#64;Rule
 * public RuleChain cluster = EmbeddedDruidCluster.builder()
 *                                                .withDb()
 *                                                .with(overlord)
 *                                                .with(indexer)
 *                                                .build();
 * </pre>
 *
 * @see EmbeddedZookeeper
 * @see TestDerbyConnector
 * @see EmbeddedDruidServer
 */
public class EmbeddedDruidCluster
{
  private final List<EmbeddedDruidServer> servers;
  private final EmbeddedZookeeper zookeeper;
  private final TestDerbyConnector.DerbyConnectorRule dbRule;

  private EmbeddedDruidCluster(
      List<EmbeddedDruidServer> servers,
      EmbeddedZookeeper zookeeper,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  )
  {
    this.servers = List.copyOf(servers);
    this.zookeeper = zookeeper;
    this.dbRule = dbRule;
  }

  public static EmbeddedDruidCluster.Builder builder()
  {
    return new EmbeddedDruidCluster.Builder();
  }

  /**
   * Builds a {@link RuleChain} that can be used with JUnit {@code Rule} or
   * {@code ClassRule} to run simulation tests with this cluster.
   */
  public RuleChain ruleChain()
  {
    Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have atleast one server");

    RuleChain ruleChain = RuleChain.emptyRuleChain();

    if (dbRule != null) {
      ruleChain = ruleChain.around(dbRule);
    }

    ruleChain = ruleChain.around(zookeeper);

    final TemporaryFolder tempDir = new TemporaryFolder();
    ruleChain = ruleChain.around(tempDir);

    for (EmbeddedDruidServer server : servers) {
      ruleChain = ruleChain.around(server.junitResource(tempDir, zookeeper, dbRule));
    }

    return ruleChain;
  }

  /**
   * Builder for an {@link EmbeddedDruidCluster}.
   */
  public static class Builder
  {
    private final List<EmbeddedDruidServer> servers = new ArrayList<>();
    private boolean hasMetadataStore = false;

    /**
     * Adds a metadata store to this cluster.
     */
    public Builder withDb()
    {
      this.hasMetadataStore = true;
      return this;
    }

    /**
     * Adds a server to this cluster.
     */
    public Builder with(EmbeddedDruidServer server)
    {
      servers.add(server);
      return this;
    }

    public EmbeddedDruidCluster build()
    {
      return new EmbeddedDruidCluster(
          servers,
          new EmbeddedZookeeper(),
          hasMetadataStore ? new TestDerbyConnector.DerbyConnectorRule() : null
      );
    }
  }
}
