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

import org.apache.druid.metadata.TestDerbyConnector;
import org.junit.rules.RuleChain;

/**
 * Builder for an embedded Druid cluster that can be used in simulation tests.
 */
public class EmbeddedDruidCluster
{
  private EmbeddedDruidServer server;
  private boolean hasMetadataStore = false;

  public static EmbeddedDruidCluster builder()
  {
    return new EmbeddedDruidCluster();
  }

  /**
   * Adds a metadata store to this cluster.
   */
  public EmbeddedDruidCluster withDb()
  {
    this.hasMetadataStore = true;
    return this;
  }

  /**
   * Adds a server to this cluster.
   */
  public EmbeddedDruidCluster withServer(EmbeddedDruidServer server)
  {
    this.server = server;
    return this;
  }

  /**
   * Builds a {@link RuleChain} that can be used with JUnit {@code Rule} or
   * {@code ClassRule} to run simulation tests with this cluster.
   */
  public RuleChain build()
  {
    RuleChain ruleChain = RuleChain.emptyRuleChain();

    final TestDerbyConnector.DerbyConnectorRule dbRule;
    if (hasMetadataStore) {
      dbRule = new TestDerbyConnector.DerbyConnectorRule();
      ruleChain = ruleChain.around(dbRule);
    } else {
      dbRule = null;
    }

    final EmbeddedZookeeper zk = new EmbeddedZookeeper();
    ruleChain = ruleChain.around(zk);

    return ruleChain.around(
        server.junitResource(zk, dbRule)
    );
  }
}
