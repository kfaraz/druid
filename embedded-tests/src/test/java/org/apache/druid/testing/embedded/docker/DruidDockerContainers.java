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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;

public class DruidDockerContainers
{
  /**
   * Creates a new {@link DruidContainer} to run a Coordinator node.
   */
  public static DruidContainer newCoordinator()
  {
    return new DruidContainer(NodeRole.COORDINATOR, 8081)
        .addProperty("druid.coordinator.startDelay", "PT0.1S")
        .addProperty("druid.coordinator.period", "PT0.5S")
        .addProperty("druid.manager.segments.pollDuration", "PT0.1S");
  }

  /**
   * Creates a new {@link DruidContainer} to run an Overlord node.
   */
  public static DruidContainer newOverlord()
  {
    // Keep a small sync timeout so that Peons and Indexers are not stuck
    // handling a change request when Overlord has already shutdown
    return new DruidContainer(NodeRole.OVERLORD, 8090)
        .addProperty("druid.indexer.storage.type", "metadata")
        .addProperty("druid.indexer.queue.startDelay", "PT0S")
        .addProperty("druid.indexer.queue.restartDelay", "PT0S")
        .addProperty("druid.indexer.runner.syncRequestTimeout", "PT1S");
  }

  /**
   * Creates a new {@link DruidContainer} to run an Indexer node.
   */
  public static DruidContainer newIndexer()
  {
    return new DruidContainer(NodeRole.INDEXER, 8091)
        .addProperty("druid.lookup.enableLookupSyncOnStartup", "false")
        .addProperty("druid.processing.buffer.sizeBytes", "50MiB")
        .addProperty("druid.processing.numMergeBuffers", "2")
        .addProperty("druid.processing.numThreads", "5");
  }

  /**
   * Creates a new {@link DruidContainer} to run a MiddleManager node.
   */
  public static DruidContainer newMiddleManager()
  {
    return new DruidContainer(NodeRole.MIDDLE_MANAGER, 8091);
  }

  /**
   * Creates a new {@link DruidContainer} to run a Historical node.
   */
  public static DruidContainer newHistorical()
  {
    final DruidContainer historical = new DruidContainer(NodeRole.HISTORICAL, 8083);
    historical.addProperty(
        "druid.segmentCache.locations",
        StringUtils.format(
            "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
            historical.getBaseMountDir() + "/segment-cache",
            HumanReadableBytes.parse("100M")
        )
    );
    historical.addProperty("druid.processing.buffer.sizeBytes", "50MiB");
    historical.addProperty("druid.processing.numMergeBuffers", "2");
    historical.addProperty("druid.processing.numThreads", "5");
    return historical;
  }

  /**
   * Creates a new {@link DruidContainer} to run a Broker node.
   */
  public static DruidContainer newBroker()
  {
    return new DruidContainer(NodeRole.BROKER, 8082);
  }

  /**
   * Creates a new {@link DruidContainer} to run a Router node.
   */
  public static DruidContainer newRouter()
  {
    return new DruidContainer(NodeRole.ROUTER, 8888);
  }
}
