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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.emitter.core.Event;

import java.util.List;

/**
 * Runner for a coordinator simulation.
 */
public interface CoordinatorSimulation
{
  /**
   * Starts the simulation if not already started.
   */
  void start();

  /**
   * Stops the simulation.
   */
  void stop();

  CoordinatorState coordinator();

  ClusterState cluster();

  /**
   * Represents the state of the coordinator during a simulation.
   */
  interface CoordinatorState
  {
    /**
     * Runs a single coordinator cycle.
     */
    void runCycle();

    /**
     * Synchronizes the inventory view maintained by the coordinator with the
     * actual state of the cluster.
     */
    void syncInventoryView();

    /**
     * Gets the inventory view of the specified server as maintained by the
     * coordinator.
     */
    DruidServer getInventoryView(String serverName);

    /**
     * Returns the metric events emitted in the previous coordinator run.
     */
    List<Event> getMetricEvents();

    /**
     * Gets the load percentage of the specified datasource as seen by the coordinator.
     */
    double getLoadPercentage(String datasource);
  }

  /**
   * Represents the state of the cluster during a simulation.
   */
  interface ClusterState
  {
    /**
     * Finishes load of all the segments that were queued in the previous
     * coordinator run. Also handles the responses and executes the respective
     * callbacks on the coordinator.
     */
    void loadQueuedSegments();

  }
}
