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

package org.apache.druid.server.metrics;

import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;

public interface TaskCountStatsProvider
{

  /**
   * Collects all task level stats such as:
   * <ul>
   * <li>Number of tasks currently in RUNNING, PENDING or WAITING state</li>
   * <li>Number of tasks that succeeded or failed since the previous invocation
   * of this method</li>
   * </ul>
   *
   * @return All task stats collected since the previous invocation of this method.
   */
  CoordinatorRunStats getStats();
}
