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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unloads segments that are no longer marked as used from servers.
 */
public class UnloadUnusedSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(UnloadUnusedSegments.class);

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, Boolean> broadcastStatusByDatasource = new HashMap<>();
    for (String broadcastDatasource : params.getBroadcastDatasources()) {
      broadcastStatusByDatasource.put(broadcastDatasource, true);
    }

    final CoordinatorStats stats = new CoordinatorStats();
    int totalUnloaded = 0;
    for (ServerHolder serverHolder : params.getDruidCluster().getAllServers()) {
      totalUnloaded += unloadUnusedSegmentsFromServer(
          serverHolder,
          params.getUsedSegments(),
          params,
          stats,
          broadcastStatusByDatasource
      );
    }
    log.info(
        "Unloaded [%d] segments across [%d] tiers.",
        totalUnloaded,
        stats.getTiers(CoordinatorStats.UNNEEDED_COUNT).size()
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private int unloadUnusedSegmentsFromServer(
      ServerHolder serverHolder,
      Set<DataSegment> usedSegments,
      DruidCoordinatorRuntimeParams params,
      CoordinatorStats stats,
      Map<String, Boolean> broadcastStatusByDatasource
  )
  {
    ImmutableDruidServer server = serverHolder.getServer();
    final boolean dropBroadcastOnly = server.getType() == ServerType.REALTIME
                                      || server.getType() == ServerType.INDEXER_EXECUTOR;

    int totalUnloaded = 0;
    for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
      // Check if the datasource is broadcast so that we don't miss dropping
      // any broadcast segment from realtime servers
      boolean isBroadcastDatasource = broadcastStatusByDatasource.computeIfAbsent(
          dataSource.getName(),
          datasource -> isBroadcastDatasource(datasource, params)
      );

      // The coordinator tracks used segments by examining the metadata store.
      // For tasks, the segments they create are unpublished, so those segments will get dropped
      // unless we exclude them here. We currently drop only broadcast segments in that case.
      // This check relies on the assumption that queryable stream tasks will never
      // ingest data to a broadcast datasource. If a broadcast datasource is switched to become a non-broadcast
      // datasource, this will result in the those segments not being dropped from tasks.
      // A more robust solution which requires a larger rework could be to expose
      // the set of segments that were created by a task/indexer here, and exclude them.
      if (dropBroadcastOnly && !isBroadcastDatasource) {
        continue;
      }

      for (DataSegment segment : dataSource.getSegments()) {
        if (usedSegments.contains(segment) || serverHolder.isDroppingSegment(segment)) {
          // Do nothing as segment is used or is already being dropped
        } else {
          serverHolder.getPeon().dropSegment(segment, null);
          stats.addToTieredStat(CoordinatorStats.UNNEEDED_COUNT, server.getTier(), 1);
          ++totalUnloaded;
          log.debug(
              "Dropping uneeded segment [%s] from server [%s] in tier [%s]",
              segment.getId(),
              server.getName(),
              server.getTier()
          );
        }
      }
    }

    return totalUnloaded;
  }

  /**
   * A datasource is considered to be broadcast if it has any broadcast rule.
   * <p>
   * RunRules already identifies broadcast datasources using the DataSourcesSnapshot.
   * But since the snapshot contains info of used segments only, a datasource
   * containing no used segments would not have been checked for broadcast.
   */
  private boolean isBroadcastDatasource(String datasource, DruidCoordinatorRuntimeParams params)
  {
    List<Rule> rules = params.getDatabaseRuleManager().getRulesWithDefault(datasource);
    for (Rule rule : rules) {
      if (rule instanceof BroadcastDistributionRule) {
        return true;
      }
    }
    return false;
  }
}
