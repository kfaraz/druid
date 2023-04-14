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

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Balances segments within the servers of a tier using the balancer strategy.
 * Segments are prioritized for move in the following order:
 * <ul>
 *   <li>Segments loaded on decommissioning servers</li>
 *   <li>Segments loading on active servers</li>
 *   <li>Segments loaded on active servers</li>
 * </ul>
 */
public class TierSegmentBalancer
{
  private static final EmittingLogger log = new EmittingLogger(TierSegmentBalancer.class);

  private final String tier;
  private final DruidCoordinatorRuntimeParams params;
  private final SegmentLoader loader;

  private final BalancerStrategy strategy;
  private final CoordinatorDynamicConfig dynamicConfig;
  private final CoordinatorRunStats runStats;

  private final Set<ServerHolder> allServers;
  private final List<ServerHolder> activeServers;
  private final List<ServerHolder> decommissioningServers;
  private final int totalMaxSegmentsToMove;

  public TierSegmentBalancer(
      String tier,
      Set<ServerHolder> servers,
      SegmentLoader loader,
      DruidCoordinatorRuntimeParams params
  )
  {
    this.tier = tier;
    this.params = params;
    this.loader = loader;

    this.strategy = params.getBalancerStrategy();
    this.dynamicConfig = params.getCoordinatorDynamicConfig();
    this.totalMaxSegmentsToMove = dynamicConfig.getMaxSegmentsToMove();
    this.runStats = loader.getStats();

    Map<Boolean, List<ServerHolder>> partitions =
        servers.stream().collect(Collectors.partitioningBy(ServerHolder::isDecommissioning));
    decommissioningServers = partitions.get(true);
    activeServers = partitions.get(false);
    this.allServers = servers;
  }

  void run()
  {
    if (activeServers.isEmpty() || (activeServers.size() <= 1 && decommissioningServers.isEmpty())) {
      log.warn(
          "Skipping balance for tier [%s] with [%d] active servers and [%d] decomissioning servers.",
          tier, activeServers.size(), decommissioningServers.size()
      );
      return;
    }

    log.info(
        "Balancing segments in tier [%s] with [%d] active servers and [%d] decommissioning servers.",
        tier, activeServers.size(), decommissioningServers.size()
    );

    // Move segments from decommissioning to active servers
    int maxDecommPercentToMove = dynamicConfig.getDecommissioningMaxPercentOfMaxSegmentsToMove();
    int maxDecommSegmentsToMove = (int) Math.ceil(totalMaxSegmentsToMove * (maxDecommPercentToMove / 100.0));
    int movedDecommSegments = moveSegmentsFromTo(decommissioningServers, activeServers, maxDecommSegmentsToMove);
    log.info(
        "Moved [%d] segments out of max [%d (%d%%)] from decommissioning to active servers.",
        movedDecommSegments, maxDecommSegmentsToMove, maxDecommPercentToMove
    );

    // Move segments across active servers
    int maxGeneralSegmentsToMove = totalMaxSegmentsToMove - movedDecommSegments;
    int movedGeneralSegments = moveSegmentsFromTo(activeServers, activeServers, maxGeneralSegmentsToMove);
    log.info(
        "Moved [%d] segments out of max [%d] between active servers.",
        movedGeneralSegments, maxGeneralSegmentsToMove
    );

    if (dynamicConfig.emitBalancingStats()) {
      strategy.emitStats(tier, loader.getStats(), Lists.newArrayList(allServers));
    }
  }

  private int moveSegmentsFromTo(
      List<ServerHolder> sourceServers,
      List<ServerHolder> destServers,
      int maxSegmentsToMove
  )
  {
    if (maxSegmentsToMove <= 0 || sourceServers.isEmpty() || destServers.isEmpty()) {
      return 0;
    }

    Iterator<BalancerSegmentHolder> pickedSegments
        = pickSegmentsFrom(sourceServers, maxSegmentsToMove, true);
    int movedCount = moveSegmentsTo(destServers, pickedSegments, maxSegmentsToMove);

    maxSegmentsToMove -= movedCount;
    pickedSegments = pickSegmentsFrom(sourceServers, maxSegmentsToMove, false);
    movedCount += moveSegmentsTo(destServers, pickedSegments, maxSegmentsToMove);

    return movedCount;
  }

  private Iterator<BalancerSegmentHolder> pickSegmentsFrom(
      List<ServerHolder> sourceServers,
      int maxSegmentsToPick,
      boolean pickLoadingSegments
  )
  {
    if (maxSegmentsToPick <= 0 || sourceServers.isEmpty()) {
      return Collections.emptyIterator();
    } else if (dynamicConfig.useBatchedSegmentSampler()) {
      return strategy.pickSegmentsToMove(
          sourceServers,
          params.getBroadcastDatasources(),
          maxSegmentsToPick,
          pickLoadingSegments
      );
    } else {
      if (pickLoadingSegments) {
        return Collections.emptyIterator();
      } else {
        return strategy.pickSegmentsToMove(
            sourceServers,
            params.getBroadcastDatasources(),
            dynamicConfig.getPercentOfSegmentsToConsiderPerMove()
        );
      }
    }
  }

  private int moveSegmentsTo(
      List<ServerHolder> destinationServers,
      Iterator<BalancerSegmentHolder> segmentsToMove,
      int maxSegmentsToMove
  )
  {
    int processed = 0;
    int movedCount = 0;
    while (segmentsToMove.hasNext() && processed < maxSegmentsToMove) {
      ++processed;
      if (moveSegment(segmentsToMove.next(), destinationServers)) {
        ++movedCount;
      }
    }
    return movedCount;
  }

  private boolean moveSegment(
      BalancerSegmentHolder segmentToMoveHolder,
      List<ServerHolder> destinationServers
  )
  {
    // Check if the picked segment still needs to be balanced
    final DataSegment segmentToMove = getLoadableSegment(segmentToMoveHolder.getSegment());
    if (segmentToMove == null) {
      return false;
    }

    // Keep the source server in the eligible list but filter out
    // servers already serving a replica or having a full load queue
    final ServerHolder source = segmentToMoveHolder.getServer();
    final List<ServerHolder> eligibleDestinationServers =
        destinationServers.stream().filter(
            s -> s.getServer().equals(source.getServer())
                 || s.canLoadSegment(segmentToMove)
        ).collect(Collectors.toList());

    if (eligibleDestinationServers.isEmpty()) {
      log.debug("No valid movement destinations for segment [%s].", segmentToMove.getId());
      markUnmoved(segmentToMove, "no valid destination");
    } else {
      final ServerHolder destination =
          strategy.findNewSegmentHomeBalancer(segmentToMove, eligibleDestinationServers);

      if (destination == null || destination.getServer().equals(source.getServer())) {
        log.debug("Segment [%s] is already 'optimally' placed.", segmentToMove.getId());
        markUnmoved(segmentToMove, "optimally placed");
      } else if (moveSegment(segmentToMove, source, destination)) {
        markMoved(segmentToMove);
        return true;
      } else {
        markUnmoved(segmentToMove, "move failed");
      }
    }

    return false;
  }

  /**
   * Returns a DataSegment with the correct value of loadSpec (as obtained from
   * metadata store). This method may return null if there is no snapshot available
   * for the underlying datasource or if the segment is unused.
   */
  private DataSegment getLoadableSegment(DataSegment segmentToMove)
  {
    final SegmentId segmentId = segmentToMove.getId();

    if (!params.getUsedSegments().contains(segmentToMove)) {
      markUnmoved(segmentToMove, "segment is unused");
      return null;
    }

    ImmutableDruidDataSource datasource = params.getDataSourcesSnapshot().getDataSource(segmentToMove.getDataSource());
    if (datasource == null) {
      markUnmoved(segmentToMove, "datasource not found");
      return null;
    }

    DataSegment loadableSegment = datasource.getSegment(segmentId);
    if (loadableSegment == null) {
      markUnmoved(segmentToMove, "metadata not found");
      return null;
    }

    return loadableSegment;
  }

  private boolean moveSegment(DataSegment segment, ServerHolder fromServer, ServerHolder toServer)
  {
    try {
      return loader.moveSegment(segment, fromServer, toServer);
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception while moving segment [%s]", segment.getId()).emit();
      return false;
    }
  }

  private void markMoved(DataSegment segment)
  {
    runStats.addToSegmentStat(Stats.Segments.MOVED, tier, segment.getDataSource(), 1);
  }

  private void markUnmoved(DataSegment segment, String reason)
  {
    RowKey rowKey = RowKey.builder()
                          .add(Dimension.DATASOURCE, segment.getDataSource())
                          .add(Dimension.TIER, tier)
                          .add(Dimension.STATUS, reason)
                          .build();
    runStats.add(Stats.Segments.UNMOVED, rowKey, 1);
  }

}
