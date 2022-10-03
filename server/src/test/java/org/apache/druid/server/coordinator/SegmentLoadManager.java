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

package org.apache.druid.server.coordinator;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

public class SegmentLoadManager
{
  // TODO Tasks
  //  server holder should have canLoad()
  //    accounts for load queue size
  //    available disk space and if it is already loading segment or not
  //   server holder can also have canDrop() which checks if you basically already have the segment loaded
  //  loading
  //    account for disk space and load queue size
  //  balancing
  //  snapshots
  //  logging
  //  metrics

  // TODO: More
  //  top level method to skip balancing or replication altogether
  //  logging and metrics
  //  blacklist servers where segments are timing out
  //  ensure there is no possible invalid state
  //  dynamic config to enable new flow
  //  easy way to count the values of the existing metrics so that we can work through maxNonPrimary
  //  segments that are not required in a tier anymore should not be considered for balancing
  //  I feel we would soon be combining balancing and loading
  //  primary prioritization instead of replica throttling
  //  figure out the underReplicated computation and simplify it

  // TODO cases:
  //  - items stuck in a queue
  //  - server flickers
  //  - cluster starting, only one server available
  //  - prioritization of availability, fault tolerance, disk space and full replication

  // TODO: priorities
  //  - availability
  //  - fault tolerance
  //  - disk space
  //  - full replication

  // TODO holding on to the ServerHolder
  //  pros: easy and clean operations of load/drop/cancel/move
  //  pros: clean update of segment state
  //  cons: need to hold onto it
  //  cons: might miss out on delta

  private static final EmittingLogger log = new EmittingLogger(SegmentLoadManager.class);

  private final AtomicBoolean runInProgress = new AtomicBoolean(false);

  private final Table<SegmentId, String, String> pendingActions = HashBasedTable.create();
  private final Table<SegmentId, String, String> completedActions = HashBasedTable.create();

  // TODO: ensure that there is atmost a single status for a single server
  private final ConcurrentMap<SegmentId, Map<String, List<SegmentStatus>>> segmentStatusPerTier
      = new ConcurrentHashMap<>();

  private static class SegmentStatus
  {
    private final ServerHolder server;
    private SegmentState state;

    private SegmentStatus(ServerHolder server, SegmentState state)
    {
      this.server = server;
      this.state = state;
    }
  }

  private volatile DruidCluster cluster;
  private volatile BalancerStrategy balancerStrategy;
  private volatile CoordinatorStats stats;

  public void startRun(DruidCluster cluster)
  {
    if (runInProgress.get()) {
      return;
    }

    // We would also like to get the other snapshots here such as
    // - the current replication state
    // - current set of used segments
    // - overshadowed segments (2 types?)

    this.cluster = cluster;
    runInProgress.set(true);

    // add delta to the cluster
    // where do we keep the delta??
    // delta could be another DruidCluster containing servers, but then we wouldn't know what has been "dropped"
    // so basically just a set of finished queue items 👍
    // Map<String, Request> successfullyFinishedOperations
    // Map<String, Request> inProgressOperations??
  }

  public void finishRun()
  {
    runInProgress.set(false);
    this.cluster = null;
  }

  /**
   * Gets the list of segments that are eligibile for balancing.
   * - Exclude unused segments
   * - Exclude overshadowed segments
   * -
   */
  public List<String> getSegmentsToBalance()
  {
    // Balancing should not try to move segment to a server where that segment
    // is already in LOADING or LOADED or MOVING_TO state

    return Collections.emptyList();
  }

  public void moveSegment(String segment, String fromServer, String toServer)
  {

  }

  public void loadSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Handle every target tier
    tierToReplicaCount.forEach(
        (tier, targetReplicaCount)
            -> updateReplicasOnTier(segment, tier, targetReplicaCount)
    );

    // Remove the segment from dropTiers if ???
    // TODO: finalize and justify the condition
    final Map<String, List<SegmentStatus>> segmentStatus = segmentStatusPerTier
        .getOrDefault(segment.getId(), Collections.emptyMap());

    final int loadedCount = 1;
    final int minLoadedSegments = tierToReplicaCount.size() > 1 ? 2 : 1;
    if (loadedCount < minLoadedSegments) {
      return;
    }

    final Set<String> dropTiers = new HashSet<>(segmentStatus.keySet());
    dropTiers.removeAll(tierToReplicaCount.keySet());
    for (String dropTier : dropTiers) {
      updateReplicasOnTier(segment, dropTier, 0);
    }
  }

  public Map<String, Integer> getCurrentReplicas(String segment)
  {
    // Useful for
    // - getting current status
    // - count of underreplicated segments?
    // inventory view gives count of segments already loaded
    // this can also include count of segments that are in the queue, so a useful parameter

    return Collections.emptyMap();
  }

  private void updateReplicasOnTier(DataSegment segment, String tier, int targetCount)
  {
    final Map<SegmentState, List<ServerHolder>> serversByState = new EnumMap<>(SegmentState.class);
    cluster.getHistoricalsByTier(tier).forEach(
        serverHolder -> serversByState
            .computeIfAbsent(serverHolder.getSegmentState(segment), state -> new ArrayList<>())
            .add(serverHolder)
    );

    int currentCount = 0;
    int movingCount = 0;
    for (SegmentState state : serversByState.keySet()) {
      switch (state) {
        case LOADED:
        case LOADING:
          currentCount += serversByState.get(state).size();
          break;
        case DROPPING:
          currentCount -= serversByState.get(state).size();
          break;
        case MOVING_FROM:
        case MOVING_TO:
          movingCount += serversByState.get(state).size();
          break;
        default:
          break;
      }
    }

    if (targetCount == currentCount) {
      return;
    }

    if (targetCount == 0 && movingCount > 0) {
      // Cancel the MOVING_TO ops, their callback will cancel the MOVING_FROM ops
      int cancelledOps = cancelOperations(
          SegmentState.MOVING_TO,
          segment,
          serversByState.get(SegmentState.MOVING_TO),
          movingCount
      );
    }

    // TODO: update the actual states too!!!

    if (targetCount > currentCount) {
      int numReplicasToLoad = targetCount - currentCount;
      final int cancelledDrops = cancelOperations(
          SegmentState.DROPPING,
          segment,
          serversByState.get(SegmentState.DROPPING),
          numReplicasToLoad
      );
      // TODO: metric here

      numReplicasToLoad -= cancelledDrops;
      if (numReplicasToLoad > 0) {
        // assign primary and replicas to this tier;
      }
    } else {
      int numReplicasToDrop = currentCount - targetCount;
      final int cancelledLoads = cancelOperations(
          SegmentState.LOADING,
          segment,
          serversByState.get(SegmentState.LOADING),
          numReplicasToDrop
      );
      // TODO: metric here

      numReplicasToDrop -= cancelledLoads;
      if (numReplicasToDrop > 0) {
        final int successfulOperations = dropReplicas(
            numReplicasToDrop,
            segment,
            serversByState.get(SegmentState.LOADED)
        );
      }
      // TODO: final metric here
    }
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from a tier.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(int numToDrop, DataSegment segment, List<ServerHolder> eligibleServers)
  {
    final TreeSet<ServerHolder> eligibleLiveServers = new TreeSet<>();
    final TreeSet<ServerHolder> eligibleDyingServers = new TreeSet<>();
    for (ServerHolder server : eligibleServers) {
      if (!server.isServingSegment(segment)) {
        // ignore this server
      } else if (server.isDecommissioning()) {
        eligibleDyingServers.add(server);
      } else {
        eligibleLiveServers.add(server);
      }
    }

    // Drop as many replicas as possible from decommissioning servers
    int remainingNumToDrop = numToDrop;
    int numDropsQueued = dropReplicas(remainingNumToDrop, segment, eligibleDyingServers.iterator());
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          eligibleLiveServers.size() > remainingNumToDrop
          ? eligibleLiveServers.iterator()
          : balancerStrategy.pickServersToDrop(segment, eligibleLiveServers);
      numDropsQueued += dropReplicas(remainingNumToDrop, segment, serverIterator);
    }
    if (numToDrop > numDropsQueued) {
      log.warn("I have no servers serving [%s]?", segment.getId());
    }

    return numDropsQueued;
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from the servers.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(int numToDrop, DataSegment segment, Iterator<ServerHolder> serverIterator)
  {
    int numDropsQueued = 0;
    while (numToDrop > numDropsQueued && serverIterator.hasNext()) {
      // TODO: update the SegmentStatus here
      // TODO: callback should update the segment status and also the underlying DruidServer
      // I wonder if segment status should just live inside the ServerHolder

      ServerHolder holder = serverIterator.next();
      holder.getPeon().dropSegment(segment, null);
      numDropsQueued++;
    }

    return numDropsQueued;
  }

  private int cancelOperations(
      SegmentState state,
      DataSegment segment,
      List<ServerHolder> servers,
      int requiredNumCancellations
  )
  {
    int numCancelled = 0;
    for (ServerHolder server : servers) {
      if (numCancelled >= requiredNumCancellations) {
        break;
      }

      if (server.getSegmentState(segment) == state) {
        numCancelled += cancelOperation(state, segment, server) ? 1 : 0;
      }
    }

    return numCancelled;
  }

  private boolean cancelOperation(SegmentState state, DataSegment segment, ServerHolder server)
  {
    final boolean success;
    switch (state) {
      case LOADING:
      case MOVING_TO:
        success = server.getPeon().cancelLoad(segment);
        break;
      case DROPPING:
      case MOVING_FROM:
        success = server.getPeon().cancelDrop(segment);
        break;
      default:
        success = false;
    }

    // TODO: cancelDrop is fine but cancelLoad may cancel a load as well as a MOVING_TO operation together

    if (success) {
      // TODO: set state to cancelled
      // this should happen inside the ServerHolder itself
    }
    return success;
  }

}
