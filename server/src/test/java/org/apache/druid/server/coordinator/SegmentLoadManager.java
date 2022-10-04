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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SegmentLoadManager
{
  /**
   * TODO:
   * how to get the current number of loading and loaded replicas
   * segment replicant lookup has it, and is initialized at the start of the run
   * we can either get a more recent number
   * or we can just work with the replicant lookup itself
   * pros: already exists
   * cons: somewhat stale (not really an issue, I feel) it just means that we would be skipping this run
   * and assigning/dropping things in the next run
   * where do you need to fetch the current number of replicas in the new flow?
   * it is used in assignment in the old flow, so not really needed here!
   */

  //  ServerHolder
  //    should load/drop be initiated here or in the load manager itself
  //    pros would be easier tracking of metrics
  //    all the callbacks living in one place
  //    we could still just call one method to do the stuff
  //  loading
  //    replica prioritization and throttling
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

    // TODO: synchronize delta in each ServerHolder
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

    // Find the minimum number of segments required for fault tolerance
    final int totalTargetReplicas = tierToReplicaCount.values().stream()
                                                      .reduce(0, Integer::sum);
    final int minLoadedSegments = totalTargetReplicas > 1 ? 2 : 1;

    // TODO: get the loadedCount
    // Drop segment from unneeded tiers if requirement is met across target tiers
    final int loadedCount = 1;
    if (loadedCount < minLoadedSegments) {
      return;
    }

    // TODO: find the dropTiers
    final Set<String> dropTiers = new HashSet<>();
    dropTiers.removeAll(tierToReplicaCount.keySet());
    for (String dropTier : dropTiers) {
      updateReplicasOnTier(segment, dropTier, 0);
    }
  }

  // Useful for
  // - getting current status
  // - count of underreplicated segments?
  // inventory view gives count of segments already loaded
  // this can also include count of segments that are in the queue, so a useful parameter
  public Map<String, Integer> getCurrentReplicas(String segment)
  {

    return Collections.emptyMap();
  }

  private void updateReplicasOnTier(DataSegment segment, String tier, int targetCount)
  {
    final Map<SegmentState, List<ServerHolder>> serversByState = new EnumMap<>(SegmentState.class);
    Arrays.stream(SegmentState.values())
          .forEach(state -> serversByState.put(state, new ArrayList<>()));
    cluster.getHistoricalsByTier(tier).forEach(
        serverHolder -> serversByState
            .get(serverHolder.getSegmentState(segment))
            .add(serverHolder)
    );

    final int currentCount = serversByState.get(SegmentState.LOADED).size()
                             + serversByState.get(SegmentState.LOADING).size();
    if (targetCount == currentCount) {
      return;
    }

    final int movingCount = serversByState.get(SegmentState.MOVING_TO).size();
    if (targetCount == 0 && movingCount > 0) {
      // Cancel the segment balancing moves, if any
      int cancelledMoves = cancelOperations(
          SegmentState.MOVING_TO,
          segment,
          serversByState.get(SegmentState.MOVING_TO),
          movingCount
      );
    }

    if (targetCount > currentCount) {
      int numReplicasToLoad = targetCount - currentCount;
      final int cancelledDrops = cancelOperations(
          SegmentState.DROPPING,
          segment,
          serversByState.get(SegmentState.DROPPING),
          numReplicasToLoad
      );

      numReplicasToLoad -= cancelledDrops;
      int numLoaded = serversByState.get(SegmentState.LOADED).size();
      boolean primaryExists = numLoaded + cancelledDrops > 0;
      if (numReplicasToLoad > 0) {
        int successfulLoadsQueued = loadReplicas(
            numReplicasToLoad,
            segment,
            serversByState.get(SegmentState.NONE),
            primaryExists
        );
      }
    } else {
      int numReplicasToDrop = currentCount - targetCount;
      final int cancelledLoads = cancelOperations(
          SegmentState.LOADING,
          segment,
          serversByState.get(SegmentState.LOADING),
          numReplicasToDrop
      );

      numReplicasToDrop -= cancelledLoads;
      if (numReplicasToDrop > 0) {
        final int successfulOperations = dropReplicas(
            numReplicasToDrop,
            segment,
            serversByState.get(SegmentState.LOADED)
        );
      }
    }
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from a tier.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(int numToDrop, DataSegment segment, List<ServerHolder> eligibleServers)
  {
    if (eligibleServers == null || eligibleServers.isEmpty()) {
      return 0;
    }

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
      ServerHolder holder = serverIterator.next();
      holder.dropSegment(segment);
      numDropsQueued++;
    }

    return numDropsQueued;
  }

  /**
   * Queues load of {@code numToLoad} replicas of the segment on a tier.
   * Returns the number of successfully queued load operations.
   */
  private int loadReplicas(
      int numToLoad,
      DataSegment segment,
      List<ServerHolder> candidateServers,
      boolean primaryExists
  )
  {
    final List<ServerHolder> eligibleServers =
        candidateServers.stream()
                        .filter(server -> server.canLoadSegment(segment))
                        .collect(Collectors.toList());

    final Iterator<ServerHolder> serverIterator =
        balancerStrategy.findNewSegmentHomeReplicator(segment, eligibleServers);
    if (!serverIterator.hasNext()) {
      // TODO: some noise here!
    }

    // Load the primary on this tier
    int numLoadsQueued = 0;
    if (!primaryExists && serverIterator.hasNext()) {
      numLoadsQueued += loadReplica(segment, serverIterator.next(), true);
    }

    // Load the remaining replicas
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      numLoadsQueued += loadReplica(segment, serverIterator.next(), false);
    }

    if (numToLoad > numLoadsQueued) {
      log.warn("I have no servers serving [%s]?", segment.getId());
    }

    return numLoadsQueued;
  }

  /**
   * Queues load of a replica of the segment on the server.
   */
  private int loadReplica(DataSegment segment, ServerHolder server, boolean isPrimary)
  {
    // check replication throttling

    server.loadSegment(segment);
    return 1;
  }

  private int cancelOperations(
      SegmentState state,
      DataSegment segment,
      List<ServerHolder> servers,
      int maxNumToCancel
  )
  {
    int numCancelled = 0;
    for (int i = 0; i < servers.size() && numCancelled < maxNumToCancel; ++i) {
      numCancelled += servers.get(i).cancelSegmentOperation(state, segment)
                      ? 1 : 0;
    }
    return numCancelled;
  }

}
