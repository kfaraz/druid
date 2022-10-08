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

import com.google.common.collect.Sets;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SegmentLoadManager
{
  // TODO:
  //  1. balancing moves
  //  2. replicant lookup
  //  3. replication throttler
  //  4. load queue modes
  //  5. revise
  //  6. logs, metrics
  //  7. test
  //  8. revise

  // Since there are certain APIs, there is definitely a requirement of the current state of things
  // We could keep an atomic reference in here
  // Which we update once the current run finishes, that should have the latest state of things
  // Is it fine for this to be a snapshot??mreplica
  // we will have to figure this out today!!!!


  // Why is SLM not a duty
  //  Because its lifecycle during coordinator downtime is very important.
  //  It needs to keep up with the activity in the load queues.

  /**
   * TODO:
   * how to get the current number of loading and loaded replicas
   * segment replicant lookup has it, and is initialized at the start of the run
   */

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
  //  - prioritization: availability, fault tolerance, disk space, full replication

  private static final EmittingLogger log = new EmittingLogger(SegmentLoadManager.class);

  private final AtomicBoolean runInProgress = new AtomicBoolean(false);

  private volatile DruidCluster cluster;
  private volatile BalancerStrategy balancerStrategy;
  private volatile CoordinatorStats runStats;

  private final boolean isHttpLoading;
  private final ServerInventoryView serverInventoryView;

  // TODO: this could even be present inside the ReplicationThrottler
  private int totalReplicasAssignedInRun = 0;
  private final ReplicationThrottler replicationThrottler = new ReplicationThrottler();

  private volatile SegmentReplicantLookup replicantLookup;

  public SegmentLoadManager(ServerInventoryView serverInventoryView, boolean isHttpLoading)
  {
    this.serverInventoryView = serverInventoryView;
    this.isHttpLoading = isHttpLoading;
  }

  /**
   * TODO: Should runInProgress even exist??
   *
   * @param cluster
   */
  public void prepareForRun(DruidCluster cluster, CoordinatorDynamicConfig dynamicConfig)
  {
    // update currentlyMovingSegments
    // update currentlyReplicatingSegments

    if (runInProgress.get()) {
      return;
    }

    // We would also like to get the other snapshots here such as
    // - the current replication state
    // - current set of used segments
    // - overshadowed segments (2 types?)

    // create ReplicationThrottler?

    this.cluster = cluster;
    runInProgress.set(true);

    // Create a bunch of ServerHolders

    this.replicantLookup = SegmentReplicantLookup
        .make(cluster, dynamicConfig.getReplicateAfterLoadTimeout());
    this.runStats = new CoordinatorStats();

    replicationThrottler.resetParams(
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    replicationThrottler.updateReplicationState(cluster.getTierNames());
  }

  public CoordinatorStats finishRunAndGetStats()
  {
    runInProgress.set(false);
    this.cluster = null;

    return runStats;
  }

  /**
   * Gets the list of segments that are eligibile for balancing.
   * - Exclude unused segments
   * - Exclude overshadowed segments
   * - Exclude broadcast segments
   */
  public List<String> getSegmentsToBalance()
  {
    // Balancing should not try to move segment to a server where that segment
    // is already in LOADING or LOADED or MOVING_TO state

    return Collections.emptyList();
  }

  public boolean moveSegment(DataSegment segment, ServerHolder fromServer, ServerHolder toServer)
  {
    // TODO: The later condition will handle this, I feel
    if (fromServer.getServer().getMetadata().equals(toServer.getServer().getMetadata())) {
      return false;
    }

    //  TODO: the segment should be used, not overshadowed, and not a broadcast segment
    //    and not a segment that should not exist on this tier
    //    this should be easy since BalanceSegments comes after RunRules

    // fromServer must be loading or serving the segment
    // and toServer must be able to load it
    final SegmentState stateOnSrc = fromServer.getSegmentState(segment);
    if ((stateOnSrc != SegmentState.LOADING
         && stateOnSrc != SegmentState.LOADED)
        || !toServer.canLoadSegment(segment)) {
      return false;
    }

    final boolean cancelSuccess = stateOnSrc == SegmentState.LOADING
                                  && fromServer.cancelSegmentOperation(SegmentState.LOADING, segment);

    // Load the segment on toServer
    if (cancelSuccess) {
      int loadedCountOnTier = replicantLookup
          .getLoadedReplicants(segment.getId(), toServer.getServer().getTier());
      loadReplica(segment, toServer, loadedCountOnTier < 1);
    } else {
      // TODO: callback to drop from fromServer
      return moveSegment(segment, fromServer.getPeon(), toServer.getPeon(), toServer.getServer().getName());
    }

    return true;
  }

  private boolean moveSegment(
      DataSegment segment,
      LoadQueuePeon loadPeon,
      LoadQueuePeon dropPeon,
      String destServerName
  )
  {
    final LoadPeonCallback loadPeonCallback = success -> {
      dropPeon.unmarkSegmentToDrop(segment);
      // TODO: update currentlyMovingSegments here
    };

    // mark segment to drop before it is actually loaded on server
    // to be able to account for this information in BalancerStrategy immediately
    // TODO: add to currentlyMovingSegments here
    dropPeon.markSegmentToDrop(segment);
    try {
      loadPeon.loadSegment(
          segment,
          success -> {
            // Drop segment only if:
            // (1) segment load was successful on toServer
            // AND (2) segment not already queued for drop on fromServer
            // AND (3a) loading is http-based
            //     OR (3b) inventory shows segment loaded on toServer

            // Do not check the inventory with http loading as the HTTP
            // response is enough to determine load success or failure
            if (success
                && !dropPeon.getSegmentsToDrop().contains(segment)
                && (isHttpLoading
                    || serverInventoryView.isSegmentLoadedByServer(destServerName, segment))) {
              dropPeon.dropSegment(segment, loadPeonCallback);
            } else {
              loadPeonCallback.execute(success);
            }
          }
      );
    }
    catch (Exception e) {
      dropPeon.unmarkSegmentToDrop(segment);
      throw new RuntimeException(e);
    }

    return true;
  }

  public void loadSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Handle every target tier
    final Set<String> targetTiers = tierToReplicaCount.keySet();
    for (String tier : targetTiers) {
      updateReplicasOnTier(segment, tier, tierToReplicaCount.get(tier));
    }

    // Find the minimum number of segments required for fault tolerance
    final int totalTargetReplicas = tierToReplicaCount.values().stream()
                                                      .reduce(0, Integer::sum);
    final int minLoadedSegments = totalTargetReplicas > 1 ? 2 : 1;

    // Drop segment from unneeded tiers if requirement is met across target tiers
    int loadedTargetReplicas = 0;
    for (String tier : targetTiers) {
      loadedTargetReplicas += replicantLookup.getLoadedReplicants(segment.getId(), tier);
    }
    if (loadedTargetReplicas < minLoadedSegments) {
      return;
    }

    final Set<String> dropTiers = Sets.newHashSet(cluster.getTierNames());
    dropTiers.removeAll(targetTiers);
    for (String dropTier : dropTiers) {
      updateReplicasOnTier(segment, dropTier, 0);
    }
  }

  public void broadcastSegment(DataSegment segment)
  {
    for (ServerHolder server : cluster.getAllServers()) {
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        // ignore this server
      } else if (server.isDecommissioning()) {
        dropBroadcastSegment(segment, server);
      } else {
        loadBroadcastSegment(segment, server);
      }
    }
  }

  private void loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.LOADED || state == SegmentState.LOADING) {
      // Do nothing
    }

    boolean cancelDrop = state == SegmentState.DROPPING
                         && server.cancelSegmentOperation(SegmentState.DROPPING, segment);
    if (cancelDrop) {
      // TODO: cancel metric here
    } else if (server.canLoadSegment(segment)) {
      server.loadSegment(segment);
      // TODO: assignment metric here
    } else {
      log.makeAlert("Failed to broadcast segment for [%s]", segment.getDataSource())
         .addData("segmentId", segment.getId())
         .addData("segmentSize", segment.getSize())
         .addData("hostName", server.getServer().getHost())
         .addData("availableSize", server.getAvailableSize())
         .emit();
    }
  }

  private void dropBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.NONE || state == SegmentState.DROPPING) {
      // do nothing
    }

    boolean cancelLoad = state == SegmentState.LOADING
                         && server.cancelSegmentOperation(SegmentState.LOADING, segment);
    if (cancelLoad) {
      // TODO: cancel metric here
    } else {
      server.dropSegment(segment);
      // TODO: drop metric here
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
   * Queues load of a replica of the segment on the given server.
   */
  private int loadReplica(DataSegment segment, ServerHolder server, boolean isPrimary)
  {
    final String tier = server.getServer().getTier();
    if (!isPrimary && !replicationThrottler.canCreateReplicant(tier)) {
      return 0;
    }

    // TODO: update currentlyReplicating and clear it in the callback
    replicationThrottler.registerReplicantCreation(tier, segment.getId(), server.getServer().getHost());
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
