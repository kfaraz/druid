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
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.SegmentsMetadataManager;
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

/**
 * Manager for segment loading, balancing and broadcasting.
 */
public class SegmentLoadManager
{
  // TODO:
  //  2. all callbacks
  //  4. load queue modes
  //  5. revise
  //  6. logs, metrics
  //  7. test
  //  8. revise

  private static final EmittingLogger log = new EmittingLogger(SegmentLoadManager.class);

  private final boolean isHttpLoading;
  private final ServerInventoryView serverInventoryView;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ReplicationThrottler replicationThrottler = new ReplicationThrottler();

  private final AtomicBoolean runInProgress = new AtomicBoolean(false);

  // Fields tied to a single run
  private volatile DruidCluster cluster;
  private volatile CoordinatorStats runStats;
  private volatile SegmentReplicantLookup replicantLookup;
  private volatile DruidCoordinatorRuntimeParams runParams;

  public SegmentLoadManager(
      ServerInventoryView serverInventoryView,
      SegmentsMetadataManager segmentsMetadataManager,
      boolean isHttpLoading
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.isHttpLoading = isHttpLoading;
  }

  public void prepareForRun(DruidCoordinatorRuntimeParams runParams)
  {
    if (runInProgress.get()) {
      return;
    }
    runInProgress.set(true);

    this.runParams = runParams;
    this.cluster = runParams.getDruidCluster();
    this.runStats = new CoordinatorStats();

    final CoordinatorDynamicConfig dynamicConfig = runParams.getCoordinatorDynamicConfig();
    this.replicantLookup = SegmentReplicantLookup
        .make(cluster, dynamicConfig.getReplicateAfterLoadTimeout());

    replicationThrottler.resetParams(
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    replicationThrottler.updateReplicationState(cluster.getTierNames());
  }

  public CoordinatorStats getRunStats()
  {
    runInProgress.set(false);
    return runStats;
  }

  /**
   * Moves the given segment between two servers of the same tier.
   * <p>
   * See if we can move balancing here.
   */
  public boolean moveSegment(DataSegment segment, ServerHolder fromServer, ServerHolder toServer)
  {
    if (!fromServer.getServer().getTier().equals(toServer.getServer().getTier())) {
      return false;
    } else if (fromServer.getServer().getMetadata().equals(toServer.getServer().getMetadata())) {
      return false;
    }

    // fromServer must be loading or serving the segment
    // and toServer must be able to load it
    final SegmentState stateOnSrc = fromServer.getSegmentState(segment);
    if ((stateOnSrc != SegmentState.LOADING && stateOnSrc != SegmentState.LOADED)
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

  public Map<String, Integer> getUnavailableSegmentCountPerDatasource()
  {
    if (replicantLookup == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> numUnavailableSegments = new Object2IntOpenHashMap<>();

    Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      if (replicantLookup.getLoadedReplicants(segment.getId()) == 0) {
        numUnavailableSegments.addTo(segment.getDataSource(), 1);
      } else {
        numUnavailableSegments.addTo(segment.getDataSource(), 0);
      }
    }

    return numUnavailableSegments;
  }

  /**
   * Required temporarily until the computation of under-replicated segments is
   * moved here from DruidCoordinator.
   */
  public SegmentReplicantLookup getReplicantLookup()
  {
    return replicantLookup;
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
          eligibleLiveServers.size() >= remainingNumToDrop
          ? eligibleLiveServers.iterator()
          : runParams.getBalancerStrategy().pickServersToDrop(segment, eligibleLiveServers);
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
        runParams.getBalancerStrategy().findNewSegmentHomeReplicator(segment, eligibleServers);
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
