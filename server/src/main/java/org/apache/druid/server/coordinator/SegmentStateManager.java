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

import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

/**
 * Manages state of segments being loaded.
 */
public class SegmentStateManager
{
  // TODO
  //  2. all callbacks
  //  4. load queue modes

  // TODO:
  //  replicant lookup
  //  currently replicating segments
  //  currently moving segments
  //  load queue peons?
  //  what about the callbacks to different load/drop actions?

  //  this can report metrics
  //  this should determine if servers are to be blacklisted??
  //  this is where the problem starts and I feel like we are back where we started!

  private final boolean isHttpLoading;
  private final ServerInventoryView serverInventoryView;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ReplicationThrottler replicationThrottler = new ReplicationThrottler();

  private volatile SegmentReplicantLookup replicantLookup;

  public SegmentStateManager(
      ServerInventoryView serverInventoryView,
      SegmentsMetadataManager segmentsMetadataManager,
      boolean isHttpLoading
  )
  {
    this.serverInventoryView = serverInventoryView;
    this.segmentsMetadataManager = segmentsMetadataManager;
    this.isHttpLoading = isHttpLoading;
  }

  public void prepareForRun(DruidCoordinatorRuntimeParams runtimeParams)
  {
    final CoordinatorDynamicConfig dynamicConfig = runtimeParams.getCoordinatorDynamicConfig();
    replicationThrottler.resetParams(
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
    replicationThrottler.updateReplicationState(runtimeParams.getDruidCluster().getTierNames());

    this.replicantLookup = runtimeParams.getSegmentReplicantLookup();
  }

  /**
   * Queues load of a replica of the segment on the given server.
   */
  public boolean loadSegment(DataSegment segment, ServerHolder server, boolean isPrimary)
  {
    final String tier = server.getServer().getTier();
    if (isPrimary) {
      // Primary replicas are not subject to throttling
    } else if (replicationThrottler.canCreateReplicant(tier)) {
      replicationThrottler.registerReplicantCreation(tier, segment.getId(), server.getServer().getHost());
    } else {
      return false;
    }

    // TODO: update state in serverholder
    // TODO: unregister replicant creation
    server.getPeon().loadSegment(segment, null);
    return true;
  }

  public boolean dropSegment(DataSegment segment, ServerHolder server)
  {
    // TODO: update state in serverHolder
    server.getPeon().dropSegment(segment, null);
    // TODO: callbacks, if any
    return true;
  }

  public boolean moveSegment(
      DataSegment segment,
      ServerHolder fromServer,
      ServerHolder toServer
  )
  {
    final LoadQueuePeon dropPeon = fromServer.getPeon();
    final LoadPeonCallback loadPeonCallback = success -> {
      dropPeon.unmarkSegmentToDrop(segment);
      // TODO: update currentlyMovingSegments here
    };

    // mark segment to drop before it is actually loaded on server
    // to be able to account for this information in BalancerStrategy immediately
    // TODO: add to currentlyMovingSegments here
    dropPeon.markSegmentToDrop(segment);

    final LoadQueuePeon loadPeon = toServer.getPeon();
    final String destServerName = toServer.getServer().getName();
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

  /**
   * Marks the given segment as unused.
   */
  public boolean deleteSegment(DataSegment segment)
  {
    return segmentsMetadataManager.markSegmentAsUnused(segment.getId());
  }

  /**
   * Required temporarily until the computation of under-replicated segments is
   * moved here from DruidCoordinator.
   */
  public SegmentReplicantLookup getReplicantLookup()
  {
    return replicantLookup;
  }

  public Map<String, Integer> getUnavailableSegmentCountPerDatasource()
  {
    if (replicantLookup == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> numUnavailableSegments = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    for (DataSegment segment : dataSegments) {
      if (replicantLookup.getLoadedReplicants(segment.getId()) == 0) {
        numUnavailableSegments.addTo(segment.getDataSource(), 1);
      } else {
        numUnavailableSegments.addTo(segment.getDataSource(), 0);
      }
    }

    return numUnavailableSegments;
  }

}
