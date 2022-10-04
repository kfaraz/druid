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

import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class ServerHolder implements Comparable<ServerHolder>
{
  private static final Logger log = new Logger(ServerHolder.class);
  private final ImmutableDruidServer server;
  private final LoadQueuePeon peon;
  private final int maxLoadQueueSize;
  private final boolean isDecommissioning;

  private int segmentsQueuedForLoad = 0;

  private final ConcurrentMap<SegmentId, SegmentState> segmentStates = new ConcurrentHashMap<>();

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon)
  {
    this(server, peon, 0, false);
  }

  public ServerHolder(ImmutableDruidServer server, LoadQueuePeon peon, boolean isDecommissioning)
  {
    this(server, peon, 0, isDecommissioning);
  }

  public ServerHolder(
      ImmutableDruidServer server,
      LoadQueuePeon peon,
      int maxLoadQueueSize,
      boolean isDecommissioning
  )
  {
    this.server = server;
    this.peon = peon;
    this.isDecommissioning = isDecommissioning;
    this.maxLoadQueueSize = maxLoadQueueSize;

    server.iterateAllSegments().forEach(
        segment -> segmentStates.put(segment.getId(), SegmentState.LOADED)
    );
  }

  public ImmutableDruidServer getServer()
  {
    return server;
  }

  public LoadQueuePeon getPeon()
  {
    return peon;
  }

  public long getMaxSize()
  {
    return server.getMaxSize();
  }

  public long getCurrServerSize()
  {
    return server.getCurrSize();
  }

  public long getLoadQueueSize()
  {
    return peon.getLoadQueueSize();
  }

  public long getSizeUsed()
  {
    return getCurrServerSize() + getLoadQueueSize();
  }

  public double getPercentUsed()
  {
    return (100.0 * getSizeUsed()) / getMaxSize();
  }

  /**
   * Historical nodes can be 'decommissioned', which instructs Coordinator to move segments from them according to
   * the percent of move operations diverted from normal balancer moves for this purpose by
   * {@link CoordinatorDynamicConfig#getDecommissioningMaxPercentOfMaxSegmentsToMove()}. The mechanism allows draining
   * segments from nodes which are planned for replacement.
   *
   * @return true if the node is decommissioning
   */
  public boolean isDecommissioning()
  {
    return isDecommissioning;
  }

  public long getAvailableSize()
  {
    long maxSize = getMaxSize();
    long sizeUsed = getSizeUsed();
    long availableSize = maxSize - sizeUsed;

    log.debug(
        "Server[%s], MaxSize[%,d], CurrSize[%,d], QueueSize[%,d], SizeUsed[%,d], AvailableSize[%,d]",
        server.getName(),
        maxSize,
        getCurrServerSize(),
        getLoadQueueSize(),
        sizeUsed,
        availableSize
    );

    return availableSize;
  }

  /**
   * Checks if this server can load the specified segment.
   * <p>
   * A load is possible only if the server meets all of the following criteria:
   * <ul>
   *   <li>is not already serving or loading the segment</li>
   *   <li>is not being decommissioned</li>
   *   <li>has not already exceeded the load queue limit in this run</li>
   *   <li>has available disk space</li>
   * </ul>
   */
  public boolean canLoadSegment(DataSegment segment)
  {
    final SegmentState state = getSegmentState(segment);

    return !isDecommissioning
           && (maxLoadQueueSize == 0 || maxLoadQueueSize > segmentsQueuedForLoad)
           && getAvailableSize() >= segment.getSize()
           && state == SegmentState.NONE;
  }

  public SegmentState getSegmentState(DataSegment segment)
  {
    return segmentStates.getOrDefault(segment.getId(), SegmentState.NONE);
  }

  public boolean isServingSegment(DataSegment segment)
  {
    return getSegmentState(segment) == SegmentState.LOADED;
  }

  public boolean isLoadingSegment(DataSegment segment)
  {
    return getSegmentState(segment) == SegmentState.LOADING;
  }

  public boolean isDroppingSegment(DataSegment segment)
  {
    return getSegmentState(segment) == SegmentState.DROPPING;
  }

  public void loadSegment(DataSegment segment)
  {
    if (!canLoadSegment(segment)) {
      throw new ISE("Cannot load segment because ...");
    }

    segmentStates.put(segment.getId(), SegmentState.LOADING);
    peon.dropSegment(segment, success ->
        segmentStates.put(
            segment.getId(),
            success ? SegmentState.LOADED : SegmentState.NONE
        )
    );
    segmentsQueuedForLoad++;
  }

  /**
   * Let's assume that this was not called in a callback of balancing.
   * TODO: We will handle this later.
   */
  public void dropSegment(DataSegment segment)
  {
    if (!isServingSegment(segment)) {
      throw new ISE("Only a loaded segment can be queued for drop");
    }

    segmentStates.put(segment.getId(), SegmentState.DROPPING);
    peon.dropSegment(segment, success ->
        segmentStates.put(
            segment.getId(),
            success ? SegmentState.NONE : SegmentState.LOADED
        )
    );
  }

  public boolean cancelSegmentOperation(SegmentState expectedState, DataSegment segment)
  {
    SegmentState currentState = segmentStates.get(segment.getId());
    if (currentState != expectedState) {
      return false;
    }

    final boolean success;
    switch (expectedState) {
      case LOADING:
      case MOVING_TO:
        success = peon.cancelLoad(segment);
        break;
      case DROPPING:
        success = peon.cancelDrop(segment);
        break;
      default:
        success = false;
    }
    return success;
  }

  public int getNumberOfSegmentsInQueue()
  {
    return peon.getNumberOfSegmentsInQueue();
  }

  public boolean isServingSegment(SegmentId segmentId)
  {
    return segmentStates.get(segmentId) == SegmentState.LOADED;
  }

  @Override
  public int compareTo(ServerHolder serverHolder)
  {
    int result = Long.compare(getAvailableSize(), serverHolder.getAvailableSize());
    if (result != 0) {
      return result;
    }

    result = server.getHost().compareTo(serverHolder.server.getHost());
    if (result != 0) {
      return result;
    }

    result = server.getTier().compareTo(serverHolder.server.getTier());
    if (result != 0) {
      return result;
    }

    return server.getType().compareTo(serverHolder.server.getType());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ServerHolder that = (ServerHolder) o;

    if (!this.server.getHost().equals(that.server.getHost())) {
      return false;
    }

    if (!this.server.getTier().equals(that.getServer().getTier())) {
      return false;
    }

    return this.server.getType().equals(that.getServer().getType());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(server.getHost(), server.getTier(), server.getType());
  }
}
