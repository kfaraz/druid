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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.segment.DatasourceReadTransaction;
import org.apache.druid.metadata.segment.DatasourceWriteTransaction;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * TODO: implement all remaining methods
 */
class DatasourceSegmentCache
    extends BaseCache
    implements DatasourceReadTransaction, DatasourceWriteTransaction
{
  private static final DatasourceSegmentCache EMPTY_INSTANCE = new DatasourceSegmentCache();

  /**
   * Used to obtain the segment for a given ID so that it can be updated in the
   * timeline.
   */
  private final Map<String, DataSegment> idToUsedSegment = new HashMap<>();

  /**
   * Current state of segments as seen by the cache.
   */
  private final Map<String, SegmentState> idToSegmentState = new HashMap<>();

  /**
   * Allows lookup of segments by interval.
   */
  private final SegmentTimeline usedSegmentTimeline = SegmentTimeline.forSegments(Set.of());

  private final Set<String> unusedSegmentIds = new HashSet<>();

  static DatasourceSegmentCache empty()
  {
    return EMPTY_INSTANCE;
  }

  DatasourceSegmentCache()
  {
    super(true);
  }

  void clear()
  {
    withWriteLock(() -> {
      idToSegmentState.clear();
      idToUsedSegment.clear();
      unusedSegmentIds.clear();
      idToUsedSegment.values().forEach(usedSegmentTimeline::remove);
    });
  }

  /**
   * Checks if a segment needs to be refreshed. A refresh is required if the
   * cache has no known state for the given segment or if the metadata store
   * has a more recent last_updated_time than the cache.
   */
  boolean shouldRefreshSegment(SegmentState metadataState)
  {
    return withReadLock(() -> {
      final SegmentState cachedState = idToSegmentState.get(metadataState.getSegmentId());
      return cachedState == null
             || cachedState.getLastUpdatedTime().isBefore(metadataState.getLastUpdatedTime());
    });
  }

  boolean refreshUnusedSegment(SegmentState newState)
  {
    if (newState.isUsed()) {
      return false;
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(newState.getSegmentId(), newState);

      if (oldState != null && oldState.isUsed()) {
        // Segment has transitioned from used to unused
        DataSegment segment = idToUsedSegment.remove(newState.getSegmentId());
        if (segment != null) {
          usedSegmentTimeline.remove(segment);
        }
      }

      unusedSegmentIds.add(newState.getSegmentId());
      return true;
    });
  }

  boolean refreshUsedSegment(DataSegmentPlus segmentPlus)
  {
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentState newState = new SegmentState(
        segment.getId().toString(),
        segment.getDataSource(),
        Boolean.TRUE.equals(segmentPlus.getUsed()),
        segmentPlus.getUsedStatusLastUpdatedDate()
    );
    if (!newState.isUsed()) {
      return refreshUnusedSegment(newState);
    }

    return withWriteLock(() -> {
      if (!shouldRefreshSegment(newState)) {
        return false;
      }

      final SegmentState oldState = idToSegmentState.put(newState.getSegmentId(), newState);
      final DataSegment oldSegment = idToUsedSegment.put(newState.getSegmentId(), segment);

      if (oldState == null) {
        // This is a new segment
      } else if (oldState.isUsed()) {
        // Segment payload may have changed
        if (oldSegment != null) {
          usedSegmentTimeline.remove(oldSegment);
        }
      } else {
        // Segment has transitioned from unused to used
        unusedSegmentIds.remove(newState.getSegmentId());
      }

      usedSegmentTimeline.add(segment);
      return true;
    });
  }

  int removeSegmentIds(Set<String> segmentIds)
  {
    return withWriteLock(() -> {
      int removedCount = 0;
      for (String segmentId : segmentIds) {
        SegmentState state = idToSegmentState.remove(segmentId);
        if (state != null) {
          ++removedCount;
        }

        unusedSegmentIds.remove(segmentId);

        final DataSegment segment = idToUsedSegment.remove(segmentId);
        if (segment != null) {
          usedSegmentTimeline.remove(segment);
        }
      }

      return removedCount;
    });
  }

  /**
   * Returns the set of segment IDs present in the cache but not present in the
   * given set of known segment IDs.
   */
  Set<String> getSegmentIdsNotIn(Set<String> knownSegmentIds)
  {
    return withReadLock(
        () -> knownSegmentIds.stream()
                             .filter(id -> !idToSegmentState.containsKey(id))
                             .collect(Collectors.toSet())
    );
  }

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    final Set<String> segmentIdsToFind = segments.stream()
                                                 .map(s -> s.getId().toString())
                                                 .collect(Collectors.toSet());

    return withReadLock(
        () -> segmentIdsToFind.stream()
                              .filter(idToSegmentState::containsKey)
                              .collect(Collectors.toSet())
    );
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return withReadLock(
        () -> idToUsedSegment.values()
                             .stream()
                             .filter(s -> s.getInterval().overlaps(interval))
                             .map(DataSegment::getId)
                             .collect(Collectors.toSet())
    );
  }

  @Override
  public CloseableIterator<DataSegment> findUsedSegments(List<Interval> intervals)
  {
    return null;
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlus(List<Interval> intervals)
  {
    return Set.of();
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    return null;
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    return null;
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<String> segmentIds)
  {
    return List.of();
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds)
  {
    return List.of();
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return List.of();
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(String sequenceName, String sequencePreviousId)
  {
    return List.of();
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(String sequenceName, Interval interval)
  {
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    return List.of();
  }

  @Override
  public void insertSegments(Set<DataSegmentPlus> segments)
  {
    withWriteLock(() -> {
      for (DataSegmentPlus segmentPlus : segments) {
        final DataSegment segment = segmentPlus.getDataSegment();
        final SegmentState state = new SegmentState(
            segment.getId().toString(),
            segment.getDataSource(),
            Boolean.TRUE.equals(segmentPlus.getUsed()),
            segmentPlus.getUsedStatusLastUpdatedDate()
        );

        if (state.isUsed()) {
          refreshUsedSegment(segmentPlus);
        } else {
          refreshUnusedSegment(state);
        }
      }
    });
  }

  @Override
  public void insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    insertSegments(segments);
  }

  @Override
  public int markSegmentsUnused(Interval interval)
  {
    return 0;
  }

  @Override
  public void updateSegmentPayload(DataSegment segment)
  {

  }

  @Override
  public void insertPendingSegment(PendingSegmentRecord pendingSegment, boolean skipSegmentLineageCheck)
  {

  }

  @Override
  public int insertPendingSegments(List<PendingSegmentRecord> pendingSegments, boolean skipSegmentLineageCheck)
  {
    return 0;
  }

  @Override
  public int deletePendingSegments(List<String> segmentIdsToDelete)
  {
    return 0;
  }
}
