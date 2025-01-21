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

package org.apache.druid.metadata.segment;

import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link SegmentsMetadataTransaction} that performs reads using the cache
 * and sends writes first to the metadata store and then the cache (if the
 * metadata store persist succeeds).
 */
public class SqlSegmentsMetadataCachedTransaction implements SegmentsMetadataTransaction
{
  private final String dataSource;
  private final SegmentsMetadataTransaction delegate;
  private final SegmentsMetadataCache metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;

  private final AtomicBoolean isRollingBack = new AtomicBoolean(false);

  public SqlSegmentsMetadataCachedTransaction(
      String dataSource,
      SegmentsMetadataTransaction delegate,
      SegmentsMetadataCache metadataCache,
      DruidLeaderSelector leaderSelector
  )
  {
    this.dataSource = dataSource;
    this.delegate = delegate;
    this.metadataCache = metadataCache;
    this.leaderSelector = leaderSelector;

    if (leaderSelector.isLeader()) {
      throw InternalServerError.exception("Not leader anymore");
    } else {
      this.startTerm = leaderSelector.localTerm();
    }
  }

  private void verifyStillLeaderWithSameTerm()
  {
    if (!isLeaderWithSameTerm()) {
      throw InternalServerError.exception("Failing transaction. Not leader anymore");
    }
  }

  private boolean isLeaderWithSameTerm()
  {
    return leaderSelector.isLeader() && startTerm == leaderSelector.localTerm();
  }

  private DatasourceSegmentMetadataReader cacheReader()
  {
    return metadataCache.readerForDatasource(dataSource);
  }

  private DatasourceSegmentMetadataWriter cacheWriter()
  {
    return metadataCache.writerForDatasource(dataSource);
  }

  @Override
  public Handle getHandle()
  {
    return delegate.getHandle();
  }

  @Override
  public void setRollbackOnly()
  {
    delegate.setRollbackOnly();
  }

  @Override
  public void complete()
  {
    if (isRollingBack.get()) {
      // rollback the changes made to the cache
    } else {
      // commit the changes to the cache
    }

    delegate.complete();
  }

  // READ METHODS

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return cacheReader().findExistingSegmentIds(segments);
  }

  @Override
  public Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval)
  {
    return cacheReader().findUsedSegmentIdsOverlapping(interval);
  }

  @Override
  public Set<String> findUnusedSegmentIdsWithExactIntervalAndVersion(Interval interval, String version)
  {
    // TODO: we need to start caching some info of unused segments to empower this method
    return delegate.findUnusedSegmentIdsWithExactIntervalAndVersion(interval, version);
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<String> segmentIds)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegments(segmentIds);
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegmentsWithSchema(segmentIds);
  }

  @Override
  public CloseableIterator<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals)
  {
    return cacheReader().findUsedSegmentsOverlappingAnyOf(intervals);
  }

  @Override
  public List<DataSegment> findUsedSegments(Set<String> segmentIds)
  {
    return cacheReader().findUsedSegments(segmentIds);
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals)
  {
    return cacheReader().findUsedSegmentsPlusOverlappingAnyOf(intervals);
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findUnusedSegments(interval, versions, limit, maxUsedStatusLastUpdatedTime);
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    // Read from metadata store since unused segment payloads are not cached
    return delegate.findSegment(segmentId);
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    return cacheReader().findUsedSegment(segmentId);
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  )
  {
    // TODO
    return List.of();
  }

  @Override
  public List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  )
  {
    // TODO
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval)
  {
    // TODO
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval)
  {
    // TODO
    return List.of();
  }

  @Override
  public List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId)
  {
    // TODO
    return List.of();
  }

  // WRITE METHODS

  @Override
  public void insertSegments(Set<DataSegmentPlus> segments)
  {
    verifyStillLeaderWithSameTerm();
    delegate.insertSegments(segments);

    if (isLeaderWithSameTerm() && !segments.isEmpty()) {
      cacheWriter().insertSegments(segments);
    }
  }

  @Override
  public void insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    verifyStillLeaderWithSameTerm();
    delegate.insertSegmentsWithMetadata(segments);

    if (isLeaderWithSameTerm() && !segments.isEmpty()) {
      cacheWriter().insertSegmentsWithMetadata(segments);
    }
  }

  @Override
  public int markSegmentsUnused(Interval interval)
  {
    verifyStillLeaderWithSameTerm();
    int updatedCount = delegate.markSegmentsUnused(interval);

    if (isLeaderWithSameTerm()) {
      cacheWriter().markSegmentsUnused(interval);
    }

    return updatedCount;
  }

  @Override
  public int deleteSegments(Set<DataSegment> segments)
  {
    verifyStillLeaderWithSameTerm();
    int deletedCount = delegate.deleteSegments(segments);

    if (isLeaderWithSameTerm()) {
      cacheWriter().deleteSegments(segments);
    }

    return deletedCount;
  }

  @Override
  public void updateSegmentPayload(DataSegment segment)
  {
    // TODO
  }

  @Override
  public void insertPendingSegment(
      PendingSegmentRecord pendingSegment,
      boolean skipSegmentLineageCheck
  )
  {

  }

  @Override
  public int insertPendingSegments(
      List<PendingSegmentRecord> pendingSegments,
      boolean skipSegmentLineageCheck
  )
  {
    // TODO
    return 0;
  }

  @Override
  public int deleteAllPendingSegments()
  {
    return 0;
  }

  @Override
  public int deletePendingSegments(List<String> segmentIdsToDelete)
  {
    // TODO
    return 0;
  }

  @Override
  public int deletePendingSegments(String taskAllocatorId)
  {
    return 0;
  }

  @Override
  public int deletePendingSegmentsCreatedIn(Interval interval)
  {
    return 0;
  }
}
