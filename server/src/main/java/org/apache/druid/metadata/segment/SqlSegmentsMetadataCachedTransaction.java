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

/**
 * A {@link SqlSegmentsMetadataTransaction} that performs reads using the cache
 * and sends writes first to the metadata store and then the cache (if the
 * metadata store persist succeeds).
 */
public class SqlSegmentsMetadataCachedTransaction implements SqlSegmentsMetadataTransaction
{
  private final String dataSource;
  private final SqlSegmentsMetadataTransaction delegate;
  private final SegmentsMetadataCache metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;

  public SqlSegmentsMetadataCachedTransaction(
      String dataSource,
      SqlSegmentsMetadataTransaction delegate,
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
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    return metadataCache.findExistingSegmentIds(dataSource, segments);
  }

  @Override
  public Set<SegmentId> findUsedSegmentIds(Interval interval)
  {
    return metadataCache.findUsedSegmentIds(dataSource, interval);
  }

  @Override
  public List<DataSegmentPlus> findSegments(Set<String> segmentIds)
  {
    // TODO
    return null;
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds)
  {
    // TODO
    return List.of();
  }

  @Override
  public CloseableIterator<DataSegment> findUsedSegments(List<Interval> intervals)
  {
    // TODO: implement this
    return null;
  }

  @Override
  public Set<DataSegmentPlus> findUsedSegmentsPlus(List<Interval> intervals)
  {
    // TODO: implement this
    return null;
  }

  @Override
  public List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    // TODO
    return null;
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    // TODO
    return null;
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    // TODO
    return null;
  }

  @Override
  public void insertSegments(Set<DataSegmentPlus> segments)
  {
    verifyStillLeaderWithSameTerm();
    delegate.insertSegments(segments);

    if (isLeaderWithSameTerm() && !segments.isEmpty()) {
      metadataCache.addSegments(getDataSourceName(segments), segments);
    }
  }

  @Override
  public void insertSegmentsWithMetadata(Set<DataSegmentPlus> segments)
  {
    verifyStillLeaderWithSameTerm();
    delegate.insertSegmentsWithMetadata(segments);

    if (isLeaderWithSameTerm() && !segments.isEmpty()) {
      metadataCache.addSegments(getDataSourceName(segments), segments);
    }
  }

  @Override
  public int markSegmentsUnused(Interval interval)
  {
    // TODO
    return 0;
  }

  @Override
  public void updateSegmentPayload(DataSegment segment)
  {
    // TODO
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
  public List<PendingSegmentRecord> findPendingSegmentsOverlappingInterval(Interval interval)
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
  public int deletePendingSegments(List<String> segmentIdsToDelete)
  {
    // TODO
    return 0;
  }

  private static String getDataSourceName(Set<DataSegmentPlus> segments)
  {
    return segments.stream().findFirst().map(s -> s.getDataSegment().getDataSource()).orElse(null);
  }
}
