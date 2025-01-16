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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Implementation of {@link IndexerMetadataStorageCoordinator} backed by an in-memory
 * {@link SegmentsMetadataCache}.
 * <p>
 * While the cache is being read by a thread, no other thread should update it.
 * This is currently ensured by invoking all the methods of this class after
 * acquiring the {@code giant} lock in {@code TaskLockbox}.
 *
 * TODO:
 *   - implement segment allocate methods
 *   - implement segment read methods
 *   - implement pending segment read methods
 *   - implement pending segment write methods
 */
public class SqlSegmentsMetadataStorageWithCache extends IndexerSQLMetadataStorageCoordinator
{
  private final SegmentsMetadataCache segmentsMetadataCache;
  private final DruidLeaderSelector leaderSelector;

  public SqlSegmentsMetadataStorageWithCache(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      DruidLeaderSelector leaderSelector,
      SegmentsMetadataCache segmentsMetadataCache,
      SegmentSchemaManager segmentSchemaManager,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    super(jsonMapper, dbTables, connector, segmentSchemaManager, centralizedDatasourceSchemaConfig);
    this.segmentsMetadataCache = segmentsMetadataCache;
    this.leaderSelector = leaderSelector;
  }

  @Override
  public Set<DataSegment> commitSegments(
      Set<DataSegment> segments,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    final SegmentPublishResult result =
        commitSegmentsAndMetadata(segments, null, null, segmentSchemaMapping);
    if (!result.isSuccess()) {
      throw DruidException.defensive(
          "Could not commit [%d] segments: %s",
          segments.size(), result.getErrorMsg()
      );
    }

    return result.getSegments();
  }

  @Override
  public SegmentPublishResult commitSegmentsAndMetadata(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitSegmentsAndUpdateCache(
        () -> super.commitSegmentsAndMetadata(
            segments,
            startMetadata,
            endMetadata,
            segmentSchemaMapping
        )
    );
  }

  @Override
  public SegmentPublishResult commitAppendSegments(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitSegmentsAndUpdateCache(
        // TODO: this method also calls getPendingSegmentsForTaskAllocatorId(handle)
        //    which may be served from cache
        () -> super.commitAppendSegments(
            appendSegments,
            appendSegmentToReplaceLock,
            taskAllocatorId,
            segmentSchemaMapping
        )
    );
  }

  @Override
  public SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitSegmentsAndUpdateCache(
        () -> super.commitAppendSegmentsAndMetadata(
            appendSegments,
            appendSegmentToReplaceLock,
            startMetadata,
            endMetadata,
            taskAllocatorId,
            segmentSchemaMapping
        )
    );
  }

  @Override
  public SegmentPublishResult commitReplaceSegments(
      Set<DataSegment> replaceSegments,
      Set<ReplaceTaskLock> locksHeldByReplaceTask,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return commitSegmentsAndUpdateCache(
        // TODO: this method also calls retrieveSegmentsById(handle)
        //    which may be served from cache
        () -> super.commitReplaceSegments(replaceSegments, locksHeldByReplaceTask, segmentSchemaMapping)
    );
  }

  @Override
  public int markSegmentsAsUnusedWithinInterval(String dataSource, Interval interval)
  {
    // TODO: finish implementation
    return super.markSegmentsAsUnusedWithinInterval(dataSource, interval);
  }

  @Override
  public int deleteSegments(Set<DataSegment> segments)
  {
    final int startTerm = getLeaderTermOrFail();

    final int numDeletedSegments = super.deleteSegments(segments);
    if (startTerm == getLeaderTerm()) {
      segmentsMetadataCache.deleteSegments(segments);
    }

    return numDeletedSegments;
  }

  // METHODS FOR SEGMENT ALLOCATION

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocatePendingSegment(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      SegmentCreateRequest createRequest
  )
  {
    return super.allocatePendingSegment(dataSource, interval, skipSegmentLineageCheck, createRequest);
  }


  // Methods for pending segments

  @Override
  public int deletePendingSegments(String dataSource)
  {
    final int startTerm = getLeaderTermOrFail();

    final int numDeletedSegments = super.deletePendingSegments(dataSource);
    if (startTerm == getLeaderTerm()) {
      segmentsMetadataCache.deletePendingSegments(dataSource);
    }

    return numDeletedSegments;
  }

  @Override
  public int deletePendingSegmentsForTaskAllocatorId(String datasource, String taskAllocatorId)
  {
    final int startTerm = getLeaderTermOrFail();

    final int numDeletedSegments = super.deletePendingSegmentsForTaskAllocatorId(datasource, taskAllocatorId);
    if (startTerm == getLeaderTerm()) {
      segmentsMetadataCache.deletePendingSegments(datasource, taskAllocatorId);
    }

    return numDeletedSegments;
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    final int startTerm = getLeaderTermOrFail();

    final int numDeletedSegments = super.deletePendingSegmentsCreatedInInterval(dataSource, deleteInterval);
    if (startTerm == getLeaderTerm()) {
      segmentsMetadataCache.deletePendingSegmentsCreatedIn(dataSource, deleteInterval);
    }

    return numDeletedSegments;
  }

  private SegmentPublishResult commitSegmentsAndUpdateCache(
      Supplier<SegmentPublishResult> commitAction
  )
  {
    final int startTerm = getLeaderTermOrFail();
    final SegmentPublishResult result = commitAction.get();

    // Update the cache only if leadership did not change during the commit
    if (result.isSuccess() && startTerm == getLeaderTerm()) {
      // TODO: should these two be done atomically?
      //  yeah, I think that would be best
      //  take a single write lock on the datasource and do all that is necessary
      segmentsMetadataCache.commitSegments(result.getSegments());
      segmentsMetadataCache.commitPendingSegments(result.getUpgradedPendingSegments());

      // TODO: commitAppendSegments also deletes pending segments for each ID that
      //  is now being inserted as a full segment
    }

    return result;
  }

  /**
   * Returns the current leader term if we are the leader.
   *
   * @throws DruidException if we are not the leader.
   */
  private int getLeaderTermOrFail()
  {
    if (leaderSelector.isLeader()) {
      return leaderSelector.localTerm();
    } else {
      throw InternalServerError.exception("Not leader anymore");
    }
  }

  /**
   * Returns the current leader term if we are the leader, -1 otherwise.
   */
  private int getLeaderTerm()
  {
    return leaderSelector.isLeader() ? leaderSelector.localTerm() : -1;
  }
}
