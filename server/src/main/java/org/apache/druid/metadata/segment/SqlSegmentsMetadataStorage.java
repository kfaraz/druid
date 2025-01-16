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
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentCreateRequest;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles all segment metadata read/write operations. If the {@link SegmentsMetadataCache}
 * is enabled and ready, the methods are served by cache-backed implementation in
 * {@link SqlSegmentsMetadataStorageWithCache}, otherwise they are served by the
 * default implementation in {@link IndexerSQLMetadataStorageCoordinator}.
 */
public class SqlSegmentsMetadataStorage implements IndexerMetadataStorageCoordinator
{
  private final SegmentsMetadataCache segmentsMetadataCache;
  private final SQLMetadataConnector connector;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  private final IndexerMetadataStorageCoordinator delegateNoCache;
  private final IndexerMetadataStorageCoordinator delegateWithCache;

  @Inject
  public SqlSegmentsMetadataStorage(
      ObjectMapper jsonMapper,
      MetadataStorageTablesConfig dbTables,
      SQLMetadataConnector connector,
      SegmentsMetadataCache segmentsMetadataCache,
      @IndexingService DruidLeaderSelector leaderSelector,
      SegmentSchemaManager segmentSchemaManager,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.connector = connector;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    this.delegateNoCache = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        dbTables,
        connector,
        segmentSchemaManager,
        centralizedDatasourceSchemaConfig
    );
    this.delegateWithCache = new SqlSegmentsMetadataStorageWithCache(
        jsonMapper,
        dbTables,
        connector,
        leaderSelector,
        segmentsMetadataCache,
        segmentSchemaManager,
        centralizedDatasourceSchemaConfig
    );
    this.segmentsMetadataCache = segmentsMetadataCache;
  }

  @LifecycleStart
  public void start()
  {
    connector.createDataSourceTable();
    connector.createPendingSegmentsTable();
    if (centralizedDatasourceSchemaConfig.isEnabled()) {
      connector.createSegmentSchemasTable();
    }
    connector.createSegmentTable();
    connector.createUpgradeSegmentsTable();
  }

  /**
   * Returns the cache-based implementation if the cache is ready. Otherwise,
   * returns the default implementation {@link IndexerSQLMetadataStorageCoordinator}.
   */
  private IndexerMetadataStorageCoordinator getDelegate()
  {
    return segmentsMetadataCache.isReady() ? delegateWithCache : delegateNoCache;
  }

  // METHODS TO READ SEGMENTS

  @Override
  public Set<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility)
  {
    return getDelegate().retrieveAllUsedSegments(dataSource, visibility);
  }

  @Override
  public Collection<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(
      String dataSource,
      List<Interval> intervals
  )
  {
    return getDelegate().retrieveUsedSegmentsAndCreatedDates(dataSource, intervals);
  }

  @Override
  public Set<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  )
  {
    return getDelegate().retrieveUsedSegmentsForIntervals(dataSource, intervals, visibility);
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return getDelegate().retrieveUnusedSegmentsForInterval(
        dataSource,
        interval,
        versions,
        limit,
        maxUsedStatusLastUpdatedTime
    );
  }

  @Override
  public Set<DataSegment> retrieveSegmentsById(String dataSource, Set<String> segmentIds)
  {
    return getDelegate().retrieveSegmentsById(dataSource, segmentIds);
  }

  @Override
  public DataSegment retrieveSegmentForId(String id, boolean includeUnused)
  {
    return getDelegate().retrieveSegmentForId(id, includeUnused);
  }

  // METHODS TO UPDATE SEGMENTS

  @Override
  public Set<DataSegment> commitSegments(
      Set<DataSegment> segments,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return getDelegate().commitSegments(segments, segmentSchemaMapping);
  }

  @Override
  public SegmentPublishResult commitSegmentsAndMetadata(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return getDelegate().commitSegmentsAndMetadata(
        segments,
        startMetadata,
        endMetadata,
        segmentSchemaMapping
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
    return getDelegate().commitAppendSegments(
        appendSegments,
        appendSegmentToReplaceLock,
        taskAllocatorId,
        segmentSchemaMapping
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
    return getDelegate().commitAppendSegmentsAndMetadata(
        appendSegments,
        appendSegmentToReplaceLock,
        startMetadata,
        endMetadata,
        taskAllocatorId,
        segmentSchemaMapping
    );
  }

  @Override
  public SegmentPublishResult commitReplaceSegments(
      Set<DataSegment> replaceSegments,
      Set<ReplaceTaskLock> locksHeldByReplaceTask,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    return getDelegate().commitReplaceSegments(
        replaceSegments,
        locksHeldByReplaceTask,
        segmentSchemaMapping
    );
  }

  @Override
  public int markSegmentsAsUnusedWithinInterval(String dataSource, Interval interval)
  {
    return getDelegate().markSegmentsAsUnusedWithinInterval(dataSource, interval);
  }

  @Override
  public int deleteSegments(Set<DataSegment> segments)
  {
    return getDelegate().deleteSegments(segments);
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
    return getDelegate().allocatePendingSegment(
        dataSource,
        interval,
        skipSegmentLineageCheck,
        createRequest
    );
  }

  @Override
  public Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests,
      boolean reduceMetadataIO
  )
  {
    return getDelegate().allocatePendingSegments(
        dataSource,
        interval,
        skipSegmentLineageCheck,
        requests,
        reduceMetadataIO
    );
  }

  @Override
  public SegmentTimeline getSegmentTimelineForAllocation(
      String dataSource,
      Interval interval,
      boolean skipSegmentPayloadFetchForAllocation
  )
  {
    return getDelegate().getSegmentTimelineForAllocation(
        dataSource,
        interval,
        skipSegmentPayloadFetchForAllocation
    );
  }

  // METHODS FOR PENDING SEGMENTS

  @Override
  public int deletePendingSegments(String dataSource)
  {
    return getDelegate().deletePendingSegments(dataSource);
  }

  @Override
  public int deletePendingSegmentsForTaskAllocatorId(String datasource, String taskAllocatorId)
  {
    return getDelegate().deletePendingSegmentsForTaskAllocatorId(datasource, taskAllocatorId);
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    return getDelegate().deletePendingSegmentsCreatedInInterval(dataSource, deleteInterval);
  }

  // METHODS THAT DO NOT USE THE SEGMENT CACHE AND ARE ALWAYS SERVED BY THE DEFAULT IMPLEMENTATION

  @Nullable
  @Override
  public DataSourceMetadata retrieveDataSourceMetadata(String dataSource)
  {
    return delegateNoCache.retrieveDataSourceMetadata(dataSource);
  }

  @Override
  public boolean deleteDataSourceMetadata(String dataSource)
  {
    return delegateNoCache.deleteDataSourceMetadata(dataSource);
  }

  @Override
  public boolean resetDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata) throws IOException
  {
    return delegateNoCache.resetDataSourceMetadata(dataSource, dataSourceMetadata);
  }

  @Override
  public boolean insertDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata)
  {
    return delegateNoCache.insertDataSourceMetadata(dataSource, dataSourceMetadata);
  }

  @Override
  public int removeDataSourceMetadataOlderThan(long timestamp, Set<String> excludeDatasources)
  {
    return delegateNoCache.removeDataSourceMetadataOlderThan(timestamp, excludeDatasources);
  }

  @Override
  public SegmentPublishResult commitMetadataOnly(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    return delegateNoCache.commitMetadataOnly(dataSource, startMetadata, endMetadata);
  }

  @Override
  public void updateSegmentMetadata(Set<DataSegment> segments)
  {
    delegateNoCache.updateSegmentMetadata(segments);
  }

  @Override
  public List<PendingSegmentRecord> getPendingSegments(String datasource, Interval interval)
  {
    // Used only in tests
    return delegateNoCache.getPendingSegments(datasource, interval);
  }

  @Override
  public Map<String, String> retrieveUpgradedFromSegmentIds(String dataSource, Set<String> segmentIds)
  {
    // Contents of upgrade_segments table are currently not being cached
    return delegateNoCache.retrieveUpgradedFromSegmentIds(dataSource, segmentIds);
  }

  @Override
  public Map<String, Set<String>> retrieveUpgradedToSegmentIds(String dataSource, Set<String> segmentIds)
  {
    // Contents of upgrade_segments table are currently not being cached
    return delegateNoCache.retrieveUpgradedToSegmentIds(dataSource, segmentIds);
  }

  @Override
  public int deleteUpgradeSegmentsForTask(String taskId)
  {
    // Contents of upgrade_segments table are currently not being cached
    return delegateNoCache.deleteUpgradeSegmentsForTask(taskId);
  }
}
