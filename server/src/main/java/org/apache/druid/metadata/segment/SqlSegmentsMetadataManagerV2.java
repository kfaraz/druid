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
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SortOrder;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Implementation V2 of {@link SegmentsMetadataManager}, that uses the segments
 * in {@link SegmentMetadataCache} to build a timeline.
 * <p>
 * This class acts as a wrapper over {@link SqlSegmentsMetadataManager} and the
 * {@link SegmentMetadataCache}. If the cache is enabled, an additional poll is
 * not done and the segments already present in the cache are used to build the
 * timeline. If the {@code SegmentMetadataCache} is disabled, the polling is
 * delegated to the legacy implementation in {@link SqlSegmentsMetadataManager}.
 *
 * TODO:
 * - mention how the behaviour of this class is different for Coordinator and Overlord
 * - should changes in leadership affect the state of this class
 */
@ManageLifecycle
public class SqlSegmentsMetadataManagerV2 implements SegmentsMetadataManager
{
  private static final Logger log = new Logger(SqlSegmentsMetadataManagerV2.class);

  private final SegmentsMetadataManager delegate;
  private final SegmentMetadataCache segmentMetadataCache;
  private final CentralizedDatasourceSchemaConfig schemaConfig;

  public SqlSegmentsMetadataManagerV2(
      SegmentMetadataCache segmentMetadataCache,
      SegmentSchemaCache segmentSchemaCache,
      SQLMetadataConnector connector,
      Supplier<SegmentsMetadataManagerConfig> managerConfig,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      ServiceEmitter serviceEmitter,
      ObjectMapper jsonMapper
  )
  {
    this.delegate = new SqlSegmentsMetadataManager(
        jsonMapper,
        managerConfig, tablesConfig, connector, segmentSchemaCache,
        centralizedDatasourceSchemaConfig, serviceEmitter
    );
    this.segmentMetadataCache = segmentMetadataCache;
    this.schemaConfig = centralizedDatasourceSchemaConfig;
  }

  /**
   * @return true if segment metadata cache is enabled and segment schema cache
   * is not enabled. Segment metadata cache currently does not handle segment
   * schema updates.
   */
  private boolean useSegmentCache()
  {
    return segmentMetadataCache.isEnabled() && !schemaConfig.isEnabled();
  }

  @Override
  @LifecycleStart
  public void start()
  {
    delegate.start();
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    delegate.stop();
  }

  @Override
  public void startPollingDatabasePeriodically()
  {
    if (useSegmentCache()) {
      log.info("Not polling metadata store directly. Using segments in metadata cache to build timeline.");
    } else {
      delegate.startPollingDatabasePeriodically();
    }
  }

  @Override
  public void stopPollingDatabasePeriodically()
  {
    if (useSegmentCache()) {
      // Cache has its own lifecycle stop
    } else {
      delegate.stopPollingDatabasePeriodically();
    }
  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    return useSegmentCache() || delegate.isPollingDatabasePeriodically();
  }

  @Nullable
  @Override
  public ImmutableDruidDataSource getImmutableDataSourceWithUsedSegments(String dataSource)
  {
    if (useSegmentCache()) {
      return getSnapshotOfDataSourcesWithAllUsedSegments().getDataSource(dataSource);
    } else {
      return delegate.getImmutableDataSourceWithUsedSegments(dataSource);
    }
  }

  @Override
  public Collection<ImmutableDruidDataSource> getImmutableDataSourcesWithAllUsedSegments()
  {
    if (useSegmentCache()) {
      return getSnapshotOfDataSourcesWithAllUsedSegments().getDataSourcesWithAllUsedSegments();
    } else {
      return delegate.getImmutableDataSourcesWithAllUsedSegments();
    }
  }

  @Override
  public DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments()
  {
    if (useSegmentCache()) {
      return segmentMetadataCache.getDatasourcesSnapshot();
    } else {
      return delegate.getSnapshotOfDataSourcesWithAllUsedSegments();
    }
  }

  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    if (useSegmentCache()) {
      return getSnapshotOfDataSourcesWithAllUsedSegments().iterateAllUsedSegmentsInSnapshot();
    } else {
      return delegate.iterateAllUsedSegments();
    }
  }

  @Override
  public Optional<Iterable<DataSegment>> iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
      String datasource,
      Interval interval,
      boolean requiresLatest
  )
  {
    if (useSegmentCache()) {
      // Ignore the flag requiresLatest since the cache polls the metadata store
      // continuously anyway
      SegmentTimeline usedSegmentsTimeline =
          getSnapshotOfDataSourcesWithAllUsedSegments()
              .getUsedSegmentsTimelinesPerDataSource()
              .get(datasource);
      return Optional.fromNullable(usedSegmentsTimeline).transform(
          timeline -> timeline.findNonOvershadowedObjectsInInterval(interval, Partitions.ONLY_COMPLETE)
      );
    } else {
      return delegate.iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(datasource, interval, requiresLatest);
    }
  }

  // Methods delegated to SqlSegmentsMetadataManager V1 implementation

  @Override
  public int markAsUsedAllNonOvershadowedSegmentsInDataSource(String dataSource)
  {
    return delegate.markAsUsedAllNonOvershadowedSegmentsInDataSource(dataSource);
  }

  @Override
  public int markAsUsedNonOvershadowedSegmentsInInterval(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions
  )
  {
    return delegate.markAsUsedNonOvershadowedSegmentsInInterval(dataSource, interval, versions);
  }

  @Override
  public int markAsUsedNonOvershadowedSegments(String dataSource, Set<SegmentId> segmentIds)
  {
    return delegate.markAsUsedNonOvershadowedSegments(dataSource, segmentIds);
  }

  @Override
  public boolean markSegmentAsUsed(String segmentId)
  {
    return delegate.markSegmentAsUsed(segmentId);
  }

  @Override
  public Iterable<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      String datasource,
      @Nullable Interval interval,
      @Nullable Integer limit,
      @Nullable String lastSegmentId,
      @Nullable SortOrder sortOrder
  )
  {
    return delegate.iterateAllUnusedSegmentsForDatasource(
        datasource,
        interval,
        limit,
        lastSegmentId,
        sortOrder
    );
  }

  @Override
  public Set<String> retrieveAllDataSourceNames()
  {
    return delegate.retrieveAllDataSourceNames();
  }

  @Override
  public List<Interval> getUnusedSegmentIntervals(
      String dataSource,
      @Nullable DateTime minStartTime,
      DateTime maxEndTime,
      int limit,
      DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return delegate.getUnusedSegmentIntervals(
        dataSource,
        minStartTime,
        maxEndTime,
        limit,
        maxUsedStatusLastUpdatedTime
    );
  }

  @Override
  public void populateUsedFlagLastUpdatedAsync()
  {
    delegate.populateUsedFlagLastUpdatedAsync();
  }

  @Override
  public void stopAsyncUsedFlagLastUpdatedUpdate()
  {
    delegate.stopAsyncUsedFlagLastUpdatedUpdate();
  }
}
