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
import org.apache.druid.timeline.SegmentId;
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
 */
@ManageLifecycle
public class SqlSegmentsMetadataManagerV2 implements SegmentsMetadataManager
{
  private final SegmentsMetadataManager delegate;
  private final SegmentMetadataCache segmentMetadataCache;

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

  }

  @Override
  public void stopPollingDatabasePeriodically()
  {

  }

  @Override
  public boolean isPollingDatabasePeriodically()
  {
    return segmentMetadataCache.isEnabled() || delegate.isPollingDatabasePeriodically();
  }

  @Nullable
  @Override
  public ImmutableDruidDataSource getImmutableDataSourceWithUsedSegments(String dataSource)
  {
    return null;
  }

  @Override
  public Collection<ImmutableDruidDataSource> getImmutableDataSourcesWithAllUsedSegments()
  {
    return List.of();
  }

  @Override
  public DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments()
  {
    return null;
  }

  @Override
  public Iterable<DataSegment> iterateAllUsedSegments()
  {
    return null;
  }

  @Override
  public Optional<Iterable<DataSegment>> iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
      String datasource,
      Interval interval,
      boolean requiresLatest
  )
  {
    return null;
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
