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
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * Handles all segment metadata read/write operations. If the {@link SegmentsMetadataCache}
 * is enabled and ready, the methods are served by cache-backed implementation in
 * {@link SqlSegmentsMetadataStorageWithCache}, otherwise they are served by the
 * default implementation in {@link IndexerSQLMetadataStorageCoordinator}.
 * <p>
 * This class extends {@link IndexerSQLMetadataStorageCoordinator} so that we
 * need not override the methods which do not use the cache. Thus, they are always
 * served by the default no cache implementation.
 */
public class SqlSegmentsMetadataStorage extends IndexerSQLMetadataStorageCoordinator
{
  private final SegmentsMetadataCache segmentsMetadataCache;
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
    super(jsonMapper, dbTables, connector, segmentSchemaManager, centralizedDatasourceSchemaConfig);
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

  @Override
  public SegmentPublishResult commitSegmentsAndMetadata(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  )
  {
    if (segmentsMetadataCache.isReady()) {
      return delegateWithCache.commitSegmentsAndMetadata(segments, startMetadata, endMetadata, segmentSchemaMapping);
    } else {
      return super.commitSegmentsAndMetadata(segments, startMetadata, endMetadata, segmentSchemaMapping);
    }
  }
}
