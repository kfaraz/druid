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
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaManager;

/**
 * Implementation of {@link IndexerMetadataStorageCoordinator} backed by an in-memory
 * {@link SegmentsMetadataCache}.
 * <p>
 * While the cache is being read by a thread, no other thread should update it.
 * This is currently ensured by invoking all the methods of this class after
 * acquiring the {@code giant} lock in {@code TaskLockbox}.
 *
 * TODO: Check if this is true for read operations as well, i.e. while a read is
 * happening, a write shouldn't come in and update stuff.
 * It is possible that the IndexerCoordinator is invoked from certain places to
 * fetch segments without going through the TaskLockbox.
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
}
