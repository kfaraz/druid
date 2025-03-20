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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.rules.ExternalResource;

public class TaskActionTestKit extends ExternalResource
{
  private final MetadataStorageTablesConfig metadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase("druid");

  private TaskStorage taskStorage;
  private GlobalTaskLockbox taskLockbox;
  private TestDerbyConnector testDerbyConnector;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private SegmentsMetadataManager segmentsMetadataManager;
  private TaskActionToolbox taskActionToolbox;
  private SegmentSchemaManager segmentSchemaManager;
  private SegmentSchemaCache segmentSchemaCache;
  private SegmentMetadataCache segmentMetadataCache;
  private BlockingExecutorService metadataCachePollExec;

  private boolean useSegmentMetadataCache = new SegmentsMetadataManagerConfig(null, null).isUseCache();
  private boolean skipSegmentPayloadFetchForAllocation = new TaskLockConfig().isBatchAllocationReduceMetadataIO();

  public GlobalTaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public IndexerMetadataStorageCoordinator getMetadataStorageCoordinator()
  {
    return metadataStorageCoordinator;
  }

  public SegmentsMetadataManager getSegmentsMetadataManager()
  {
    return segmentsMetadataManager;
  }

  public TaskActionToolbox getTaskActionToolbox()
  {
    return taskActionToolbox;
  }

  public void setSkipSegmentPayloadFetchForAllocation(boolean skipSegmentPayloadFetchForAllocation)
  {
    this.skipSegmentPayloadFetchForAllocation = skipSegmentPayloadFetchForAllocation;
  }

  public void setUseSegmentMetadataCache(boolean useSegmentMetadataCache)
  {
    this.useSegmentMetadataCache = useSegmentMetadataCache;
  }

  public void syncSegmentMetadataCache()
  {
    metadataCachePollExec.finishNextPendingTasks(4);
  }

  @Override
  public void before()
  {
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(new Period("PT24H")));
    testDerbyConnector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(metadataStorageTablesConfig)
    );
    final ObjectMapper objectMapper = new TestUtils().getTestObjectMapper();
    segmentSchemaManager = new SegmentSchemaManager(metadataStorageTablesConfig, objectMapper, testDerbyConnector);

    final SqlSegmentMetadataTransactionFactory transactionFactory = setupTransactionFactory(objectMapper);
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        transactionFactory,
        objectMapper,
        metadataStorageTablesConfig,
        testDerbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );
    taskLockbox = new GlobalTaskLockbox(taskStorage, metadataStorageCoordinator);
    segmentSchemaCache = new SegmentSchemaCache(NoopServiceEmitter.instance());
    segmentsMetadataManager = new SqlSegmentsMetadataManager(
        objectMapper,
        Suppliers.ofInstance(new SegmentsMetadataManagerConfig(null, null)),
        Suppliers.ofInstance(metadataStorageTablesConfig),
        testDerbyConnector,
        segmentSchemaCache,
        CentralizedDatasourceSchemaConfig.create(),
        NoopServiceEmitter.instance()
    );
    final TaskLockConfig taskLockConfig = new TaskLockConfig()
    {
      @Override
      public long getBatchAllocationWaitTime()
      {
        return 10L;
      }

      @Override
      public boolean isBatchAllocationReduceMetadataIO()
      {
        return skipSegmentPayloadFetchForAllocation;
      }
    };

    taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        taskStorage,
        metadataStorageCoordinator,
        new SegmentAllocationQueue(
            taskLockbox,
            taskLockConfig,
            metadataStorageCoordinator,
            NoopServiceEmitter.instance(),
            ScheduledExecutors::fixed
        ),
        NoopServiceEmitter.instance(),
        EasyMock.createMock(SupervisorManager.class),
        objectMapper
    );
    testDerbyConnector.createDataSourceTable();
    testDerbyConnector.createPendingSegmentsTable();
    testDerbyConnector.createSegmentSchemasTable();
    testDerbyConnector.createSegmentTable();
    testDerbyConnector.createRulesTable();
    testDerbyConnector.createConfigTable();
    testDerbyConnector.createTaskTables();
    testDerbyConnector.createAuditTable();

    segmentMetadataCache.start();
    segmentMetadataCache.becomeLeader();
    syncSegmentMetadataCache();
  }

  private SqlSegmentMetadataTransactionFactory setupTransactionFactory(ObjectMapper objectMapper)
  {
    metadataCachePollExec = new BlockingExecutorService("test-cache-poll-exec");
    segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        objectMapper,
        Suppliers.ofInstance(new SegmentsMetadataManagerConfig(Period.seconds(1), useSegmentMetadataCache)),
        Suppliers.ofInstance(metadataStorageTablesConfig),
        testDerbyConnector,
        (poolSize, name) -> new WrappingScheduledExecutorService(name, metadataCachePollExec, false),
        new NoopServiceEmitter()
    );

    final TestDruidLeaderSelector leaderSelector = new TestDruidLeaderSelector();
    leaderSelector.becomeLeader();

    return new SqlSegmentMetadataTransactionFactory(
        objectMapper,
        metadataStorageTablesConfig,
        testDerbyConnector,
        leaderSelector,
        segmentMetadataCache
    )
    {
      @Override
      public int getMaxRetries()
      {
        return 2;
      }
    };
  }

  @Override
  public void after()
  {
    testDerbyConnector.tearDown();
    taskStorage = null;
    taskLockbox = null;
    testDerbyConnector = null;
    metadataStorageCoordinator = null;
    segmentsMetadataManager = null;
    taskActionToolbox = null;
    segmentMetadataCache.stopBeingLeader();
    segmentMetadataCache.stop();
    useSegmentMetadataCache = new SegmentsMetadataManagerConfig(null, null).isUseCache();
  }
}
