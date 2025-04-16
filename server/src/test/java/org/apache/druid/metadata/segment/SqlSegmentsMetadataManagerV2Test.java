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

import com.google.common.base.Suppliers;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerTestBase;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.segment.cache.HeapMemorySegmentMetadataCache;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SqlSegmentsMetadataManagerV2Test extends SqlSegmentsMetadataManagerTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.create());

  private SegmentsMetadataManager manager;
  private SegmentMetadataCache segmentMetadataCache;
  private final SegmentMetadataCache.UsageMode cacheMode;

  @Parameterized.Parameters
  public static Object[][] getCacheTestingModes()
  {
    return new Object[][]{
        {SegmentMetadataCache.UsageMode.ALWAYS},
        {SegmentMetadataCache.UsageMode.NEVER}
    };
  }

  public SqlSegmentsMetadataManagerV2Test(SegmentMetadataCache.UsageMode cacheMode)
  {
    this.cacheMode = cacheMode;
  }

  @Before
  public void setup() throws Exception
  {
    setUp(derbyConnectorRule);

    // metadataCachePollExec = new BlockingExecutorService("test-cache-poll-exec");
    segmentMetadataCache = new HeapMemorySegmentMetadataCache(
        jsonMapper,
        Suppliers.ofInstance(new SegmentsMetadataManagerConfig(Period.seconds(1), cacheMode)),
        Suppliers.ofInstance(storageConfig),
        connector,
        (poolSize, name) -> new WrappingScheduledExecutorService(name, null, false),
        NoopServiceEmitter.instance()
    );

    manager = new SqlSegmentsMetadataManagerV2(
        segmentMetadataCache,
        segmentSchemaCache,
        connector,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        CentralizedDatasourceSchemaConfig.create(),
        NoopServiceEmitter.instance(),
        jsonMapper
    );
  }

  @Test
  public void test_someStuff()
  {

  }
}