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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 */
public class ZkCoordinatorTest extends CuratorTestBase
{
  private static final Logger log = new Logger(ZkCoordinatorTest.class);

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
  private final DruidServerMetadata me = new DruidServerMetadata(
      "dummyServer",
      "dummyHost",
      null,
      0,
      ServerType.HISTORICAL,
      "normal",
      0
  );
  private final ZkPathsConfig zkPaths = new ZkPathsConfig()
  {
    @Override
    public String getBase()
    {
      return "/druid";
    }
  };

  private File infoDir;
  private List<StorageLocationConfig> locations;
  private BlockingExecutorService executor;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception
  {
    executor = new BlockingExecutorService("ZkCoordinator-test");
    try {
      infoDir = temporaryFolder.newFolder();
      log.info("Creating tmp test files in [%s]", infoDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    locations = Collections.singletonList(
        new StorageLocationConfig(
            infoDir,
            100L,
            100d
        )
    );

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
    executor.shutdownNow();
  }

  @Test(timeout = 60_000L)
  public void testLoadDrop() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    DataSegment segment = new DataSegment(
        "test",
        Intervals.of("P1d/2011-04-02"),
        "v0",
        ImmutableMap.of("version", "v0", "interval", Intervals.of("P1d/2011-04-02"), "cacheDir", "/no"),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        123L
    );

    CountDownLatch loadLatch = new CountDownLatch(1);
    CountDownLatch dropLatch = new CountDownLatch(1);

    SegmentLoadDropHandler segmentLoadDropHandler = new SegmentLoadDropHandler(
        jsonMapper,
        new SegmentLoaderConfig() {
          @Override
          public File getInfoDir()
          {
            return infoDir;
          }

          @Override
          public List<StorageLocationConfig> getLocations()
          {
            return locations;
          }
        },
        EasyMock.createNiceMock(DataSegmentAnnouncer.class),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        EasyMock.createNiceMock(SegmentManager.class),
        EasyMock.createNiceMock(SegmentCacheManager.class),
        new ServerTypeConfig(ServerType.HISTORICAL),
        (corePoolSize, nameFormat) ->
            new WrappingScheduledExecutorService(nameFormat, executor, false)
    )
    {
      @Override
      public void submitCuratorRequest(DataSegmentChangeRequest request, DataSegmentChangeCallback callback)
      {
        if (!segment.getId().equals(request.getSegment().getId())) {
          return;
        }

        if (request instanceof SegmentChangeRequestLoad) {
          loadLatch.countDown();
        } else if (request instanceof SegmentChangeRequestDrop) {
          dropLatch.countDown();
        }

        callback.execute();
      }
    };

    ZkCoordinator zkCoordinator = new ZkCoordinator(
        segmentLoadDropHandler,
        jsonMapper,
        zkPaths,
        me,
        curator
    );
    zkCoordinator.start();

    String segmentZkPath = ZKPaths.makePath(zkPaths.getLoadQueuePath(), me.getName(), segment.getId().toString());

    curator
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(segmentZkPath, jsonMapper.writeValueAsBytes(new SegmentChangeRequestLoad(segment)));

    loadLatch.await();

    while (curator.checkExists().forPath(segmentZkPath) != null) {
      Thread.sleep(100);
    }

    curator
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(segmentZkPath, jsonMapper.writeValueAsBytes(new SegmentChangeRequestDrop(segment)));

    dropLatch.await();

    while (curator.checkExists().forPath(segmentZkPath) != null) {
      Thread.sleep(100);
    }

    zkCoordinator.stop();
  }
}
