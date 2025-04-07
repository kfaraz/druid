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

package org.apache.druid.indexing.overlord.duty;

import org.apache.druid.indexing.common.actions.LocalTaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionTestKit;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.test.TestDataSegmentKiller;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.TestDruidLeaderSelector;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class UnusedSegmentsKillerTest
{
  @Rule
  public TaskActionTestKit taskActionTestKit = new TaskActionTestKit();

  private static final List<DataSegment> WIKI_SEGMENTS_10X1D =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                        .forIntervals(10, Granularities.DAY)
                        .eachOfSize(500);

  private ServiceEmitter emitter;
  private UnusedSegmentsKiller killer;
  private BlockingExecutorService killExecutor;
  private TestDruidLeaderSelector leaderSelector;
  private TestDataSegmentKiller dataSegmentKiller;
  private IndexerMetadataStorageCoordinator storageCoordinator;

  @Before
  public void setup()
  {
    emitter = new StubServiceEmitter();
    leaderSelector = new TestDruidLeaderSelector();
    dataSegmentKiller = new TestDataSegmentKiller();
    killExecutor = new BlockingExecutorService("UnusedSegmentsKillerTest-%s");
    storageCoordinator = taskActionTestKit.getMetadataStorageCoordinator();
    initKiller();
  }

  private void initKiller()
  {
    killer = new UnusedSegmentsKiller(
        new SegmentsMetadataManagerConfig(
            null,
            SegmentMetadataCache.UsageMode.ALWAYS,
            new UnusedSegmentKillerConfig(true, Period.days(1))
        ),
        task -> new LocalTaskActionClient(task, taskActionTestKit.getTaskActionToolbox()),
        storageCoordinator,
        leaderSelector,
        (corePoolSize, nameFormat) -> new WrappingScheduledExecutorService(nameFormat, killExecutor, true),
        dataSegmentKiller,
        taskActionTestKit.getTaskLockbox(),
        taskActionTestKit.getTaskActionToolbox().getEmitter()
    );
  }

  private void finishQueuedKillJobs()
  {
    killExecutor.finishAllPendingTasks();
  }

  @Test
  public void test_getSchedule_returnsOneDayPeriod()
  {
    final DutySchedule schedule = killer.getSchedule();
    Assert.assertEquals(Period.days(1).getMillis(), schedule.getPeriodMillis());
    Assert.assertEquals(Period.minutes(1).getMillis(), schedule.getInitialDelayMillis());
  }

  @Test
  public void test_run_startsProcessing_ifEnabled()
  {
    Assert.assertFalse(killExecutor.hasPendingTasks());
    Assert.assertTrue(killer.isEnabled());

    killer.run();
    Assert.assertTrue(killExecutor.hasPendingTasks());
  }

  @Test
  public void test_run_isNoop_ifDisabled()
  {

  }

  @Test
  public void test_run_doesNotProcessSegments_ifNotLeader()
  {
    storageCoordinator.commitSegments(Set.copyOf(WIKI_SEGMENTS_10X1D), null);
    storageCoordinator.markAllSegmentsAsUnused(TestDataSource.WIKI);

    leaderSelector.becomeLeader();
    killer.run();

    leaderSelector.stopBeingLeader();

  }

  @Test
  public void test_stop_stopsProcessing_ifEnabled()
  {
    Assert.assertFalse(killExecutor.hasPendingTasks());
    Assert.assertTrue(killer.isEnabled());

    killer.run();
    Assert.assertTrue(killExecutor.hasPendingTasks());

    killer.stop();
    Assert.assertFalse(killExecutor.hasPendingTasks());
    Assert.assertTrue(killExecutor.isTerminated());
  }

  @Test
  public void test_run_resetsQueue_ifLeadershipIsReacquired()
  {
    leaderSelector.becomeLeader();

    killer.run();

    leaderSelector.stopBeingLeader();
    leaderSelector.becomeLeader();

    killer.run();

    // Verify that the queue has been reset
  }

  @Test
  public void test_run_doesNotResetQueue_ifLastRunWasLessThanOneDayAgo()
  {

  }

  @Test
  public void test_run_prioritizesOlderIntervals()
  {

  }

  @Test
  public void test_run_doesNotDeleteSegmentFiles_ifLoadSpecIsUsedByAnotherSegment()
  {

  }

  @Test
  public void test_run_withMultipleDatasourcesAndIntervals()
  {

  }

  @Test
  public void test_run_doesNotKillSegment_ifUpdatedRecently()
  {

  }

  @Test
  public void test_run_killsSegment_withFutureInterval()
  {

  }

  @Test
  public void test_run_skipsLockedIntervals()
  {

  }
}