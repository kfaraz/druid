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

import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.indexer.report.KillTaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.KillUnusedSegmentsTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Deletes unused segments from metadata store and the deep storage.
 * <p>
 * TODO:
 *  - verify that task finishes instantly if lock cannot be acquired
 *  - or if there are no eligible unused segments for that datasource-interval
 *
 * @see SegmentsMetadataManagerConfig to enable the cleanup
 */
public class UnusedSegmentsKiller implements OverlordDuty
{
  private static final Logger log = new Logger(UnusedSegmentsKiller.class);
  private static final String TASK_ID_PREFIX = "overlord-issued";

  private final TaskLockbox taskLockbox;
  private final DataSegmentKiller dataSegmentKiller;

  private final UnusedSegmentKillerConfig killConfig;
  private final DruidLeaderSelector leaderSelector;
  private final TaskActionClientFactory taskActionClientFactory;
  private final IndexerMetadataStorageCoordinator storageCoordinator;

  private final ScheduledExecutorService exec;
  private int previousLeaderTerm;
  private DateTime lastIntervalRefreshTime;

  /**
   * Queue of kill candidates. Use a PriorityBlockingQueue to ensure thread-safety
   * since this queue is accessed by both {@link #run()} and {@link #processKillQueue()}.
   */
  private final PriorityBlockingQueue<KillCandidate> killQueue;

  private final ConcurrentHashMap<String, Set<Interval>> datasourceToKillIntervals
      = new ConcurrentHashMap<>();

  @Inject
  public UnusedSegmentsKiller(
      SegmentsMetadataManagerConfig config,
      TaskActionClientFactory taskActionClientFactory,
      IndexerMetadataStorageCoordinator storageCoordinator,
      @IndexingService DruidLeaderSelector leaderSelector,
      ScheduledExecutorFactory executorFactory,
      DataSegmentKiller dataSegmentKiller,
      TaskLockbox taskLockbox
  )
  {
    this.taskLockbox = taskLockbox;
    this.leaderSelector = leaderSelector;
    this.dataSegmentKiller = dataSegmentKiller;
    this.storageCoordinator = storageCoordinator;
    this.taskActionClientFactory = taskActionClientFactory;

    this.killConfig = config.getKillUnused();

    if (isEnabled()) {
      this.exec = executorFactory.create(1, "UnusedSegmentsKiller-%s");
      this.killQueue = new PriorityBlockingQueue<>(
          1000,
          Ordering.from(Comparators.intervalsByEndThenStart())
                  .onResultOf(candidate -> candidate.interval)
      );
    } else {
      this.exec = null;
      this.killQueue = null;
    }
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      log.info("Starting UnusedSegmentsKiller. Will schedule kill jobs once I become leader.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      log.info("Stopping UnusedSegmentsKiller");
      exec.shutdownNow();
    }
  }

  @Override
  public boolean isEnabled()
  {
    return killConfig.isEnabled();
  }

  @Override
  public void run()
  {
    updateStateIfNewLeader();
    if (shouldRefreshKillIntervals()) {
      // TODO: ensure that exec has no pending tasks
      exec.submit(this::addJobsToKillQueue);
    }
  }

  @Override
  public DutySchedule getSchedule()
  {
    // If there are enough unused segments, the cleanup thread will be busy for
    // the entire cleanup period (1 day). If there are not, having a large period doesn't hurt.
    return new DutySchedule(Period.days(1).getMillis(), Period.minutes(1).getMillis());
  }

  private void updateStateIfNewLeader()
  {
    final int currentLeaderTerm = leaderSelector.localTerm();
    if (currentLeaderTerm != previousLeaderTerm) {
      previousLeaderTerm = currentLeaderTerm;
      datasourceToKillIntervals.clear();
      killQueue.clear();
      lastIntervalRefreshTime = null;
    }
  }

  private boolean shouldRefreshKillIntervals()
  {
    return lastIntervalRefreshTime == null
           || !lastIntervalRefreshTime.isAfter(DateTimes.nowUtc().minusDays(1));
  }

  /**
   * Resets the kill queue with fresh jobs.
   */
  private void addJobsToKillQueue()
  {
    try {
      final Set<String> dataSources = storageCoordinator.retrieveAllDatasourceNames();
      for (String dataSource : dataSources) {
        storageCoordinator.retrieveUnusedSegmentIntervals(dataSource, 10_000)
                          .forEach(interval -> killQueue.add(new KillCandidate(dataSource, interval)));
      }
      lastIntervalRefreshTime = DateTimes.nowUtc();

      killQueue.clear();
      datasourceToKillIntervals.forEach(
          (dataSource, killIntervals) -> killIntervals.forEach(
              interval -> killQueue.add(new KillCandidate(dataSource, interval))
          )
      );

      if (!killQueue.isEmpty()) {
        processKillQueue();
      }
    }
    catch (Throwable t) {
      log.error(t, "Could not queue kill jobs");
    }
  }

  /**
   * Cleans up all unused segments added to the {@link #killQueue} by the last
   * {@link #run()} of this duty. An {@link EmbeddedKillTask} is launched for
   * each eligible unused interval of a datasource
   */
  public void processKillQueue()
  {
    if (!isEnabled() || killQueue.isEmpty()) {
      return;
    }

    while (!killQueue.isEmpty() && leaderSelector.isLeader()) {
      final KillCandidate candidate = killQueue.poll();
      if (candidate == null) {
        return;
      }

      final String taskId = IdUtils.newTaskId(
          TASK_ID_PREFIX,
          KillUnusedSegmentsTask.TYPE,
          candidate.dataSource,
          candidate.interval
      );
      try {
        runKillTask(candidate, taskId);
      }
      catch (Throwable t) {
        log.error(
            t,
            "Error while running embedded kill task[%s] for datasource[%s], interval[%s].",
            taskId, candidate.dataSource, candidate.interval
        );
      }
    }
  }

  /**
   * Launches an embedded kill task for the given candidate.
   */
  private void runKillTask(KillCandidate candidate, String taskId)
  {
    final EmbeddedKillTask killTask = new EmbeddedKillTask(
        taskId,
        candidate,
        DateTimes.nowUtc().minus(killConfig.getBufferPeriod())
    );

    final TaskActionClient taskActionClient = taskActionClientFactory.create(killTask);
    final TaskToolbox taskToolbox = new TaskToolbox.Builder()
        .taskActionClient(taskActionClient)
        .dataSegmentKiller(dataSegmentKiller)
        .build();

    try {
      final boolean isReady = killTask.isReady(taskActionClient);
      if (!isReady) {
        return;
      }

      taskLockbox.add(killTask);
      killTask.runTask(taskToolbox);
    }
    catch (Throwable t) {
      log.error(t, "Embedded kill task[%s] failed", killTask.getId());
    }
    finally {
      taskLockbox.remove(killTask);
    }
  }

  /**
   * Represents a single candidate interval that contains unused segments.
   */
  private static class KillCandidate
  {
    private final String dataSource;
    private final Interval interval;

    private KillCandidate(String dataSource, Interval interval)
    {
      this.dataSource = dataSource;
      this.interval = interval;
    }
  }

  /**
   * Embedded kill task. Unlike other task types, this task is not persisted and
   * does not run on a worker or indexer. Hence, it doesn't take up any task slots.
   * Also, this kill task targets only a single unused segment interval at a time
   * to ensure that locks are held over short intervals over brief periods of time.
   */
  private class EmbeddedKillTask extends KillUnusedSegmentsTask
  {
    private EmbeddedKillTask(
        String taskId,
        KillCandidate candidate,
        DateTime maxUpdatedTimeOfEligibleSegment
    )
    {
      super(
          taskId,
          candidate.dataSource,
          candidate.interval,
          null,
          Map.of(Tasks.PRIORITY_KEY, 25),
          null,
          null,
          maxUpdatedTimeOfEligibleSegment
      );
    }

    @Nullable
    @Override
    protected Integer getNumTotalBatches()
    {
      // Do everything in a single batch
      return 1;
    }

    @Override
    protected void writeTaskReport(KillTaskReport report, TaskToolbox toolbox)
    {
      // TODO: emit stats, I guess
    }

    @Override
    protected List<DataSegment> fetchUnusedSegmentsBatch(TaskToolbox toolbox, int nextBatchSize) throws IOException
    {
      return storageCoordinator.retrieveUnusedSegmentsWithExactInterval(
          getDataSource(),
          getInterval(),
          getMaxUsedStatusLastUpdatedTime(),
          1000
      );
    }
  }
}
