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

package org.apache.druid.indexing.compact;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.compact.CompactionSegmentIterator;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * TODO: pending items
 *  - [ ] should scheduler config be runtime or dynamic. Runtime might create
 *  trouble during rolling upgrades if coordinator and overlord leaders are different.
 *  - [ ] callback on taskrunner
 *  - [ ] handle success and failure inside DatasourceQueue
 *  - [ ] make policy serializable
 *  - [ ] add another policy
 *  - [x] enable segments polling if overlord is standalone
 *  - [ ] test on cluster - standalone, coordinator-overlord
 *  - [ ] unit tests
 *  - [ ] integration tests
 */
public class CompactionSchedulerImpl implements CompactionScheduler
{
  private static final Logger log = new Logger(CompactionSchedulerImpl.class);

  private final TaskMaster taskMaster;
  private final TaskQueryTool taskQueryTool;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentManager;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService executor;

  private final TaskRunnerListener taskStateListener;
  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private final CompactSegments duty;
  private final boolean shouldPollSegments;

  private final Map<String, DatasourceCompactionQueue> datasourceQueues = new HashMap<>();

  private final AtomicReference<CoordinatorCompactionConfig> currentConfig
      = new AtomicReference<>(CoordinatorCompactionConfig.empty());

  @Inject
  public CompactionSchedulerImpl(
      TaskMaster taskMaster,
      TaskQueryTool taskQueryTool,
      SegmentsMetadataManager segmentManager,
      JacksonConfigManager configManager,
      CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.taskMaster = taskMaster;
    this.taskQueryTool = taskQueryTool;
    this.configManager = configManager;
    this.segmentManager = segmentManager;
    this.executor = executorFactory.create(1, "CompactionScheduler-%s");
    this.shouldPollSegments = coordinatorOverlordServiceConfig.isEnabled() && segmentManager != null;
    this.duty = new CompactSegments(new WrapperPolicy(), new LocalOverlordClient());
    this.taskStateListener = new TaskRunnerListener()
    {

      @Override
      public String getListenerId()
      {
        return "CompactionScheduler";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {
        // Do nothing
      }

      @Override
      public void statusChanged(String taskId, TaskStatus status)
      {

      }
    };
  }

  @LifecycleStart
  public void becomeLeader()
  {
    log.info("Becoming leader");
    if (isLeader.compareAndSet(false, true)) {
      executor.submit(this::checkSchedulingStatus);
    }
  }

  @LifecycleStop
  public void stopBeingLeader()
  {
    log.info("not leader anymore");
    isLeader.set(false);
  }

  private synchronized void initState()
  {
    // TODO: add the task state listener
    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().unregisterListener(taskStateListener.getListenerId());
    } else {
      log.warn("No TaskRunner. Unable to register callbacks.");
    }

    if (shouldPollSegments) {
      segmentManager.startPollingDatabasePeriodically();
    }
  }

  private synchronized void cleanupState()
  {
    log.info("Cleaning up scheduler state");
    datasourceQueues.forEach((datasource, queue) -> queue.stop());
    datasourceQueues.clear();

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(taskStateListener, executor);
    } else {
      log.warn("No TaskRunner. Unable to de-register callbacks.");
    }

    if (shouldPollSegments) {
      segmentManager.stopPollingDatabasePeriodically();
    }
  }

  private TaskQueue getValidTaskQueue()
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get();
    } else {
      throw DruidException.defensive("No TaskQueue. Cannot proceed.");
    }
  }

  public boolean isEnabled()
  {
    return currentConfig.get().getSchedulerConfig().isEnabled();
  }

  private synchronized void checkSchedulingStatus()
  {
    log.info("Checking schedule");
    final CoordinatorCompactionConfig latestConfig = getLatestConfig();
    currentConfig.set(latestConfig);

    if (isLeader.get()) {
      if (isEnabled()) {
        initState();
        processCompactionQueue(latestConfig);
      } else {
        cleanupState();
      }

      // Continue the schedule as long as we are the leader
      executor.schedule(this::checkSchedulingStatus, 60, TimeUnit.SECONDS);
    } else {
      cleanupState();
    }
  }

  private synchronized void processCompactionQueue(
      CoordinatorCompactionConfig currentConfig
  )
  {
    log.info("Processing compaction queue");
    final Set<String> compactionEnabledDatasources = new HashSet<>();
    if (currentConfig.getCompactionConfigs() != null) {
      currentConfig.getCompactionConfigs().forEach(config -> compactionEnabledDatasources.add(config.getDataSource()));

      // Create queues for datasources where compaction has been freshly enabled
      currentConfig.getCompactionConfigs().forEach(datasourceConfig -> datasourceQueues.computeIfAbsent(
          datasourceConfig.getDataSource(),
          DatasourceCompactionQueue::new
      ).updateConfig(datasourceConfig));
    }

    // Stop queues for datasources where compaction has been freshly disabled
    final Set<String> currentlyRunningDatasources = new HashSet<>(datasourceQueues.keySet());
    for (String datasource : currentlyRunningDatasources) {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceQueues.remove(datasource).stop();
      }
    }

    DataSourcesSnapshot dataSourcesSnapshot
        = segmentManager.getSnapshotOfDataSourcesWithAllUsedSegments();
    final CoordinatorRunStats stats = new CoordinatorRunStats();
    duty.run(currentConfig, dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource(), stats);

    // Now check the task slots and stuff and submit the highest priority tasks one by one
    // 1. Compute maximum compaction task slots
    // 2. Compute currently available task slots
    // 3. Until all slots are taken up,
    //    a) Ask each datasource queue for their highest priority job
    //    b) Pick the highest priority job out of those (if there is no prioritized datasource)
    //    c) Check if there is already a task running for that datasource-interval??
    //    d) If not, then submit the job
    //    e) TODO: Whether submitted or ignored, tell the datasource queue what you did
    //    f) Track jobs that have just been submitted to ensure that you do not resubmit those, see if the TaskQueue can
    //    somehow help perform the deduplication without us having to maintain a separate data structure - yes TaskQueue
    //    can do that. We just get all active tasks.
  }

  private CoordinatorCompactionConfig getLatestConfig()
  {
    return configManager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class,
        CoordinatorCompactionConfig.empty()
    ).get();
  }

  @Override
  public AutoCompactionSnapshot getAutoCompactionSnapshotForDataSource(String dataSource)
  {
    return AutoCompactionSnapshot.builder("wiki").build();
  }

  @Override
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    return 0L;
  }

  @Override
  public Map<Object, AutoCompactionSnapshot> getAutoCompactionSnapshot()
  {
    return Collections.emptyMap();
  }

  private static class WrapperPolicy implements CompactionSegmentSearchPolicy
  {

    @Override
    public CompactionSegmentIterator createIterator(
        Map<String, DataSourceCompactionConfig> compactionConfigs,
        Map<String, SegmentTimeline> dataSources,
        Map<String, List<Interval>> skipIntervals
    )
    {
      return null;
    }
  }

  /**
   * Dummy Overlord client used by the {@link #duty} to fetch task related info.
   * This client simply redirects all queries to the {@link TaskQueryTool}.
   */
  private class LocalOverlordClient extends NoopOverlordClient
  {
    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      try {
        getValidTaskQueue().add((Task) taskObject);
        return Futures.immediateVoidFuture();
      }
      catch (Throwable t) {
        return Futures.immediateFailedFuture(t);
      }
    }

    @Override
    public ListenableFuture<Void> cancelTask(String taskId)
    {
      try {
        getValidTaskQueue().shutdown(taskId, "just coz!");
        return Futures.immediateVoidFuture();
      }
      catch (Throwable t) {
        return Futures.immediateFailedFuture(t);
      }
    }

    @Override
    public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
    {
      return futureOf(
          () -> new TaskPayloadResponse(taskId, taskQueryTool.getTask(taskId).transform().orNull())
      );
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      final ListenableFuture<List<TaskStatusPlus>> tasksFuture
          = futureOf(taskQueryTool.get);
      return Futures.transform(
          tasksFuture,
          taskList -> CloseableIterators.withEmptyBaggage(taskList.iterator()),
          Execs.directExecutor()
      );
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      return futureOf(() -> taskQueryTool.getLockedIntervals(lockFilterPolicies));
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return futureOf(() -> taskQueryTool.getTotalWorkerCapacity());
    }

    @SuppressWarnings("unchecked")
    private <T> ListenableFuture<T> futureOf(Supplier<T> supplier)
    {
      try {
        return Futures.immediateFuture(supplier.get());
      }
      catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      }
    }
  }

}
