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
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.http.OverlordResource;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.compact.CompactionSegmentIterator;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionSchedulerImpl
{
  private static final Logger log = new Logger(CompactionSchedulerImpl.class);

  private final TaskMaster taskMaster;
  private final OverlordResource overlordResource;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentManager;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private volatile ScheduledExecutorService executor;
  private final ScheduledExecutorFactory executorFactory;
  private final CompactSegments duty;

  private final Map<String, DatasourceCompactionQueue> datasourceQueues = new HashMap<>();

  private final AtomicReference<CoordinatorCompactionConfig> currentConfig = new AtomicReference<>(
      CoordinatorCompactionConfig.empty());

  @Inject
  public CompactionSchedulerImpl(
      TaskMaster taskMaster,
      OverlordResource overlordResource,
      SegmentsMetadataManager segmentManager,
      JacksonConfigManager configManager,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.taskMaster = taskMaster;
    this.configManager = configManager;
    this.overlordResource = overlordResource;
    this.segmentManager = segmentManager;
    this.executorFactory = executorFactory;

    // TODO: Setup polling if standalone

    this.duty = new CompactSegments(new WrapperPolicy(), new LocalOverlordClient());
    taskMaster.registerToLeaderLifecycle(this);
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      log.info("Starting scheduler as we are now the leader.");
      initExecutor();
      executor.submit(this::checkSchedulingStatus);

      // TODO: setup callbacks
      getValidTaskQueue();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      log.info("Pausing scheduler as we are not the leader anymore.");

      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        // TODO: remove callbacks
      } else {
        log.warn("No TaskQueue. Unable to de-register callbacks.");
      }
    }
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
    cleanupState();
  }

  private void initExecutor()
  {
    if (executor == null) {
      executor = executorFactory.create(1, "CompactionScheduler-%s");
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

  /*
  TODO: TASKS
  For tasks, we need to know the following:
  all items are the same as CompactSegments
  - available number of slots - can be determined in a way similar to how CompactSegments does it
  - whether there is already a task for the chosen datasource + interval - do it the same way CompactSegments does it
  - locked intervals (we need to support it for now)

   Do we need to know which tasks are currently running?
   */
  public void onTaskStatusChanged(TaskStatus taskStatus)
  {
    // Update the datasource queue with this info
    // What if we never got notified for the completion of a certain task?
    // We should be able to recover from that situation.

    // If failed, put that job back in the queue upto a max number of retries.
    // If succeeded or retry exhausted, put that job in a different waiting queue

    // Also trigger a check
  }

  public boolean isEnabled()
  {
    return currentConfig.get().getSchedulerConfig().isEnabled();
  }

  private synchronized void cleanupState()
  {
    datasourceQueues.forEach((datasource, queue) -> queue.stop());
    datasourceQueues.clear();
  }

  private synchronized void checkSchedulingStatus()
  {
    final CoordinatorCompactionConfig latestConfig = getLatestConfig();
    currentConfig.set(latestConfig);

    if (isEnabled()) {
      processCompactionQueue(latestConfig);
    } else {
      // Do not process but continue the schedule
      cleanupState();
    }
    executor.schedule(this::checkSchedulingStatus, 60, TimeUnit.SECONDS);
  }

  private synchronized void processCompactionQueue(
      CoordinatorCompactionConfig currentConfig
  )
  {
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
      return futureOf(overlordResource.getTaskPayload(taskId));
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state, @Nullable String dataSource, @Nullable Integer maxCompletedTasks
    )
    {
      final ListenableFuture<List<TaskStatusPlus>> tasksFuture
          = futureOf(overlordResource.getAllActiveTasks());
      return Futures.transform(
          tasksFuture,
          taskList -> CloseableIterators.withEmptyBaggage(taskList.iterator()),
          Execs.directExecutor()
      );
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      return futureOf(overlordResource.getDatasourceLockedIntervalsV2(lockFilterPolicies));
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return futureOf(overlordResource.getTotalWorkerCapacity());
    }

    @SuppressWarnings("unchecked")
    private <T> ListenableFuture<T> futureOf(Response response)
    {
      final int responseCode = response.getStatus();
      if (responseCode >= 200 && responseCode < 300) {
        return Futures.immediateFuture((T) response.getEntity());
      } else if (response.getEntity() instanceof ErrorResponse) {
        ErrorResponse error = (ErrorResponse) response.getEntity();
        return Futures.immediateFailedFuture(error.getUnderlyingException());
      } else {
        log.warn("Error while invoking overlord method: [%s]", response.getEntity());
        return Futures.immediateFailedFuture(DruidException.defensive("Unknown error"));
      }
    }
  }

}
