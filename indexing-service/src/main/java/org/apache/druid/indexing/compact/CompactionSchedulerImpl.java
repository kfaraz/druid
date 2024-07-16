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

import com.google.inject.Inject;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionSchedulerImpl
{
  private static final Logger log = new Logger(CompactionSchedulerImpl.class);

  private final TaskQueue taskQueue;
  private final JacksonConfigManager configManager;
  private final MetadataSegmentsWatcher segmentsWatcher;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private volatile ScheduledExecutorService executor;
  private final ScheduledExecutorFactory executorFactory;

  private final Map<String, DatasourceCompactionQueue> datasourceQueues = new HashMap<>();

  private final AtomicReference<CoordinatorCompactionConfig> currentConfig
      = new AtomicReference<>(CoordinatorCompactionConfig.empty());

  @Inject
  public CompactionSchedulerImpl(
      TaskQueue taskQueue,
      JacksonConfigManager configManager,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.taskQueue = taskQueue;
    this.configManager = configManager;
    this.segmentsWatcher = null;
    this.executorFactory = executorFactory;

    // TODO: setup some callbacks on TaskQueue and the SegmentsWatcher
  }

  @LifecycleStart
  public void start()
  {
    if (isEnabled()) {
      log.info("Starting scheduler as we are now the leader.");
      initExecutor();
      executor.submit(this::checkSchedulingStatus);
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (isEnabled()) {
      log.info("Pausing scheduler as we are not the leader anymore.");
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

  /*
  TODO: SEGMENTS
   - easiest way is to just do it with SegmentTimeline.lookup
  - whatever segments you add or remove to an interval
  - just check if a lookup on the respective intervals has changed, right?
  - can anything else happen?

  - if some segments have been removed, an older set of segments might become re-visible.
  - how can we check this??

  - a lookup of that interval will reveal some other set.
   */
  public void onSegmentsUpdated()
  {

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
      currentConfig.getCompactionConfigs().forEach(
          config -> compactionEnabledDatasources.add(config.getDataSource())
      );

      // Create queues for datasources where compaction has been freshly enabled
      currentConfig.getCompactionConfigs().forEach(
          datasourceConfig -> datasourceQueues
              .computeIfAbsent(datasourceConfig.getDataSource(), DatasourceCompactionQueue::new)
              .updateConfig(datasourceConfig)
      );
    }

    // Stop queues for datasources where compaction has been freshly disabled
    final Set<String> currentlyRunningDatasources = new HashSet<>(datasourceQueues.keySet());
    for (String datasource : currentlyRunningDatasources) {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceQueues.remove(datasource).stop();
      }
    }

    // Now check the task slots and stuff and submit the highest priority tasks one by one
    // 1. Compute maximum compaction task slots
    // 2. Compute currently available task slots
    // 3. Until all slots are taken up,
    //    a) Ask each datasource queue for their highest priority job
    //    b) Pick the highest priority job out of those (if there is no prioritized datasource)
    //    c) Check if there is already a task running for that datasource-interval??
    //    d) If not, then submit the job
    //    e) Whether submitted or ignored, tell the datasource queue what you did
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

  /*
  What if we decide to do everything on the coordinator?

  What changes?
  - keep track of everything that you have submitted
  - keep polling for the statuses
  - feels super weird for the coordinator to do this

  Biggest problem now is the status.
  We can just redirect.

  Overlord pros:
  - natural place for ingestion stuff to happen
  - easy to keep track of running tasks and get task complete callbacks

  Coordinator pros:
  - compaction status APIs
  - already has a segment timeline that we can use

*/

}
