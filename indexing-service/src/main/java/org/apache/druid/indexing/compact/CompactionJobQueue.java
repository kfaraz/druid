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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Iterates over all eligible compaction jobs in order of their priority.
 * A fresh instance of this class must be used in every run of the
 * {@link CompactionScheduler}.
 *
 * TODO: Remaining items:
 *  - better catalog template
 *  - fill timeline gaps, support realiging intervals
 *  - write up more test cases
 *  - cancel mismatching task
 *  - pass in the engine to the template
 *  - invoke onTimelineUpdated - timeline will now get updated very frequently,
 *    - we don't want to recompact intervals, try to find the right thing to do.
 *    - we might have to do it via the policy
 * <p>
 * - if the granularity of this rule leaves gaps in the timeline, should they be filled be a prior rule
 * - because there can always be gaps depending on the current date
 * - maybe we can realign intervals if needed
 * - supervisors: cancel a task (on supervisor update or when new jobs are computed)
 * - maybe use searchInterval instead of skipIntervals
 * - how does this whole thing affect queuedIntervals
 * - for duty, it doesn't matter
 * - for supervisors, intervals will always be mutually exclusive
 * - CATALOG changes
 */
public class CompactionJobQueue
{
  private static final Logger log = new Logger(CompactionJobQueue.class);

  private final CompactionJobParams jobParams;
  private final CompactionCandidateSearchPolicy searchPolicy;

  private final CompactionStatusTracker statusTracker;
  private final TaskActionClientFactory taskActionClientFactory;
  private final OverlordClient overlordClient;
  private final GlobalTaskLockbox taskLockbox;

  private final CompactionSnapshotBuilder snapshotBuilder;
  private final PriorityQueue<CompactionJob> queue;
  private final CoordinatorRunStats runStats;

  private final CompactionSlotManager slotManager;

  public CompactionJobQueue(
      DataSourcesSnapshot dataSourcesSnapshot,
      ClusterCompactionConfig clusterCompactionConfig,
      CompactionStatusTracker statusTracker,
      TaskActionClientFactory taskActionClientFactory,
      GlobalTaskLockbox taskLockbox,
      OverlordClient overlordClient,
      ObjectMapper objectMapper
  )
  {
    this.searchPolicy = clusterCompactionConfig.getCompactionPolicy();
    this.queue = new PriorityQueue<>(
        (o1, o2) -> searchPolicy.compareCandidates(o1.getCandidate(), o2.getCandidate())
    );
    this.jobParams = new CompactionJobParams(
        DateTimes.nowUtc(),
        objectMapper,
        dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource()::get
    );
    this.slotManager = new CompactionSlotManager(
        overlordClient,
        statusTracker,
        clusterCompactionConfig
    );

    this.runStats = new CoordinatorRunStats();
    this.snapshotBuilder = new CompactionSnapshotBuilder(runStats);
    this.taskActionClientFactory = taskActionClientFactory;
    this.overlordClient = overlordClient;
    this.statusTracker = statusTracker;
    this.taskLockbox = taskLockbox;

    computeAvailableTaskSlots();
  }

  /**
   * Adds a job to this queue.
   */
  public void add(CompactionJob job)
  {
    queue.add(job);
  }

  /**
   * Creates jobs for the given {@link CompactionSupervisor} and adds them to
   * the job queue.
   */
  public void createAndEnqueueJobs(CompactionSupervisor supervisor)
  {
    final String supervisorId = supervisor.getSpec().getId();
    try {
      if (supervisor.shouldCreateJobs(jobParams)) {
        queue.addAll(supervisor.createJobs(jobParams));
      } else {
        log.debug("Skipping job creation for supervisor[%s]", supervisorId);
      }
    }
    catch (Exception e) {
      log.error(e, "Error while creating jobs for supervisor[%s]", supervisor);
    }
  }

  /**
   * Submits jobs which are ready to either the Overlord or a Broker (if it is
   * an MSQ SQL job).
   */
  public void runReadyJobs()
  {
    while (!queue.isEmpty()) {
      final CompactionJob job = queue.poll();
      final Task task = Objects.requireNonNull(job.getNonNullTask());

      if (startJobIfPendingAndReady(job, searchPolicy)) {
        statusTracker.onTaskSubmitted(task.getId(), job.getCandidate());
        runStats.add(Stats.Compaction.SUBMITTED_TASKS, RowKey.of(Dimension.DATASOURCE, task.getDataSource()), 1);
      }
    }

    // TODO: Add the skipped and the already compacted stuff determined by the DatasourceCompactibleSegmentIterator
    //  to the stats
  }

  /**
   * Builds and returns the compaction snapshots for all the datasources being
   * tracked in this queue. Must be called after {@link #runReadyJobs()}.
   */
  public Map<String, AutoCompactionSnapshot> getCompactionSnapshots()
  {
    return snapshotBuilder.build();
  }

  public CoordinatorRunStats getRunStats()
  {
    return runStats;
  }

  private void computeAvailableTaskSlots()
  {
    // Do not cancel any currently running compaction tasks to be valid
    // Future iterations can cancel a job if it doesn't match the given template
    for (ClientCompactionTaskQuery task : slotManager.fetchRunningCompactionTasks()) {
      slotManager.reserveTaskSlots(task);
    }
  }

  /**
   * Starts a job if it is ready and is not already in progress.
   *
   * @return true if the job was submitted successfully for execution
   */
  private boolean startJobIfPendingAndReady(CompactionJob job, CompactionCandidateSearchPolicy policy)
  {
    // Check if enough compaction task slots are available
    if (job.getMaxRequiredTaskSlots() <= slotManager.getNumAvailableTaskSlots()) {
      slotManager.reserveTaskSlots(job.getMaxRequiredTaskSlots());
    } else {
      return false;
    }

    // Check the current status of the job to determine if it can be run
    final CompactionCandidate candidate = job.getCandidate();
    final CompactionStatus compactionStatus = getCurrentStatusForJob(job, policy);

    if (compactionStatus.isComplete()) {
      snapshotBuilder.addToComplete(candidate);
    } else if (compactionStatus.isSkipped()) {
      snapshotBuilder.addToSkipped(candidate);
    } else {
      snapshotBuilder.addToPending(candidate);
    }

    // Job is already running, completed or skipped
    if (compactionStatus.getState() != CompactionStatus.State.PENDING) {
      return false;
    }

    return startTaskIfReady(job);
  }

  /**
   * Starts the given job if the underlying Task is able to acquire locks.
   *
   * @return true if the Task was submitted successfully.
   */
  private boolean startTaskIfReady(CompactionJob job)
  {
    // Assume MSQ jobs to be always ready
    if (job.isMsq()) {
      return true;
    }

    final Task task = job.getNonNullTask();
    log.info("Checking readiness of task[%s] with interval[%s]", task.getId(), job.getCompactionInterval());
    try {
      taskLockbox.add(task);
      if (task.isReady(taskActionClientFactory.create(task))) {
        // Hold the locks acquired by task.isReady() as we will reacquire them anyway
        FutureUtils.getUnchecked(overlordClient.runTask(task.getId(), task), true);
        return true;
      } else {
        taskLockbox.unlockAll(task);
        return false;
      }
    }
    catch (Exception e) {
      log.error(e, "Error while checking readiness of task[%s]", task.getId());
      taskLockbox.unlockAll(task);
      return false;
    }
  }

  public CompactionStatus getCurrentStatusForJob(CompactionJob job, CompactionCandidateSearchPolicy policy)
  {
    final CompactionStatus compactionStatus = statusTracker.computeCompactionStatus(job.getCandidate(), policy);
    final CompactionCandidate candidatesWithStatus = job.getCandidate().withCurrentStatus(null);
    statusTracker.onCompactionStatusComputed(candidatesWithStatus, null);
    return compactionStatus;
  }
}
