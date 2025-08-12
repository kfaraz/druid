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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Iterates over all eligible compaction jobs in order of their priority.
 * TODO: Remaining items:
 *  - task slot logic in canRunJob
 *  - if the granularity of this rule leaves gaps in the timeline, should they be filled be a prior rule
 *  - because there can always be gaps depending on the current date
 *  - maybe we can realign intervals if needed
 *  - compute multi rule skip intervals correctly
 *  - maybe use searchInterval instead of skipIntervals
 *  - supervisors: cancel a task (on supervisor update or when new jobs are computed)
 *  - how does this whole thing affect queuedIntervals
 *    - for duty, it doesn't matter
 *    - for supervisors, intervals will always be mutually exclusive
 *  - CATALOG changes
 */
public class CompactionJobQueue
{
  private static final Logger log = new Logger(CompactionJobQueue.class);

  private final CompactionJobParams jobParams;
  private final CompactionCandidateSearchPolicy searchPolicy;

  private final CompactionStatusTracker statusTracker;
  private final TaskActionClientFactory taskActionClientFactory;
  private final OverlordClient overlordClient;

  private final CompactionSnapshotBuilder snapshotBuilder;
  private final PriorityQueue<CompactionJob> queue;
  private final CoordinatorRunStats runStats;

  public CompactionJobQueue(
      DataSourcesSnapshot dataSourcesSnapshot,
      CompactionCandidateSearchPolicy searchPolicy,
      CompactionStatusTracker statusTracker,
      TaskActionClientFactory taskActionClientFactory,
      OverlordClient overlordClient,
      ObjectMapper objectMapper
  )
  {
    this.searchPolicy = searchPolicy;
    this.queue = new PriorityQueue<>(
        (o1, o2) -> searchPolicy.compareCandidates(o1.getCandidate(), o2.getCandidate())
    );
    this.jobParams = new CompactionJobParams(
        DateTimes.nowUtc(),
        objectMapper,
        dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource()::get
    );

    this.runStats = new CoordinatorRunStats();
    this.snapshotBuilder = new CompactionSnapshotBuilder(runStats);
    this.taskActionClientFactory = taskActionClientFactory;
    this.overlordClient = overlordClient;
    this.statusTracker = statusTracker;
  }

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

      if (canStartJob(job, searchPolicy)) {
        statusTracker.onTaskSubmitted(task.getId(), job.getCandidate());
        FutureUtils.getUnchecked(overlordClient.runTask(task.getId(), task), true);
        runStats.add(Stats.Compaction.SUBMITTED_TASKS, RowKey.of(Dimension.DATASOURCE, task.getDataSource()), 1);
      }
    }

    // TODO: Handle the skipped and the already compacted stuff
  }

  public Map<String, AutoCompactionSnapshot> getCompactionSnapshots()
  {
    return snapshotBuilder.build();
  }

  public CoordinatorRunStats getRunStats()
  {
    return runStats;
  }

  private boolean canStartJob(CompactionJob job, CompactionCandidateSearchPolicy policy)
  {
    if (!areCompactionTaskSlotsAvailable(job) || isTaskReady(job)) {
      return false;
    }

    final CompactionCandidate candidate = job.getCandidate();
    final CompactionStatus compactionStatus = getCurrentStatusForJob(job, policy);

    if (compactionStatus.isComplete()) {
      snapshotBuilder.addToComplete(candidate);
    } else if (compactionStatus.isSkipped()) {
      snapshotBuilder.addToSkipped(candidate);
    } else {
      snapshotBuilder.addToPending(candidate);
    }

    return compactionStatus.getState() == CompactionStatus.State.PENDING;
  }

  private boolean areCompactionTaskSlotsAvailable(CompactionJob job)
  {
    // track the task slot counts somehow
    // this should be probably be governed by the compaction task slots thing
    return true;
  }

  private boolean isTaskReady(CompactionJob job)
  {
    // Assume MSQ jobs to be always ready
    if (job.isMsq()) {
      return true;
    }

    final Task task = job.getNonNullTask();
    try {
      return task.isReady(taskActionClientFactory.create(task));
    }
    catch (Exception e) {
      log.error(e, "Error while checking readiness of task[%s]", task.getId());
      return false;
    }
  }

  public CompactionStatus getCurrentStatusForJob(CompactionJob job, CompactionCandidateSearchPolicy policy)
  {
    // TODO: check the submitted tasks to determine if task should be skipped due to
    //  - locked intervals
    final CompactionStatus compactionStatus = statusTracker.computeCompactionStatus(job.getCandidate(), policy);
    final CompactionCandidate candidatesWithStatus = job.getCandidate().withCurrentStatus(null);
    statusTracker.onCompactionStatusComputed(candidatesWithStatus, null);
    return compactionStatus;
  }
}
