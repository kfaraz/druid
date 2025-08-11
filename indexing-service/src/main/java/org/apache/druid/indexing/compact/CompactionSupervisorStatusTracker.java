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
import com.google.inject.Inject;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;

/**
 * There is still quite a bit of logic in CompactSegments.
 * When should we cancel a task?
 *
 * When a supervisor is updated, we should just check all the compaction tasks
 * and see if anything needs to be canceled?
 *
 *  Option 1:
 *  - Override computeCompactionStatus() to account for locked intervals too.
 *  - It should also account for task slots
 *  - Continue passing the status tracker into the flow
 *  - Coordinator duty remains unchanged
 *
 *  Option 2:
 *  - Create all jobs.
 *  - Pass in a dummy status tracker which just says OK to everything.
 *  - Then filter out jobs based on actual compaction status.
 *  - Coordinator duty remains unchanged.
 *
 *  Option 3:
 *  - Similar to option 2 but update the Coordinator duty too.
 *  - No dummy status tracker required.
 *  - Duty gets all the candidates, then runs them through the status tracker one by one.
 *  - Only 1 item remains, which is the partial eternity thing.
 *  - So we keep the `getSkippedSegments()` method
 *  -
 */
public class CompactionSupervisorStatusTracker extends CompactionStatusTracker
{
  @Inject
  public CompactionSupervisorStatusTracker(ObjectMapper objectMapper)
  {
    super(objectMapper);
  }

  public boolean canRunJob(CompactionJob job)
  {
    return areTaskSlotsAvailable(job)
           && computeCompactionStatus(job).getState() == CompactionStatus.State.PENDING;
  }

  public void addPendingJobToSnapshot(CompactionJob job, CompactionSnapshotBuilder snapshotBuilder)
  {
    final CompactionStatus compactionStatus = computeCompactionStatus(job);
    final CompactionCandidate candidate = job.getCandidate();

    if (compactionStatus.isComplete()) {
      snapshotBuilder.addToComplete(candidate);
    } else if (compactionStatus.isSkipped()) {
      snapshotBuilder.addToSkipped(candidate);
    }
  }

  private boolean areTaskSlotsAvailable(CompactionJob job)
  {
    // track the task slot counts somehow
    // this should be probably be governed by the compaction task slots thing
  }

  private CompactionStatus computeCompactionStatus(CompactionJob job)
  {
    // TODO: check the submitted tasks to determine if task should be skipped due to
    //  - locked intervals
    //  - already running
    //  - any other reason
    final CompactionStatus compactionStatus = super.computeCompactionStatus(

    );
    final CompactionCandidate candidatesWithStatus = job.getCandidate().withCurrentStatus(compactionStatus);
    onCompactionStatusComputed(candidatesWithStatus, config);
  }
}
