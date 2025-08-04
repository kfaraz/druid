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

package org.apache.druid.indexing.overlord.supervisor;

import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;

import java.util.List;

/**
 * TODO: Motivation:
 *  - batch supervisor as an analog of streaming supervisor
 *  - ties in to the indexing template
 *  - remove duplicate code
 *
 * TODO: tasks:
 *  - Wire up CompactionSupervisor
 *  - Wire up Multi Rule
 *  - Run tests for both
 *  - Wire up scheduled batch
 *  - Write embedded tests for all
 *
 * TODO: spec vs supervisor
 *  - spec is the persisted object
 *  - supervisor has a lifecycle start and stop
 *  - any logic which has to do with the lifecycle should be in the supervisor
 *  - so stuff like status, progress, etc. is definitely in the supervisor
 *  - what else can go here?
 *  - spec creates the supervisor
 *  - boilerplate stuff should go into the supervisor so that spec can remain just an interface
 *  - similar to streaming
 *
 * TODO:
 *  - start with 3 impls: batch, compaction, multi rule compaction
 *  - Supervisor probably needs to contain stuff that the scheduler is going to ask for
 *  - Is there going to be a lot of retro-fitting? Are we fighting against stuff?
 *  - It should all come and fit naturally.
 *  - OVERALL FLOW:
 *  - Scheduler runs every 5 s (or triggered?)
 *  - Each supervisor (spec?) has a target datasource
 *  - Given target datasource, we have the timeline, which can be empty or not
 *  - Ask the supervisor what needs to be done
 *    - Supervisor checks status of previously submitted tasks
 *    - Supervisor checks the current state of the timeline
 *    - Supervisor creates one or more Tasks for different parts of the timeline
 *    - List of Tasks has implicit priority, we will just submit all of them to the TaskQueue
 *    - Supervisor provides validation of the Tasks
 *  - Run them
 *  - Since scheduler is running every few seconds, we don't need further prioritization
 *  - So we can just launch the Tasks provided by the supervisor, at least for now
 *  - We can worry about the other stuff later
 */
public interface BatchIndexingSupervisor<S extends BatchIndexingSupervisorSpec> extends Supervisor
{
  /**
   * @return the spec for this supervisor.
   */
  S getSpec();

  /**
   * Creates jobs to be launched in the current run of the scheduler.
   *
   * @param timeline    Timeline of the target datasource
   * @param currentTime Time when the current run of the scheduler started
   * @return Empty list if no tasks are to be submitted in the current run of the
   * scheduler.
   */
  List<BatchIndexingJob> createJobs(SegmentTimeline timeline, DateTime currentTime);

  /**
   * Checks if a given job is valid for this supervisor (spec?).
   */
  boolean validateJob(BatchIndexingJob job);
}
