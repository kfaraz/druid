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

import java.util.List;

/**
 * TODO: Motivation:
 *  - batch supervisor as an analog of streaming supervisor
 *  - ties in to the indexing template
 *  - remove duplicate code
 * <p>
 * TODO: tasks:
 *  - Wire up CompactionSupervisor
 *  - Wire up Multi Rule
 *  - Run tests for both
 *  - Wire up scheduled batch
 *  - Write embedded tests for all
 * <p>
 * TODO: OVERALL FLOW:
 *  - Scheduler runs every 5 s (or triggered?)
 *  - Get target datasource of supervisor
 *  - Get the timeline
 *  - Ask supervisor to create jobs
 *    - For compaction:
 *    - Pass period, timeline and other stuff to compactible iterator
 *    - CompactionJobTemplate knows how to create a compaction job
 *    - For each candidate, create a job
 *  - Ask the supervisor which jobs can be run now
 *    - If current job requires more slots than available, should we check later jobs?
 *      - I guess so 🤷🏻
 *    - Filter out intervals that are locked, already running or recently done or something
 *  - Run them
 *  - If skipped, track reason somewhere
 */
public interface BatchIndexingSupervisor
    <J extends BatchIndexingJob, P extends JobParams> extends Supervisor
{
  /**
   * Checks if this supervisor is ready to create jobs in the current run.
   *
   * @param jobParams Parameters for the current run of the scheduler.
   */
  boolean shouldCreateJobs(P jobParams);

  /**
   * Creates jobs to be launched in the current run of the scheduler.
   *
   * @param jobParams Parameters for the current run of the scheduler.
   * @return Empty iterator if no tasks are to be submitted in the current run
   * of the scheduler.
   */
  List<J> createJobs(P jobParams);

  /**
   * Checks if a given job is valid and can be run right now.
   *
   * @param jobParams Parameters for the current run of the scheduler.
   */
  boolean canRunJob(J job, P jobParams);
}
