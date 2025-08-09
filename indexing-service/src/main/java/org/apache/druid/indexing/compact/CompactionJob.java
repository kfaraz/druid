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

import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.supervisor.BatchIndexingJob;
import org.apache.druid.server.compaction.CompactionCandidate;

import java.util.Objects;

/**
 */
public class CompactionJob extends BatchIndexingJob
{
  private final CompactionCandidate candidate;

  public CompactionJob(Task task, CompactionCandidate candidate)
  {
    super(task, null);
    this.candidate = candidate;
  }

  public String getDataSource()
  {
    return Objects.requireNonNull(getTask()).getDataSource();
  }

  public CompactionCandidate getCandidate()
  {
    return candidate;
  }
}
