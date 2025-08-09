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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * Iterates over all eligible compaction jobs in order of their priority.
 *
 * TODO:
 *  - track stats
 *  - build snapshot for each datasource
 */
public class PriorityBasedCompactionJobIterator implements Iterator<CompactionJob>
{
  private static final Logger log = new Logger(PriorityBasedCompactionJobIterator.class);

  private final PriorityQueue<CompactionJob> queue;

  public static PriorityBasedCompactionJobIterator create(
      CompactionCandidateSearchPolicy searchPolicy,
      Map<String, CompactionSupervisor> supervisors,
      CompactionJobParams jobParams
  )
  {
    final PriorityBasedCompactionJobIterator iterator = new PriorityBasedCompactionJobIterator(searchPolicy);

    supervisors.forEach(
        (datasource, supervisor) -> iterator.createAndEnqueueEligibleJobs(supervisor, jobParams)
    );

    return iterator;
  }

  private PriorityBasedCompactionJobIterator(CompactionCandidateSearchPolicy searchPolicy)
  {
    this.queue = new PriorityQueue<>(
        (o1, o2) -> searchPolicy.compareCandidates(o1.getCandidate(), o2.getCandidate())
    );
  }

  public List<CompactionCandidate> getCompactedSegments()
  {
    return datasourceToHolder.values().stream().flatMap(
        iterator -> iterator.getCompactedSegments().stream()
    ).collect(Collectors.toList());
  }

  public List<CompactionCandidate> getSkippedSegments()
  {
    return datasourceToHolder.values().stream().flatMap(
        iterator -> iterator.getSkippedSegments().stream()
    ).collect(Collectors.toList());
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public CompactionJob next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final CompactionJob entry = queue.poll();
    if (entry == null) {
      throw new NoSuchElementException();
    }

    return entry;
  }

  private void createAndEnqueueEligibleJobs(CompactionSupervisor supervisor, CompactionJobParams params)
  {
    if (supervisor.shouldCreateJobs(params)) {
      for (CompactionJob job : supervisor.createJobs(params)) {
        if (supervisor.canRunJob(job, params)) {
          queue.add(job);
        } else {
          log.info("Skipping job[%s] for supervisor[%s]", job, supervisor.getSpec().getId());
        }
      }
    }
  }
}
