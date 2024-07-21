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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tracks status of both recently submitted compaction tasks and the compaction
 * state of segments. Can be used to check if a set of segments is currently
 * eligible for compaction.
 */
public class CompactionStatusTracker
{
  private static final Logger log = new Logger(CompactionStatusTracker.class);

  private final ObjectMapper objectMapper;
  private final Map<String, Set<Interval>> datasourceToRecentlySubmittedIntervals = new HashMap<>();

  @Inject
  public CompactionStatusTracker(
      ObjectMapper objectMapper
  )
  {
    this.objectMapper = objectMapper;
  }

  public CompactionStatus computeCompactionStatus(
      SegmentsToCompact candidate,
      DataSourceCompactionConfig config
  )
  {
    final CompactionStatus status = CompactionStatus.compute(candidate, config, objectMapper);
    if (status.isComplete()) {
      return status;
    }

    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    if (candidate.getTotalBytes() > inputSegmentSize) {
      return CompactionStatus.skipped(
          "Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
          candidate.getTotalBytes(), inputSegmentSize
      );
    }

    final Set<Interval> recentlySubmittedIntervals
        = datasourceToRecentlySubmittedIntervals.getOrDefault(config.getDataSource(), Collections.emptySet());
    if (recentlySubmittedIntervals.contains(candidate.getUmbrellaInterval())) {
      return CompactionStatus.skipped(
          "Interval[%s] has been recently submitted for compaction",
          candidate.getUmbrellaInterval()
      );
    }

    return status;
  }

  public void onCompactionConfigUpdated(CoordinatorCompactionConfig compactionConfig)
  {
    final Set<String> compactionEnabledDatasources = new HashSet<>();
    if (compactionConfig.getCompactionConfigs() != null) {
      compactionConfig.getCompactionConfigs().forEach(
          config -> compactionEnabledDatasources.add(config.getDataSource())
      );
    }

    // Clean up state for datasources where compaction has been freshly disabled
    final Set<String> allDatasources = new HashSet<>(datasourceToRecentlySubmittedIntervals.keySet());
    allDatasources.forEach(datasource -> {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceToRecentlySubmittedIntervals.remove(datasource);
      }
    });
  }

  public void onTaskSubmitted(
      ClientCompactionTaskQuery taskPayload,
      SegmentsToCompact candidateSegments
  )
  {
    datasourceToRecentlySubmittedIntervals
        .computeIfAbsent(taskPayload.getDataSource(), ds -> new HashSet<>())
        .add(candidateSegments.getUmbrellaInterval());
  }

  public void onTaskFinished(String taskId, TaskStatus taskStatus)
  {
    log.info("Task[%s] has new status[%s].", taskId, taskStatus);

    if (taskStatus.isFailure()) {
      // TODO: retry logic and other stuff
    }

    // Do not remove the interval of this task from recently submitted intervals
    // as there might be some delay before the segments of this task are published
    // and updated in the timeline. If compaction duty runs before the segments
    // are published, it might re-submit the same task.
  }

  public void reset()
  {
    datasourceToRecentlySubmittedIntervals.clear();
  }
}
