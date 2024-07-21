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
  private final Map<String, DatasourceStatus> datasourceStatuses = new HashMap<>();
  private final Map<String, ClientCompactionTaskQuery> submittedTaskIdToPayload = new HashMap<>();

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
    final CompactionStatus compactionStatus = CompactionStatus.compute(candidate, config, objectMapper);
    if (compactionStatus.isComplete()) {
      return compactionStatus;
    }

    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    if (candidate.getTotalBytes() > inputSegmentSize) {
      return CompactionStatus.skipped(
          "Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
          candidate.getTotalBytes(), inputSegmentSize
      );
    }

    final Interval compactionInterval = candidate.getUmbrellaInterval();

    final IntervalStatus intervalStatus
        = datasourceStatuses.getOrDefault(config.getDataSource(), DatasourceStatus.EMPTY)
                            .getIntervalStatuses()
                            .get(compactionInterval);

    if (intervalStatus == null) {
      return compactionStatus;
    }

    switch (intervalStatus.state) {
      case TASK_SUBMITTED:
      case COMPACTED:
      case FAILED_ALL_RETRIES:
        return CompactionStatus.skipped(
            "Interval[%s] was recently submitted for compaction and has state[%s].",
            compactionInterval, intervalStatus.state
        );
      default:
        break;
    }

    return compactionStatus;
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
    final Set<String> allDatasources = new HashSet<>(datasourceStatuses.keySet());
    allDatasources.forEach(datasource -> {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceStatuses.remove(datasource);
      }
    });
  }

  public void onTaskSubmitted(
      ClientCompactionTaskQuery taskPayload,
      SegmentsToCompact candidateSegments
  )
  {
    final DatasourceStatus datasourceStatus = getOrComputeDatasourceStatus(taskPayload.getDataSource());

    datasourceStatus.getIntervalStatuses().computeIfAbsent(
        candidateSegments.getUmbrellaInterval(),
        i -> new IntervalStatus(IntervalState.TASK_SUBMITTED, 0)
    );

    submittedTaskIdToPayload.put(taskPayload.getId(), taskPayload);
  }

  public void onTaskFinished(String taskId, TaskStatus taskStatus)
  {
    if (!taskStatus.isComplete()) {
      return;
    }

    final ClientCompactionTaskQuery taskPayload = submittedTaskIdToPayload.remove(taskId);
    if (taskPayload == null) {
      // Nothing to do since we don't know the corresponding datasource or interval
      return;
    }

    final DatasourceStatus datasourceStatus
        = getOrComputeDatasourceStatus(taskPayload.getDataSource());
    final Interval compactionInterval = taskPayload.getIoConfig().getInputSpec().getInterval();

    final IntervalStatus lastKnownStatus = datasourceStatus.getIntervalStatuses().get(compactionInterval);

    if (taskStatus.isSuccess()) {
      datasourceStatus.intervalStatus.put(
          compactionInterval,
          new IntervalStatus(IntervalState.COMPACTED, 10)
      );
    } else if (lastKnownStatus == null) {
      // This is the first failure
      datasourceStatus.intervalStatus.put(
          compactionInterval,
          new IntervalStatus(IntervalState.FAILED, 0)
      );
    } else if (lastKnownStatus.state == IntervalState.FAILED
               && ++lastKnownStatus.retryCount > 10) {
      // Failure retries have been exhausted
      datasourceStatus.intervalStatus.put(
          compactionInterval,
          new IntervalStatus(IntervalState.FAILED_ALL_RETRIES, 10)
      );
    }
  }

  public void reset()
  {
    datasourceStatuses.clear();
  }

  private DatasourceStatus getOrComputeDatasourceStatus(String datasource)
  {
    return datasourceStatuses.computeIfAbsent(datasource, ds -> new DatasourceStatus());
  }

  private static class DatasourceStatus
  {
    static final DatasourceStatus EMPTY = new DatasourceStatus();

    final Map<Interval, IntervalStatus> intervalStatus = new HashMap<>();

    Map<Interval, IntervalStatus> getIntervalStatuses()
    {
      return intervalStatus;
    }
  }

  private static class IntervalStatus
  {
    final IntervalState state;
    int turnsToSkip;
    int retryCount;

    IntervalStatus(IntervalState state, int turnsToSkip)
    {
      this.state = state;
      this.turnsToSkip = turnsToSkip;
    }

    void markSkipped()
    {
      this.turnsToSkip--;
    }
  }

  private enum IntervalState
  {
    TASK_SUBMITTED, COMPACTED, FAILED, FAILED_ALL_RETRIES
  }
}
