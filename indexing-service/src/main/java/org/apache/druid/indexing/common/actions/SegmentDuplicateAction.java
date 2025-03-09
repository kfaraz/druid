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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Set;

public class SegmentDuplicateAction implements TaskAction<Integer>
{
  private static final int MAX_SEGMENTS_IN_INTERVAL = 1000;

  private final String dataSource;
  private final String segmentId;
  private final Interval startInterval;
  private final int maxCount;

  @JsonCreator
  public SegmentDuplicateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("maxCount") int maxCount,
      @JsonProperty("startInterval") Interval startInterval
  )
  {
    this.dataSource = dataSource;
    this.segmentId = segmentId;
    this.maxCount = maxCount;
    this.startInterval = startInterval;
  }

  @Override
  public TypeReference<Integer> getReturnTypeReference()
  {
    return new TypeReference<>()
    {
    };
  }

  @Override
  public Integer perform(Task task, TaskActionToolbox toolbox)
  {
    // Get a segment
    // Make multiple copies of it, without breaking time chunk limits
    // It probably doesn't need to handle so many cases

    final DataSegment baseSegment = toolbox.getIndexerMetadataStorageCoordinator()
                                           .retrieveSegmentForId(dataSource, segmentId);


    final long duration = startInterval.toDurationMillis();

    int totalSegmentCount = 0;
    for (int intervalIndex = 0; ; ++intervalIndex) {
      final int requiredSegmentCount = Math.min(maxCount - totalSegmentCount, MAX_SEGMENTS_IN_INTERVAL);
      if (requiredSegmentCount <= 0) {
        break;
      }

      final Interval interval = new Interval(
          startInterval.getStart().plus(intervalIndex * duration),
          startInterval.getEnd().plus(intervalIndex * duration)
      );

      final Set<DataSegment> segments = new HashSet<>();
      for (int partitionNumber = 0; partitionNumber < requiredSegmentCount; ++partitionNumber) {
        segments.add(
            DataSegment.builder(baseSegment)
                       .interval(interval)
                       .shardSpec(new NumberedShardSpec(partitionNumber, 1))
                       .build()
        );
      }

      totalSegmentCount += toolbox.getIndexerMetadataStorageCoordinator()
                                  .commitSegments(segments, null).size();
    }

    return totalSegmentCount;
  }

  @Override
  public String toString()
  {
    return "SegmentDuplicateAction{" +
           "dataSource='" + dataSource + '\'' +
           ", segmentId='" + segmentId + '\'' +
           ", startInterval=" + startInterval +
           ", maxCount=" + maxCount +
           '}';
  }
}
