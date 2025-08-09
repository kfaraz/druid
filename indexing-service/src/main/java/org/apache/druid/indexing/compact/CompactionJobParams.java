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
import org.apache.druid.indexing.overlord.supervisor.JobParams;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Parameters used while creating a {@link CompactionJob} using a {@link CompactionJobTemplate}.
 */
public class CompactionJobParams implements JobParams
{
  private final Interval interval;
  private final DateTime scheduleStartTime;
  private final ObjectMapper mapper;
  private final TimelineProvider timelineProvider;

  // TODO: this should be removed later
  private final CompactionStatusTracker statusTracker;

  public CompactionJobParams(
      Interval interval,
      DateTime scheduleStartTime,
      ObjectMapper mapper,
      TimelineProvider timelineProvider,
      CompactionStatusTracker statusTracker
  )
  {
    this.mapper = mapper;
    this.interval = interval;
    this.scheduleStartTime = scheduleStartTime;
    this.timelineProvider = timelineProvider;
    this.statusTracker = statusTracker;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public DateTime getScheduleStartTime()
  {
    return scheduleStartTime;
  }

  public ObjectMapper getMapper()
  {
    return mapper;
  }

  public SegmentTimeline getTimeline(String dataSource)
  {
    return timelineProvider.getTimelineForDataSource(dataSource);
  }

  public CompactionStatusTracker getStatusTracker()
  {
    return statusTracker;
  }

  @FunctionalInterface
  public interface TimelineProvider
  {
    SegmentTimeline getTimelineForDataSource(String dataSource);
  }
}
