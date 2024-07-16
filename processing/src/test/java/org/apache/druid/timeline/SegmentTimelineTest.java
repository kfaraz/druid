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

package org.apache.druid.timeline;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SegmentTimelineTest
{

  @Test
  public void testIsOvershadowed()
  {
    final SegmentTimeline timeline = SegmentTimeline.forSegments(
        Arrays.asList(
            createSegment("2022-01-01/2022-01-02", "v1", 0, 3),
            createSegment("2022-01-01/2022-01-02", "v1", 1, 3),
            createSegment("2022-01-01/2022-01-02", "v1", 2, 3),
            createSegment("2022-01-02/2022-01-03", "v2", 0, 2),
            createSegment("2022-01-02/2022-01-03", "v2", 1, 2)
        )
    );

    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 1, 3))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 2, 3))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01/2022-01-02", "v1", 1, 4))
    );
    Assert.assertFalse(
        timeline.isOvershadowed(createSegment("2022-01-01T00:00:00/2022-01-01T06:00:00", "v1", 1, 4))
    );

    Assert.assertTrue(
        timeline.isOvershadowed(createSegment("2022-01-02/2022-01-03", "v1", 2, 4))
    );
    Assert.assertTrue(
        timeline.isOvershadowed(createSegment("2022-01-02/2022-01-03", "v1", 0, 1))
    );
  }

  @Test
  public void testDayOvershadowsMonth()
  {
    final DataSegment doubleDaySegment = createSegment("2024-01-01/P2D", "v1", 0, 1);
    final DataSegment day1Segment = createSegment("2024-01-01/P1D", "v2", 0, 1);
    final DataSegment day2Segment = createSegment("2024-01-02/P1D", "v3", 0, 1);

    final SegmentTimeline timeline = SegmentTimeline.forSegments(
        Arrays.asList(doubleDaySegment, day1Segment, day2Segment)
    );

    Assert.assertFalse(timeline.isOvershadowed(day1Segment));
    // Assert.assertTrue(timeline.isOvershadowed(monthSegment));

    List<TimelineObjectHolder<String, DataSegment>> timelineEntries = timeline.lookup(day1Segment.getInterval());
    for (TimelineObjectHolder<String, DataSegment> entry : timelineEntries) {
      for (PartitionChunk<DataSegment> segment : entry.getObject()) {
        System.out.println("Day 1 has segment: " + segment.getObject().getId());
      }
    }

    timelineEntries = timeline.lookup(doubleDaySegment.getInterval());
    for (TimelineObjectHolder<String, DataSegment> entry : timelineEntries) {
      for (PartitionChunk<DataSegment> segment : entry.getObject()) {
        System.out.println("Double day has segment: " + segment.getObject().getId());
      }
    }

    timeline.remove(
        day2Segment.getInterval(),
        day2Segment.getVersion(),
        day2Segment.getShardSpec().createChunk(day2Segment)
    );
    System.out.println("Removed segment for Day 2");

    timelineEntries = timeline.lookup(day1Segment.getInterval());
    for (TimelineObjectHolder<String, DataSegment> entry : timelineEntries) {
      for (PartitionChunk<DataSegment> segment : entry.getObject()) {
        System.out.println("Day 1 has segment: " + segment.getObject().getId());
      }
    }

    timelineEntries = timeline.lookup(doubleDaySegment.getInterval());
    for (TimelineObjectHolder<String, DataSegment> entry : timelineEntries) {
      for (PartitionChunk<DataSegment> segment : entry.getObject()) {
        System.out.println("Double day has segment: " + segment.getObject().getId());
      }
    }
  }

  private DataSegment createSegment(String interval, String version, int partitionNum, int totalNumPartitions)
  {
    return new DataSegment(
        "wiki",
        Intervals.of(interval),
        version,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        new NumberedShardSpec(partitionNum, totalNumPartitions),
        0x9,
        1L
    );
  }
}
