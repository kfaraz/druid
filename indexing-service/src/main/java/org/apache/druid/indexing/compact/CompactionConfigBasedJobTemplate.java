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

import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.DataSourceCompactibleSegmentIterator;
import org.apache.druid.server.compaction.NewestSegmentFirstPolicy;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.ArrayList;
import java.util.List;

/**
 * This template never needs to be deserialized as a {@code BatchIndexingJobTemplate},
 * and simply uses a {@link DataSourceCompactionConfig} to create compaction jobs.
 */
public class CompactionConfigBasedJobTemplate implements CompactionJobTemplate
{
  private final DataSourceCompactionConfig config;

  public CompactionConfigBasedJobTemplate(DataSourceCompactionConfig config)
  {
    this.config = config;
  }

  @Override
  public List<CompactionJob> createJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams params
  )
  {
    // Validate the input and output
    if (!(source instanceof DruidInputSource)) {
      throw InvalidInput.exception("Invalid input source[%s] for compaction.", source);
    }

    final DruidInputSource druidDatasource = (DruidInputSource) source;

    final SegmentTimeline timeline = null;
    final DataSourceCompactibleSegmentIterator segmentIterator = new DataSourceCompactibleSegmentIterator(
        config,
        timeline,
        Intervals.complementOf(params.getInterval()),
        new NewestSegmentFirstPolicy(null),
        null
    );

    final List<CompactionJob> jobs = new ArrayList<>();
    while (segmentIterator.hasNext()) {
      final CompactionCandidate candidate = segmentIterator.next();

      // Use the candidate and the config to create a Task
      // Any other stuff that we might need should come from params
      // What other stuff might we need?

      // Any validation needed here?
      // jobs.add(CompactionJob.forTask());
    }

    return jobs;
  }
}
