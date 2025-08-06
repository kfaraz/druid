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
import org.apache.druid.server.compaction.DataSourceCompactibleSegmentIterator;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.List;

/**
 * TODO:
 *  - what do we really need here?
 *  - the config that we have passed in here, what should it have?
 *  - is this really that different from the catalog-based template?
 *  - I doubt if there is going to be any real difference
 *
 * TODO: steps:
 *  - validate the input and output
 *  - get the timeline
 *  - create a datasourcecompactibleiterator
 *  - map each item to a Job
 *
 * TODO: For cascading, impl would definitely be different but what would the template look like:
 *  - not like the one below, I feel
 */
public class InlineCompactionJobTemplate implements CompactionJobTemplate
{
  private final InlineSchemaDataSourceCompactionConfig config;

  public InlineCompactionJobTemplate(InlineSchemaDataSourceCompactionConfig config)
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
        List.of(),
        null,
        // Do we need a search policy? I think so since we are giving back an iterator? Alternatively, the template can just give a list
        // and then we do whatever we want with this
        // template shouldn't have to prioritize stuff
        // if we pass in the policy, it becomes a very specific thing
        // we can always use an interface that changes as required
        // like JobParams
        // which can definitely change as needed
        // I think that might be useful in the long run
        null // Do we need the status tracker here, ideally both of these things should come later
        // in shouldRunJob or something, probably shouldn't even be a part of the template
    );

    return null;
  }
}
