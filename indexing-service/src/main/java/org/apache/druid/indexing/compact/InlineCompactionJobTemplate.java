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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;

import java.util.List;

/**
 * Template to create compaction jobs using inline specifications. This template
 * does not fetch any information from the Druid catalog while creating jobs.
 * <p>
 * This template does not contain all the fields supported by
 * {@link InlineSchemaDataSourceCompactionConfig} since some of those fields may
 * change the data itself (and not just its layout) and are thus not considered
 * compaction-compatible.
 */
public class InlineCompactionJobTemplate implements CompactionJobTemplate
{
  private final UserCompactionTaskQueryTuningConfig tuningConfig;
  private final Granularity segmentGranularity;

  @JsonCreator
  public InlineCompactionJobTemplate(
      @JsonProperty("tuningConfig") UserCompactionTaskQueryTuningConfig tuningConfig,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    this.tuningConfig = tuningConfig;
    this.segmentGranularity = segmentGranularity;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @JsonProperty
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public List<CompactionJob> createJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams jobParams
  )
  {
    final String dataSource = ensureDruidInputSource(source).getDataSource();
    return new CompactionConfigBasedJobTemplate(
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withTuningConfig(tuningConfig)
            .withGranularitySpec(new UserCompactionTaskGranularityConfig(segmentGranularity, null, null))
            .build()
    ).createJobs(source, destination, jobParams);
  }
}
