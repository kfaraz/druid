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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.config.Configs;
import org.apache.druid.server.coordinator.compact.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.compact.NewestSegmentFirstPolicy;

import javax.annotation.Nullable;
import java.util.Objects;

public class CompactionSchedulerConfig
{
  private static final CompactionSchedulerConfig DEFAULT = new CompactionSchedulerConfig(null, null);

  @JsonProperty
  private final boolean enabled;

  @JsonProperty
  private final CompactionSegmentSearchPolicy compactionPolicy;

  public static CompactionSchedulerConfig defaultConfig()
  {
    return DEFAULT;
  }

  @JsonCreator
  public CompactionSchedulerConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JacksonInject ObjectMapper objectMapper
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.compactionPolicy = new NewestSegmentFirstPolicy(objectMapper);
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public CompactionSegmentSearchPolicy getDefaultCompactionPolicy()
  {
    return compactionPolicy;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompactionSchedulerConfig that = (CompactionSchedulerConfig) o;
    return enabled == that.enabled;
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(enabled);
  }

  @Override
  public String toString()
  {
    return "CompactionSchedulerConfig{" +
           "enabled=" + enabled +
           '}';
  }
}
