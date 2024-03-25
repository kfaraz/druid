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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Contains row stats for the following task phases:
 * <ul>
 * <li>{@link RowIngestionMeters#BUILD_SEGMENTS}</li>
 * <li>{@link RowIngestionMeters#DETERMINE_PARTITIONS}</li>
 * </ul>
 */
public class MultiPhaseRowStats<V>
{
  private final V buildSegments;
  private final V determinePartitions;

  @JsonCreator
  public MultiPhaseRowStats(
      @JsonProperty(RowIngestionMeters.BUILD_SEGMENTS) @Nullable V buildSegments,
      @JsonProperty(RowIngestionMeters.DETERMINE_PARTITIONS) @Nullable V determinePartitions
  )
  {
    this.buildSegments = buildSegments;
    this.determinePartitions = determinePartitions;
  }

  @Nullable
  @JsonProperty
  public V getBuildSegments()
  {
    return buildSegments;
  }

  @Nullable
  @JsonProperty
  public V getDeterminePartitions()
  {
    return determinePartitions;
  }

  public static <V> MultiPhaseRowStats<V> buildSegments(V value)
  {
    return new MultiPhaseRowStats<>(value, null);
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
    MultiPhaseRowStats<?> that = (MultiPhaseRowStats<?>) o;
    return buildSegments.equals(that.buildSegments)
           && determinePartitions.equals(that.determinePartitions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(buildSegments, determinePartitions);
  }

  @Override
  public String toString()
  {
    return "MultiPhaseRowStats{" +
           "buildSegments=" + buildSegments +
           ", determinePartitions=" + determinePartitions +
           '}';
  }
}
