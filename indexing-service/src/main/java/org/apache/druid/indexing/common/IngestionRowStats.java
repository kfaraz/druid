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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.incremental.MultiPhaseRowStats;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class IngestionRowStats extends MultiPhaseRowStats<RowIngestionMetersTotals>
{
  private static final IngestionRowStats EMPTY = new IngestionRowStats(null, null, null, null);

  private final MultiPhaseRowStats<RowIngestionMetersTotals> totals;
  private final MultiPhaseRowStats<Map<String, Object>> movingAverages;

  @JsonCreator
  public IngestionRowStats(
      @JsonProperty(RowIngestionMeters.BUILD_SEGMENTS) @Nullable
          RowIngestionMetersTotals buildSegments,
      @JsonProperty(RowIngestionMeters.DETERMINE_PARTITIONS) @Nullable
          RowIngestionMetersTotals determinePartitions,
      @JsonProperty(RowIngestionMeters.TOTALS) @Nullable
          MultiPhaseRowStats<RowIngestionMetersTotals> totals,
      @JsonProperty(RowIngestionMeters.MOVING_AVERAGES) @Nullable
          MultiPhaseRowStats<Map<String, Object>> movingAverages
  )
  {
    super(buildSegments, determinePartitions);

    if ((buildSegments != null || determinePartitions != null)
        && (totals != null || movingAverages != null)) {
      throw InvalidInput.exception("");
    }

    this.totals = totals;
    this.movingAverages = movingAverages;
  }

  @JsonProperty
  public MultiPhaseRowStats<RowIngestionMetersTotals> getTotals()
  {
    return totals;
  }

  @Nullable
  @JsonProperty
  public MultiPhaseRowStats<Map<String, Object>> getMovingAverages()
  {
    return movingAverages;
  }

  @JsonIgnore
  public MultiPhaseRowStats<RowIngestionMetersTotals> getOverallTotals()
  {
    return totals == null ? this : totals;
  }

  public static IngestionRowStats empty()
  {
    return EMPTY;
  }

  public static IngestionRowStats totalsForBuildSegmentsPhase(RowIngestionMetersTotals totals)
  {
    return new IngestionRowStats(null, null, new MultiPhaseRowStats<>(totals, null), null);
  }

  public static IngestionRowStats buildSegments(RowIngestionMetersTotals stats)
  {
    return new IngestionRowStats(stats, null, null, null);
  }

  public static IngestionRowStats totalsAndMovingAverages(
      MultiPhaseRowStats<RowIngestionMetersTotals> totals,
      MultiPhaseRowStats<Map<String, Object>> movingAverages
  )
  {
    return new IngestionRowStats(null, null, totals, movingAverages);
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
    IngestionRowStats that = (IngestionRowStats) o;
    return Objects.equals(totals, that.totals)
           && Objects.equals(movingAverages, that.movingAverages);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(totals, movingAverages);
  }

  @Override
  public String toString()
  {
    return "IngestionRowStats{" +
           "totals=" + totals +
           ", movingAverages=" + movingAverages +
           '}';
  }

  // use the right constructor which accepts two sets of things
  // and then go from there.

  // index/index_parallel/single_phase_sub_task:
  //    totals -> buildSegments -> processed -> 24
  // partial_index_generate/index_kafka/index_kinesis/index_realtime_appenderator:
  //    buildSegments -> processed -> 24
  // index_hadoop:
  //    buildSegments -> rowsProcessed -> 24

}
