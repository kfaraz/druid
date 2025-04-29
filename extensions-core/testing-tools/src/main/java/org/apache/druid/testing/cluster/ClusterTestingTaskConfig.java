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

package org.apache.druid.testing.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Config used for testing scalability of a Druid cluster by introducing faults
 * at various interface points.
 */
public class ClusterTestingTaskConfig
{
  private final OverlordClientConfig overlordClient;
  private final CoordinatorClientConfig coordinatorClient;

  @JsonCreator
  public ClusterTestingTaskConfig(
      @JsonProperty("overlordClient") @Nullable OverlordClientConfig overlordClient,
      @JsonProperty("coordinatorClient") @Nullable CoordinatorClientConfig coordinatorClient
  )
  {
    this.overlordClient = Configs.valueOrDefault(overlordClient, OverlordClientConfig.DEFAULT);
    this.coordinatorClient = Configs.valueOrDefault(coordinatorClient, CoordinatorClientConfig.DEFAULT);
  }

  public OverlordClientConfig getOverlordClientConfig()
  {
    return overlordClient;
  }

  public CoordinatorClientConfig getCoordinatorClient()
  {
    return coordinatorClient;
  }

  @Override
  public String toString()
  {
    return "ClusterTestingConfig{" +
           "overlordClient=" + overlordClient +
           '}';
  }

  /**
   * Config for faulty overlord client.
   */
  public static class OverlordClientConfig
  {
    private static final OverlordClientConfig DEFAULT = new OverlordClientConfig(null, null);

    private final Duration segmentPublishDelay;
    private final Duration segmentAllocateDelay;

    @JsonCreator
    public OverlordClientConfig(
        @JsonProperty("segmentAllocateDelay") @Nullable Duration segmentAllocateDelay,
        @JsonProperty("segmentPublishDelay") @Nullable Duration segmentPublishDelay
    )
    {
      this.segmentAllocateDelay = segmentAllocateDelay;
      this.segmentPublishDelay = segmentPublishDelay;
    }

    @Nullable
    public Duration getSegmentPublishDelay()
    {
      return segmentPublishDelay;
    }

    @Nullable
    public Duration getSegmentAllocateDelay()
    {
      return segmentAllocateDelay;
    }

    @Override
    public String toString()
    {
      return "OverlordClientConfig{" +
             "segmentPublishDelay=" + segmentPublishDelay +
             '}';
    }
  }

  /**
   * Config for faulty coordinator client.
   */
  public static class CoordinatorClientConfig
  {
    private static final CoordinatorClientConfig DEFAULT = new CoordinatorClientConfig(null);

    private final Duration segmentHandoffDelay;

    @JsonCreator
    public CoordinatorClientConfig(
        @JsonProperty("segmentHandoffDelay") @Nullable Duration segmentHandoffDelay
    )
    {
      this.segmentHandoffDelay = segmentHandoffDelay;
    }

    @Nullable
    public Duration getSegmentHandoffDelay()
    {
      return segmentHandoffDelay;
    }

    @Override
    public String toString()
    {
      return "CoordinatorClientConfig{" +
             "segmentHandoffDelay=" + segmentHandoffDelay +
             '}';
    }
  }
}
