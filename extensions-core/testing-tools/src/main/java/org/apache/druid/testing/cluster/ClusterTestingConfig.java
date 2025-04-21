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

import javax.annotation.Nullable;

public class ClusterTestingConfig
{
  private final OverlordClientConfig overlordClient;

  @JsonCreator
  public ClusterTestingConfig(
      @JsonProperty("overlordClient") @Nullable OverlordClientConfig overlordClient
  )
  {
    this.overlordClient = overlordClient;
  }

  public OverlordClientConfig getOverlordClientConfig()
  {
    return overlordClient;
  }

  @Override
  public String toString()
  {
    return "ClusterTestingConfig{" +
           "overlordClient=" + overlordClient +
           '}';
  }

  /**
   * Config for overlord client.
   */
  public static class OverlordClientConfig
  {
    private final Long segmentPublishDelay;

    @JsonCreator
    public OverlordClientConfig(
        @JsonProperty("segmentPublishDelay") @Nullable Long segmentPublishDelay
    )
    {
      this.segmentPublishDelay = segmentPublishDelay;
    }

    @Nullable
    public Long getSegmentPublishDelay()
    {
      return segmentPublishDelay;
    }

    @Override
    public String toString()
    {
      return "OverlordClientConfig{" +
             "segmentPublishDelay=" + segmentPublishDelay +
             '}';
    }
  }
}
