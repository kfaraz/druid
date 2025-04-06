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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Config for {@code UnusedSegmentKiller}.
 */
public class UnusedSegmentKillerConfig
{
  @JsonProperty("bufferPeriod")
  private final Duration bufferPeriod;

  @JsonCreator
  public UnusedSegmentKillerConfig(
      @JsonProperty("bufferPeriod") @Nullable Duration bufferPeriod
  )
  {
    this.bufferPeriod = bufferPeriod;
  }

  /**
   * Period for which segments are retained even after being marked as unused.
   * If this returns null, segments are never killed by the {@code UnusedSegmentKiller}
   * but they might still be killed by the Coordinator.
   */
  @Nullable
  public Duration getBufferPeriod()
  {
    return bufferPeriod;
  }

  /**
   * @return true if {@link #getBufferPeriod()} returns a non-null value.
   */
  public boolean isEnabled()
  {
    return bufferPeriod != null;
  }
}
