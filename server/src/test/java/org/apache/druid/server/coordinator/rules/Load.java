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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.java.util.common.Intervals;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for a {@link LoadRule}.
 */
public class Load
{
  private final Map<String, Integer> tieredReplicants = new HashMap<>();

  public static Load on(String tier, int numReplicas)
  {
    Load load = new Load();
    load.tieredReplicants.put(tier, numReplicas);
    return load;
  }

  public Load andOn(String tier, int numReplicas)
  {
    tieredReplicants.put(tier, numReplicas);
    return this;
  }

  public LoadRule forever()
  {
    return new ForeverLoadRule(tieredReplicants, null);
  }

  public LoadRule forInterval(String interval)
  {
    return new IntervalLoadRule(Intervals.of(interval), tieredReplicants, null);
  }
}
