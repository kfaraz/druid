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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.shaded.com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.guava.Comparators;

import javax.annotation.Nullable;
import java.util.Comparator;

/**
 * This policy searches segments for compaction from newest to oldest.
 */
public class NewestSegmentFirstPolicy extends BaseSegmentSearchPolicy
{
  @JsonCreator
  public NewestSegmentFirstPolicy(
      @JsonProperty("priorityDatasource") @Nullable String priorityDatasource
  )
  {
    super(priorityDatasource);
  }

  @Override
  protected Comparator<SegmentsToCompact> getSegmentComparator()
  {
    return (o1, o2) -> Comparators.intervalsByStartThenEnd()
                           .compare(o2.getUmbrellaInterval(), o1.getUmbrellaInterval());
  }
}
