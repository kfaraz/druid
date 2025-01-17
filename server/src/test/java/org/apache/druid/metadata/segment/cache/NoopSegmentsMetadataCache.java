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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;

import java.util.Map;
import java.util.Set;

public class NoopSegmentsMetadataCache implements SegmentsMetadataCache
{
  @Override
  public void start()
  {

  }

  @Override
  public void stop()
  {

  }

  @Override
  public void addSegments(String dataSource, Set<DataSegmentPlus> segments)
  {

  }

  @Override
  public boolean isReady()
  {
    return false;
  }

  @Override
  public Set<String> findExistingSegmentIds(String dataSource, Set<DataSegment> segments)
  {
    return Set.of();
  }

  @Override
  public Map<String, SegmentTimeline> getDataSourceToUsedSegmentTimeline()
  {
    return Map.of();
  }
}
