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

import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 * TODO:
 * -[x] Handle all cases of cache vs metadata store
 * -[ ] Perform read writes to the cache only if it is READY
 * -[ ] Add APIs in cache to read/update
 * -[ ] Mark as used / unused should happen within TaskLockbox.giant()
 * -[ ] Wire up cache in IndexerSQLMetadataStorageCoordinator
 * -[ ] Poll pending segments too
 * -[ ] How to ensure that cache is not updated while some read is happening.
 * - We cannot acquire TaskLockbox.giant as that would significantly slow down read operations.
 * - I think read from cache would be fast anyway. We just need to ensure that we read a consistent state
 * - and I think that would be ensured by the lock inside the DatasourceSegmentCache.
 * -[ ] Add a common interface with 2 impls: SqlSegmentsMetadataQuery and the other powered by cache
 * -[ ] Wire up cache in OverlordCompactionScheduler
 * -[ ] Write unit tests
 * -[ ] Write integration tests
 * -[ ] Write a benchmark
 */
public interface SegmentsMetadataCache extends SegmentsMetadataReadOnlyCache
{
  void start();

  void stop();

  void commitSegments(Set<DataSegment> segments);

  void deleteSegments(Set<DataSegment> segments);

  // Methods for pending segments

  void commitPendingSegments(List<PendingSegmentRecord> pendingSegments);

  void deletePendingSegments(String dataSource);

  void deletePendingSegments(String dataSource, String taskAllocatorId);

  void deletePendingSegmentsCreatedIn(String dataSource, Interval createdInterval);
}
