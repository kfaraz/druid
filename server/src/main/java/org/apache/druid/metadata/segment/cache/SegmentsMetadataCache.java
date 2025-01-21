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

import org.apache.druid.metadata.segment.DatasourceSegmentMetadataReader;
import org.apache.druid.metadata.segment.DatasourceSegmentMetadataWriter;

/**
 * TODO:
 * -[x] Handle all cases of cache vs metadata store
 * -[x] Perform read writes to the cache only if it is READY
 * -[x] Add APIs in cache to read/update
 * -[ ] Figure out the best datastructure to cache pending segments
 * -[ ] Add transaction API to return timeline and/or timeline holders
 * -[ ] What about rollback strategy? If any command has failed, the transaction must be rolled back.
 * We would need to undo the changes done so far to the cache. So, a better thing to do would be just not commit anything.
 * -[ ] Mark as used / unused should happen within TaskLockbox.giant()
 * -[x] Wire up cache in IndexerSQLMetadataStorageCoordinator
 * -[x] Just using a handle doesn't ensure a transaction. Make sure the read + write
 * stuff happens in a transaction wherever applicable
 * -[ ] Poll and cache pending segments too
 * -[ ] How to ensure that cache is not updated while some read is happening.
 * - We cannot acquire TaskLockbox.giant as that would significantly slow down read operations.
 * - I think read from cache would be fast anyway. We just need to ensure that we read a consistent state
 * - and I think that would be ensured by the lock inside the DatasourceSegmentCache.
 * - BUT we want the lock to be held for the ENTIRE DURATION of the transaction,
 * not just while we are doing the actual read operation.
 * -[ ] Wire up cache in OverlordCompactionScheduler
 * -[ ] Write unit tests
 * -[ ] Write integration tests
 * -[ ] Write a benchmark
 */
public interface SegmentsMetadataCache
{
  void start();

  void stop();

  boolean isReady();

  DatasourceSegmentMetadataReader readerForDatasource(String dataSource);

  DatasourceSegmentMetadataWriter writerForDatasource(String dataSource);

}
