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

package org.apache.druid.metadata.segment;

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionStatus;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Represents a single transaction involving read/write of segment metadata into
 * the metadata store. A transaction is associated with a single instance of a
 * {@link Handle} and is meant to be short-lived.
 */
public interface SqlSegmentsMetadataTransaction
{
  /**
   * @return The JDBI handle used in this transaction
   */
  Handle getHandle();

  /**
   * Marks this transaction to be rolled back.
   */
  void setRollbackOnly();

  /**
   * Returns the IDs of segments (out of the given set) which already exist in
   * the metadata store.
   */
  Set<String> findExistingSegmentIds(Set<DataSegment> segments);

  Set<SegmentId> findUsedSegmentIds(String dataSource, Interval interval);

  CloseableIterator<DataSegment> findUsedSegments(String dataSource, List<Interval> intervals);

  CloseableIterator<DataSegmentPlus> findUsedSegmentsPlus(String dataSource, List<Interval> intervals);

  DataSegment findSegment(String segmentId);

  DataSegment findUsedSegment(String segmentId);

  List<DataSegmentPlus> findSegments(String dataSource, Set<String> segmentIds);

  List<DataSegmentPlus> findSegmentsWithSchema(String dataSource, Set<String> segmentIds);

  CloseableIterator<DataSegment> findUnusedSegments(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  );

  /**
   * Inserts the given segments into the metadata store.
   */
  void insertSegments(Set<DataSegmentPlus> segments) throws Exception;

  void insertSegmentsWithMetadata(Set<DataSegmentPlus> segments) throws Exception;

  int markSegmentsUnused(String dataSource, Interval interval);
}
