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

import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * Represents a single transaction involving read of segment metadata into
 * the metadata store. A transaction is associated with a single instance of a
 * {@link Handle} and is meant to be short-lived.
 */
public interface SegmentMetadataReadTransaction
    extends DatasourceSegmentMetadataReader, Closeable
{
  /**
   * @return The JDBI handle used in this transaction
   */
  Handle getHandle();

  /**
   * Completes the transaction by either committing it or rolling it back.
   */
  @Override
  void close();

  // Methods that can be served only by the metadata store since the required info is not cached

  /**
   * Retrieves the segment for the given segment ID.
   *
   * @return null if no such segment exists in the metadata store.
   */
  @Nullable
  DataSegment findSegment(SegmentId segmentId);

  /**
   * Retrieves segments for the given segment IDs.
   */
  List<DataSegmentPlus> findSegments(
      Set<SegmentId> segmentIds
  );

  /**
   * Retrieves segments with additional metadata info such as number of rows and
   * schema fingerprint for the given segment IDs.
   */
  List<DataSegmentPlus> findSegmentsWithSchema(
      Set<SegmentId> segmentIds
  );

  /**
   * Retrieves unused segments that are fully contained within the given interval.
   *
   * @param interval       Returned segments must be fully contained within this
   *                       interval
   * @param versions       Optional list of segment versions. If passed as null,
   *                       all segment versions are eligible.
   * @param limit          Maximum number of segments to return. If passed as null,
   *                       all segments are returned.
   * @param maxUpdatedTime Returned segments must have a {@code used_status_last_updated}
   *                       which is either null or earlier than this value.
   */
  List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUpdatedTime
  );

  /**
   * Retrieves unused segments that exactly match the given interval.
   *
   * @param interval       Returned segments must exactly match this interval.
   * @param maxUpdatedTime Returned segments must have a {@code used_status_last_updated}
   *                       which is either null or earlier than this value.
   * @param limit          Maximum number of segments to return
   */
  List<DataSegment> findUnusedSegmentsWithExactInterval(
      Interval interval,
      DateTime maxUpdatedTime,
      int limit
  );

  /**
   * Retrieves intervals that contain any unused segment. There is no guarantee
   * on the order of intervals in the list or on whether the limited list contains
   * the earliest or latest intervals present in the datasource.
   *
   * @return Unsorted list of unused segment intervals containing upto {@code limit} entries.
   */
  List<Interval> findUnusedSegmentIntervals(int limit);

  @FunctionalInterface
  interface Callback<T>
  {
    T inTransaction(SegmentMetadataReadTransaction transaction) throws Exception;
  }

}
