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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.util.StringMapper;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An object that helps {@link SqlSegmentsMetadataManager} and {@link IndexerSQLMetadataStorageCoordinator} make
 * queries to the metadata store segments table. Each instance of this class is scoped to a single handle and is meant
 * to be short-lived.
 */
public class SqlSegmentsMetadataQuery
{
  private static final Logger log = new Logger(SqlSegmentsMetadataQuery.class);

  /**
   * Maximum number of intervals to consider for a batch.
   * This is similar to {@link IndexerSQLMetadataStorageCoordinator#MAX_NUM_SEGMENTS_TO_ANNOUNCE_AT_ONCE}, but imposed
   * on the intervals size.
   */
  private static final int MAX_INTERVALS_PER_BATCH = 100;

  private final Handle handle;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;

  private SqlSegmentsMetadataQuery(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    this.handle = handle;
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
  }

  /**
   * Create a query object. This instance is scoped to a single handle and is meant to be short-lived. It is okay
   * to use it for more than one query, though.
   */
  public static SqlSegmentsMetadataQuery forHandle(
      final Handle handle,
      final SQLMetadataConnector connector,
      final MetadataStorageTablesConfig dbTables,
      final ObjectMapper jsonMapper
  )
  {
    return new SqlSegmentsMetadataQuery(handle, connector, dbTables, jsonMapper);
  }

  /**
   * Retrieves segments for a given datasource that are marked used (i.e. published) in the metadata store, and that
   * *overlap* any interval in a particular collection of intervals. If the collection of intervals is empty, this
   * method will retrieve all used segments.
   *
   * You cannot assume that segments returned by this call are actually active. Because there is some delay between
   * new segment publishing and the marking-unused of older segments, it is possible that some segments returned
   * by this call are overshadowed by other segments. To check for this, use
   * {@link org.apache.druid.timeline.SegmentTimeline#forSegments(Iterable)}.
   *
   * This call does not return any information about realtime segments.
   *
   * @return a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegment> retrieveUsedSegments(
      final String dataSource,
      final Collection<Interval> intervals
  )
  {
    return retrieveUsedSegments(dataSource, intervals, null);
  }

  /**
   * Similar to {@link #retrieveUsedSegments}, but with an additional {@code versions} argument. When {@code versions}
   * is specified, all used segments in the specified {@code intervals} and {@code versions} are retrieved.
   */
  public CloseableIterator<DataSegment> retrieveUsedSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      final List<String> versions
  )
  {
    return retrieveSegments(
        dataSource,
        intervals,
        versions,
        IntervalMode.OVERLAPS,
        true,
        null,
        null,
        null,
        null
    );
  }

  public CloseableIterator<DataSegmentPlus> retrieveUsedSegmentsPlus(
      String dataSource,
      Collection<Interval> intervals
  )
  {
    return retrieveSegmentsPlus(
        dataSource,
        intervals, null, IntervalMode.OVERLAPS, true, null, null, null, null
    );
  }

  public Set<String> retrieveUnusedSegmentIdsForExactIntervalAndVersion(
      String dataSource,
      Interval interval,
      String version
  )
  {
    final String sql = StringUtils.format(
        "SELECT id FROM %1$s"
        + " WHERE used = :used"
        + " AND dataSource = :dataSource"
        + " AND version = :version"
        + " AND start = :start AND %2$send%2$s = :end",
        dbTables.getSegmentsTable(), connector.getQuoteString()
    );

    final Query<Map<String, Object>> query = handle
        .createQuery(sql)
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", false)
        .bind("dataSource", dataSource)
        .bind("version", version)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString());

    try (final ResultIterator<String> iterator = query.map(StringMapper.FIRST).iterator()) {
      return ImmutableSet.copyOf(iterator);
    }
  }

  /**
   * Retrieves segments for a given datasource that are marked unused and that are <b>fully contained by</b> any interval
   * in a particular collection of intervals. If the collection of intervals is empty, this method will retrieve all
   * unused segments.
   * <p>
   * This call does not return any information about realtime segments.
   * </p>
   *
   * @param dataSource    The name of the datasource
   * @param intervals     The intervals to search over
   * @param versions      An optional list of unused segment versions to retrieve in the given {@code intervals}.
   *                      If unspecified, all versions of unused segments in the {@code intervals} must be retrieved. If an
   *                      empty list is passed, no segments are retrieved.
   * @param limit         The limit of segments to return
   * @param lastSegmentId the last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or ASC, or < this segment
   *                      lexigraphically if sortOrder is DESC.
   * @param sortOrder     Specifies the order with which to return the matching segments by start time, end time.
   *                      A null value indicates that order does not matter.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code intervals}
   *                                     with {@code used_status_last_updated} no later than this time will be included in the
   *                                     iterator. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                     from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   *
   * @return a closeable iterator. You should close it when you are done.
   *
   */
  public CloseableIterator<DataSegment> retrieveUnusedSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveSegments(
        dataSource,
        intervals,
        versions,
        IntervalMode.CONTAINS,
        false,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime
    );
  }

  /**
   * Similar to {@link #retrieveUnusedSegments}, but also retrieves associated metadata for the segments for a given
   * datasource that are marked unused and that are <b>fully contained by</b> any interval in a particular collection of
   * intervals. If the collection of intervals is empty, this method will retrieve all unused segments.
   *
   * This call does not return any information about realtime segments.
   *
   * @param dataSource    The name of the datasource
   * @param intervals     The intervals to search over
   * @param limit         The limit of segments to return
   * @param lastSegmentId the last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or ASC, or < this segment
   *                      lexigraphically if sortOrder is DESC.
   * @param sortOrder     Specifies the order with which to return the matching segments by start time, end time.
   *                      A null value indicates that order does not matter.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code intervals}
   *                                   with {@code used_status_last_updated} no later than this time will be included in the
   *                                   iterator. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                   from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored

   * @return a closeable iterator. You should close it when you are done.
   */
  public CloseableIterator<DataSegmentPlus> retrieveUnusedSegmentsPlus(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveSegmentsPlus(
        dataSource,
        intervals,
        versions,
        IntervalMode.CONTAINS,
        false,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime
    );
  }

  /**
   * Retrieves IDs of used segments that belong to the datasource and overlap
   * the given interval.
   */
  public Set<SegmentId> retrieveUsedSegmentIds(
      final String dataSource,
      final Interval interval
  )
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("SELECT id FROM %s WHERE used = :used AND dataSource = :dataSource");

    // If the interval supports comparing as a string, bake it into the SQL
    final boolean compareAsString = Intervals.canCompareEndpointsAsStrings(interval);
    if (compareAsString) {
      sb.append(
          getConditionForIntervalsAndMatchMode(
              Collections.singletonList(interval),
              IntervalMode.OVERLAPS,
              connector.getQuoteString()
          )
      );
    }

    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(sb.toString(), dbTables.getSegmentsTable()))
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", true)
        .bind("dataSource", dataSource);

    if (compareAsString) {
      bindIntervalsToQuery(sql, Collections.singletonList(interval));
    }

    final Set<SegmentId> segmentIds = new HashSet<>();
    try (final ResultIterator<String> iterator = sql.map(StringMapper.FIRST).iterator()) {
      while (iterator.hasNext()) {
        final String id = iterator.next();
        final SegmentId segmentId = SegmentId.tryParse(dataSource, id);
        if (segmentId == null) {
          throw DruidException.defensive(
              "Failed to parse SegmentId for id[%s] and dataSource[%s].",
              id, dataSource
          );
        }
        if (IntervalMode.OVERLAPS.apply(interval, segmentId.getInterval())) {
          segmentIds.add(segmentId);
        }
      }
    }
    return segmentIds;

  }

  public List<DataSegmentPlus> retrieveSegmentsById(
      String datasource,
      Set<String> segmentIds
  )
  {
    final List<List<String>> partitionedSegmentIds
        = Lists.partition(new ArrayList<>(segmentIds), 100);

    final List<DataSegmentPlus> fetchedSegments = new ArrayList<>(segmentIds.size());
    for (List<String> partition : partitionedSegmentIds) {
      fetchedSegments.addAll(retrieveSegmentBatchById(datasource, partition, false));
    }
    return fetchedSegments;
  }

  public List<DataSegmentPlus> retrieveSegmentsWithSchemaById(
      String datasource,
      Set<String> segmentIds
  )
  {
    final List<List<String>> partitionedSegmentIds
        = Lists.partition(new ArrayList<>(segmentIds), 100);

    final List<DataSegmentPlus> fetchedSegments = new ArrayList<>(segmentIds.size());
    for (List<String> partition : partitionedSegmentIds) {
      fetchedSegments.addAll(retrieveSegmentBatchById(datasource, partition, true));
    }
    return fetchedSegments;
  }

  private List<DataSegmentPlus> retrieveSegmentBatchById(
      String datasource,
      List<String> segmentIds,
      boolean includeSchemaInfo
  )
  {
    if (segmentIds.isEmpty()) {
      return Collections.emptyList();
    }

    ResultIterator<DataSegmentPlus> resultIterator;
    if (includeSchemaInfo) {
      final Query<Map<String, Object>> query = handle.createQuery(
          StringUtils.format(
              "SELECT payload, used, schema_fingerprint, num_rows, upgraded_from_segment_id FROM %s WHERE dataSource = :dataSource %s",
              dbTables.getSegmentsTable(), getParameterizedInConditionForColumn("id", segmentIds)
          )
      );

      bindColumnValuesToQueryWithInCondition("id", segmentIds, query);

      resultIterator = query
          .bind("dataSource", datasource)
          .setFetchSize(connector.getStreamingFetchSize())
          .map(
              (index, r, ctx) -> {
                String schemaFingerprint = (String) r.getObject(3);
                Long numRows = (Long) r.getObject(4);
                return new DataSegmentPlus(
                    JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class),
                    null,
                    null,
                    r.getBoolean(2),
                    schemaFingerprint,
                    numRows,
                    r.getString(5)
                );
              }
          )
          .iterator();
    } else {
      final Query<Map<String, Object>> query = handle.createQuery(
          StringUtils.format(
              "SELECT payload, used, upgraded_from_segment_id FROM %s WHERE dataSource = :dataSource %s",
              dbTables.getSegmentsTable(), getParameterizedInConditionForColumn("id", segmentIds)
          )
      );

      bindColumnValuesToQueryWithInCondition("id", segmentIds, query);

      resultIterator = query
          .bind("dataSource", datasource)
          .setFetchSize(connector.getStreamingFetchSize())
          .map(
              (index, r, ctx) -> new DataSegmentPlus(
                  JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class),
                  null,
                  null,
                  r.getBoolean(2),
                  null,
                  null,
                  r.getString(3)
              )
          )
          .iterator();
    }

    return Lists.newArrayList(resultIterator);
  }

  /**
   * Marks the provided segments as either used or unused.
   *
   * For better performance, please try to
   * 1) ensure that the caller passes only used segments to this method when marking them as unused.
   * 2) Similarly, please try to call this method only on unused segments when marking segments as used with this method.
   *
   * @return the number of segments actually modified.
   */
  public int markSegments(final Collection<SegmentId> segmentIds, final boolean used)
  {
    final String dataSource;

    if (segmentIds.isEmpty()) {
      return 0;
    } else {
      dataSource = segmentIds.iterator().next().getDataSource();
      if (segmentIds.stream().anyMatch(segment -> !dataSource.equals(segment.getDataSource()))) {
        throw new IAE("Segments to drop must all be part of the same datasource");
      }
    }

    final PreparedBatch batch =
        handle.prepareBatch(
            StringUtils.format(
                "UPDATE %s SET used = ?, used_status_last_updated = ? WHERE datasource = ? AND id = ?",
                dbTables.getSegmentsTable()
            )
        );

    for (SegmentId segmentId : segmentIds) {
      batch.add(used, DateTimes.nowUtc().toString(), dataSource, segmentId.toString());
    }

    final int[] segmentChanges = batch.execute();
    return computeNumChangedSegments(
        segmentIds.stream().map(SegmentId::toString).collect(Collectors.toList()),
        segmentChanges
    );
  }

  /**
   * Marks all used segments that are <b>fully contained by</b> a particular interval as unused.
   *
   * @return Number of segments updated.
   */
  public int markSegmentsUnused(final String dataSource, final Interval interval, final DateTime updateTime)
  {
    return markSegmentsUnused(dataSource, interval, null, updateTime);
  }

  /**
   * Marks all used segments that are <b>fully contained by</b> a particular interval filtered by an optional list of versions
   * as unused.
   *
   * @return Number of segments updated.
   */
  public int markSegmentsUnused(
      final String dataSource,
      final Interval interval,
      @Nullable final List<String> versions,
      final DateTime updateTime
  )
  {
    if (versions != null && versions.isEmpty()) {
      return 0;
    }

    if (Intervals.isEternity(interval)) {
      final StringBuilder sb = new StringBuilder();
      sb.append(
          StringUtils.format(
              "UPDATE %s SET used=:used, used_status_last_updated = :used_status_last_updated "
              + "WHERE dataSource = :dataSource AND used = true",
              dbTables.getSegmentsTable()
          )
      );

      if (versions != null) {
        sb.append(getParameterizedInConditionForColumn("version", versions));
      }

      final Update stmt = handle
          .createStatement(sb.toString())
          .bind("dataSource", dataSource)
          .bind("used", false)
          .bind("used_status_last_updated", updateTime.toString());

      if (versions != null) {
        bindColumnValuesToQueryWithInCondition("version", versions, stmt);
      }

      return stmt.execute();
    } else if (Intervals.canCompareEndpointsAsStrings(interval)
               && interval.getStart().getYear() == interval.getEnd().getYear()) {
      // Safe to write a WHERE clause with this interval. Note that it is unsafe if the years are different, because
      // that means extra characters can sneak in. (Consider a query interval like "2000-01-01/2001-01-01" and a
      // segment interval like "20001/20002".)
      final StringBuilder sb = new StringBuilder();
      sb.append(
          StringUtils.format(
              "UPDATE %s SET used=:used, used_status_last_updated = :used_status_last_updated "
              + "WHERE dataSource = :dataSource AND used = true AND %s",
              dbTables.getSegmentsTable(),
              IntervalMode.CONTAINS.makeSqlCondition(connector.getQuoteString(), ":start", ":end")
          )
      );

      if (versions != null) {
        sb.append(getParameterizedInConditionForColumn("version", versions));
      }

      final Update stmt = handle
          .createStatement(sb.toString())
          .bind("dataSource", dataSource)
          .bind("used", false)
          .bind("start", interval.getStart().toString())
          .bind("end", interval.getEnd().toString())
          .bind("used_status_last_updated", updateTime.toString());

      if (versions != null) {
        bindColumnValuesToQueryWithInCondition("version", versions, stmt);
      }
      return stmt.execute();
    } else {
      // Retrieve, then drop, since we can't write a WHERE clause directly.
      final List<SegmentId> segments = ImmutableList.copyOf(
          Iterators.transform(
              retrieveSegments(
                  dataSource,
                  Collections.singletonList(interval),
                  versions,
                  IntervalMode.CONTAINS,
                  true,
                  null,
                  null,
                  null,
                  null
              ),
              DataSegment::getId
          )
      );
      return markSegments(segments, false);
    }
  }

  /**
   * Retrieve the used segment for a given id if it exists in the metadata store and null otherwise
   */
  public DataSegment retrieveUsedSegmentForId(String id)
  {
    final String query = "SELECT payload FROM %s WHERE used = true AND id = :id";

    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(query, dbTables.getSegmentsTable()))
        .bind("id", id);

    final ResultIterator<DataSegment> resultIterator =
        sql.map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
           .iterator();

    if (resultIterator.hasNext()) {
      return resultIterator.next();
    }

    return null;
  }

  /**
   * Retrieve the segment for a given id if it exists in the metadata store and null otherwise
   */
  public DataSegment retrieveSegmentForId(String id)
  {
    final String query = "SELECT payload FROM %s WHERE id = :id";

    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(query, dbTables.getSegmentsTable()))
        .bind("id", id);

    final ResultIterator<DataSegment> resultIterator =
        sql.map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
           .iterator();

    if (resultIterator.hasNext()) {
      return resultIterator.next();
    }

    return null;
  }

  public List<SegmentIdWithShardSpec> retrievePendingSegmentIds(
      final String dataSource,
      final String sequenceName,
      final String sequencePreviousId
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "sequence_prev_id = :sequence_prev_id",
        dbTables.getPendingSegmentsTable()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("sequence_name", sequenceName)
        .bind("sequence_prev_id", sequencePreviousId)
        .map(
            (index, r, ctx) -> JacksonUtils.readValue(
                jsonMapper,
                r.getBytes("payload"),
                SegmentIdWithShardSpec.class
            )
        )
        .list();
  }

  public List<SegmentIdWithShardSpec> retrievePendingSegmentIdsWithExactInterval(
      final String dataSource,
      final String sequenceName,
      final Interval interval
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload FROM %s WHERE "
        + "dataSource = :dataSource AND "
        + "sequence_name = :sequence_name AND "
        + "start = :start AND "
        + "%2$send%2$s = :end",
        dbTables.getPendingSegmentsTable(),
        connector.getQuoteString()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("sequence_name", sequenceName)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .map(
            (index, r, ctx) -> JacksonUtils.readValue(
                jsonMapper,
                r.getBytes("payload"),
                SegmentIdWithShardSpec.class
            )
        )
        .list();
  }

  public List<PendingSegmentRecord> retrievePendingSegmentsWithExactInterval(
      final String dataSource,
      final Interval interval
  )
  {
    final String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id, task_allocator_id, upgraded_from_segment_id"
        + " FROM %1$s WHERE"
        + " dataSource = :dataSource"
        + " AND start = :start"
        + " AND %2$send%2$s = :end",
        dbTables.getPendingSegmentsTable(), connector.getQuoteString()
    );
    return handle
        .createQuery(sql)
        .bind("dataSource", dataSource)
        .bind("start", interval.getStart().toString())
        .bind("end", interval.getEnd().toString())
        .map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
        .list();
  }

  /**
   * Fetches all the pending segments, whose interval overlaps with the given
   * search interval, from the metadata store.
   */
  public List<PendingSegmentRecord> retrievePendingSegmentsOverlappingInterval(
      final String dataSource,
      final Interval interval
  )
  {
    final boolean compareIntervalEndpointsAsStrings = Intervals.canCompareEndpointsAsStrings(interval);

    String sql = StringUtils.format(
        "SELECT payload, sequence_name, sequence_prev_id, task_allocator_id, upgraded_from_segment_id"
        + " FROM %1$s"
        + " WHERE dataSource = :dataSource",
        dbTables.getPendingSegmentsTable()
    );
    if (compareIntervalEndpointsAsStrings) {
      sql += " AND start < :end"
             + StringUtils.format(" AND %1$send%1$s > :start", connector.getQuoteString());
    }

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource);
    if (compareIntervalEndpointsAsStrings) {
      query = query.bind("start", interval.getStart().toString())
                   .bind("end", interval.getEnd().toString());
    }

    final ResultIterator<PendingSegmentRecord> pendingSegmentIterator =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();
    final ImmutableList.Builder<PendingSegmentRecord> pendingSegments = ImmutableList.builder();
    while (pendingSegmentIterator.hasNext()) {
      final PendingSegmentRecord pendingSegment = pendingSegmentIterator.next();
      if (compareIntervalEndpointsAsStrings || pendingSegment.getId().getInterval().overlaps(interval)) {
        pendingSegments.add(pendingSegment);
      }
    }
    pendingSegmentIterator.close();
    return pendingSegments.build();
  }

  public List<PendingSegmentRecord> retrievePendingSegmentsForTaskAllocatorId(
      final String dataSource,
      final String taskAllocatorId
  )
  {
    String sql = "SELECT payload, sequence_name, sequence_prev_id, task_allocator_id, upgraded_from_segment_id"
                 + " FROM " + dbTables.getPendingSegmentsTable()
                 + " WHERE dataSource = :dataSource AND task_allocator_id = :task_allocator_id";

    Query<Map<String, Object>> query = handle.createQuery(sql)
                                             .bind("dataSource", dataSource)
                                             .bind("task_allocator_id", taskAllocatorId);

    final ResultIterator<PendingSegmentRecord> pendingSegmentRecords =
        query.map((index, r, ctx) -> PendingSegmentRecord.fromResultSet(r, jsonMapper))
             .iterator();

    final List<PendingSegmentRecord> pendingSegments = new ArrayList<>();
    while (pendingSegmentRecords.hasNext()) {
      pendingSegments.add(pendingSegmentRecords.next());
    }

    pendingSegmentRecords.close();

    return pendingSegments;
  }

  /**
   * Get the condition for the interval and match mode.
   * @param intervals - intervals to fetch the segments for
   * @param matchMode - Interval match mode - overlaps or contains
   * @param quoteString - the connector-specific quote string
   */
  public static String getConditionForIntervalsAndMatchMode(
      final Collection<Interval> intervals,
      final IntervalMode matchMode,
      final String quoteString
  )
  {
    if (intervals.isEmpty()) {
      return "";
    }

    final StringBuilder sb = new StringBuilder();

    sb.append(" AND (");
    for (int i = 0; i < intervals.size(); i++) {
      sb.append(
          matchMode.makeSqlCondition(
              quoteString,
              StringUtils.format(":start%d", i),
              StringUtils.format(":end%d", i)
          )
      );

      // Add a special check for a segment which have one end at eternity and the other at some finite value. Since
      // we are using string comparison, a segment with this start or end will not be returned otherwise.
      if (matchMode.equals(IntervalMode.OVERLAPS)) {
        sb.append(StringUtils.format(
            " OR (start = '%s' AND \"end\" != '%s' AND \"end\" > :start%d)",
            Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i
        ));
        sb.append(StringUtils.format(
            " OR (start != '%s' AND \"end\" = '%s' AND start < :end%d)",
            Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd(), i
        ));
      }

      if (i != intervals.size() - 1) {
        sb.append(" OR ");
      }
    }

    // Add a special check for a single segment with eternity. Since we are using string comparison, a segment with
    // this start and end will not be returned otherwise.
    // Known Issue: https://github.com/apache/druid/issues/12860
    if (matchMode.equals(IntervalMode.OVERLAPS)) {
      sb.append(StringUtils.format(
          " OR (start = '%s' AND \"end\" = '%s')", Intervals.ETERNITY.getStart(), Intervals.ETERNITY.getEnd()
      ));
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Bind the supplied {@code intervals} to {@code query}.
   * @see #getConditionForIntervalsAndMatchMode(Collection, IntervalMode, String)
   */
  public static void bindIntervalsToQuery(final Query<Map<String, Object>> query, final Collection<Interval> intervals)
  {
    if (intervals.isEmpty()) {
      return;
    }

    final Iterator<Interval> iterator = intervals.iterator();
    for (int i = 0; iterator.hasNext(); i++) {
      Interval interval = iterator.next();
      query.bind(StringUtils.format("start%d", i), interval.getStart().toString())
           .bind(StringUtils.format("end%d", i), interval.getEnd().toString());
    }
  }

  private CloseableIterator<DataSegment> retrieveSegments(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    if (versions != null && versions.isEmpty()) {
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    if (intervals.isEmpty() || intervals.size() <= MAX_INTERVALS_PER_BATCH) {
      return CloseableIterators.withEmptyBaggage(
          retrieveSegmentsInIntervalsBatch(dataSource, intervals, versions, matchMode, used, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)
      );
    } else {
      final List<List<Interval>> intervalsLists = Lists.partition(new ArrayList<>(intervals), MAX_INTERVALS_PER_BATCH);
      final List<Iterator<DataSegment>> resultingIterators = new ArrayList<>();
      Integer limitPerBatch = limit;

      for (final List<Interval> intervalList : intervalsLists) {
        final UnmodifiableIterator<DataSegment> iterator = retrieveSegmentsInIntervalsBatch(
            dataSource,
            intervalList,
            versions,
            matchMode,
            used,
            limitPerBatch,
            lastSegmentId,
            sortOrder,
            maxUsedStatusLastUpdatedTime
        );
        if (limitPerBatch != null) {
          // If limit is provided, we need to shrink the limit for subsequent batches or circuit break if
          // we have reached what was requested for.
          final List<DataSegment> dataSegments = ImmutableList.copyOf(iterator);
          resultingIterators.add(dataSegments.iterator());
          if (dataSegments.size() >= limitPerBatch) {
            break;
          }
          limitPerBatch -= dataSegments.size();
        } else {
          resultingIterators.add(iterator);
        }
      }
      return CloseableIterators.withEmptyBaggage(Iterators.concat(resultingIterators.iterator()));
    }
  }

  private CloseableIterator<DataSegmentPlus> retrieveSegmentsPlus(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    if (intervals.isEmpty() || intervals.size() <= MAX_INTERVALS_PER_BATCH) {
      return CloseableIterators.withEmptyBaggage(
          retrieveSegmentsPlusInIntervalsBatch(dataSource, intervals, versions, matchMode, used, limit, lastSegmentId, sortOrder, maxUsedStatusLastUpdatedTime)
      );
    } else {
      final List<List<Interval>> intervalsLists = Lists.partition(new ArrayList<>(intervals), MAX_INTERVALS_PER_BATCH);
      final List<Iterator<DataSegmentPlus>> resultingIterators = new ArrayList<>();
      Integer limitPerBatch = limit;

      for (final List<Interval> intervalList : intervalsLists) {
        final UnmodifiableIterator<DataSegmentPlus> iterator = retrieveSegmentsPlusInIntervalsBatch(
            dataSource,
            intervalList,
            versions,
            matchMode,
            used,
            limitPerBatch,
            lastSegmentId,
            sortOrder,
            maxUsedStatusLastUpdatedTime
        );
        if (limitPerBatch != null) {
          // If limit is provided, we need to shrink the limit for subsequent batches or circuit break if
          // we have reached what was requested for.
          final List<DataSegmentPlus> dataSegments = ImmutableList.copyOf(iterator);
          resultingIterators.add(dataSegments.iterator());
          if (dataSegments.size() >= limitPerBatch) {
            break;
          }
          limitPerBatch -= dataSegments.size();
        } else {
          resultingIterators.add(iterator);
        }
      }
      return CloseableIterators.withEmptyBaggage(Iterators.concat(resultingIterators.iterator()));
    }
  }

  private UnmodifiableIterator<DataSegment> retrieveSegmentsInIntervalsBatch(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {
    final Query<Map<String, Object>> sql = buildSegmentsTableQuery(
        dataSource,
        intervals,
        versions,
        matchMode,
        used,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime,
        false
    );

    final ResultIterator<DataSegment> resultIterator = getDataSegmentResultIterator(sql);

    return filterDataSegmentIteratorByInterval(resultIterator, intervals, matchMode);
  }

  private UnmodifiableIterator<DataSegmentPlus> retrieveSegmentsPlusInIntervalsBatch(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime
  )
  {

    final Query<Map<String, Object>> sql = buildSegmentsTableQuery(
        dataSource,
        intervals,
        versions,
        matchMode,
        used,
        limit,
        lastSegmentId,
        sortOrder,
        maxUsedStatusLastUpdatedTime,
        true
    );

    final ResultIterator<DataSegmentPlus> resultIterator = getDataSegmentPlusResultIterator(sql);

    return filterDataSegmentPlusIteratorByInterval(resultIterator, intervals, matchMode);
  }

  private Query<Map<String, Object>> buildSegmentsTableQuery(
      final String dataSource,
      final Collection<Interval> intervals,
      @Nullable final List<String> versions,
      final IntervalMode matchMode,
      final boolean used,
      @Nullable final Integer limit,
      @Nullable final String lastSegmentId,
      @Nullable final SortOrder sortOrder,
      @Nullable final DateTime maxUsedStatusLastUpdatedTime,
      final boolean includeExtraInfo
  )
  {
    // Check if the intervals all support comparing as strings. If so, bake them into the SQL.
    final boolean compareAsString = intervals.stream().allMatch(Intervals::canCompareEndpointsAsStrings);
    final StringBuilder sb = new StringBuilder();
    if (includeExtraInfo) {
      sb.append("SELECT payload, created_date, used_status_last_updated FROM %s WHERE used = :used AND dataSource = :dataSource");
    } else {
      sb.append("SELECT payload FROM %s WHERE used = :used AND dataSource = :dataSource");
    }

    if (compareAsString) {
      sb.append(getConditionForIntervalsAndMatchMode(intervals, matchMode, connector.getQuoteString()));
    }

    if (versions != null) {
      sb.append(getParameterizedInConditionForColumn("version", versions));
    }

    // Add the used_status_last_updated time filter only for unused segments when maxUsedStatusLastUpdatedTime is non-null.
    final boolean addMaxUsedLastUpdatedTimeFilter = !used && maxUsedStatusLastUpdatedTime != null;
    if (addMaxUsedLastUpdatedTimeFilter) {
      sb.append(" AND (used_status_last_updated IS NOT NULL AND used_status_last_updated <= :used_status_last_updated)");
    }

    if (lastSegmentId != null) {
      sb.append(
          StringUtils.format(
              " AND id %s :id",
              (sortOrder == null || sortOrder == SortOrder.ASC)
                  ? ">"
                  : "<"
          )
      );
    }

    if (sortOrder != null) {
      sb.append(StringUtils.format(" ORDER BY id %2$s, start %2$s, %1$send%1$s %2$s",
          connector.getQuoteString(),
          sortOrder.toString()));
    }
    final Query<Map<String, Object>> sql = handle
        .createQuery(StringUtils.format(
            sb.toString(),
            dbTables.getSegmentsTable()
        ))
        .setFetchSize(connector.getStreamingFetchSize())
        .bind("used", used)
        .bind("dataSource", dataSource);

    if (addMaxUsedLastUpdatedTimeFilter) {
      sql.bind("used_status_last_updated", maxUsedStatusLastUpdatedTime.toString());
    }

    if (lastSegmentId != null) {
      sql.bind("id", lastSegmentId);
    }

    if (limit != null) {
      sql.setMaxRows(limit);
    }

    if (compareAsString) {
      bindIntervalsToQuery(sql, intervals);
    }

    if (versions != null) {
      bindColumnValuesToQueryWithInCondition("version", versions, sql);
    }

    return sql;
  }

  private ResultIterator<DataSegment> getDataSegmentResultIterator(Query<Map<String, Object>> sql)
  {
    return sql.map((index, r, ctx) -> JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class))
        .iterator();
  }

  private ResultIterator<DataSegmentPlus> getDataSegmentPlusResultIterator(Query<Map<String, Object>> sql)
  {
    return sql.map((index, r, ctx) -> new DataSegmentPlus(
            JacksonUtils.readValue(jsonMapper, r.getBytes(1), DataSegment.class),
            DateTimes.of(r.getString(2)),
            DateTimes.of(r.getString(3)),
            null,
            null,
            null,
            null
        ))
        .iterator();
  }

  private UnmodifiableIterator<DataSegment> filterDataSegmentIteratorByInterval(
      ResultIterator<DataSegment> resultIterator,
      final Collection<Interval> intervals,
      final IntervalMode matchMode
  )
  {
    return Iterators.filter(
        resultIterator,
        dataSegment -> {
          if (intervals.isEmpty()) {
            return true;
          } else {
            // Must re-check that the interval matches, even if comparing as string, because the *segment interval*
            // might not be string-comparable. (Consider a query interval like "2000-01-01/3000-01-01" and a
            // segment interval like "20010/20011".)
            for (Interval interval : intervals) {
              if (matchMode.apply(interval, dataSegment.getInterval())) {
                return true;
              }
            }

            return false;
          }
        }
    );
  }

  private UnmodifiableIterator<DataSegmentPlus> filterDataSegmentPlusIteratorByInterval(
      ResultIterator<DataSegmentPlus> resultIterator,
      final Collection<Interval> intervals,
      final IntervalMode matchMode
  )
  {
    return Iterators.filter(
        resultIterator,
        dataSegment -> {
          if (intervals.isEmpty()) {
            return true;
          } else {
            // Must re-check that the interval matches, even if comparing as string, because the *segment interval*
            // might not be string-comparable. (Consider a query interval like "2000-01-01/3000-01-01" and a
            // segment interval like "20010/20011".)
            for (Interval interval : intervals) {
              if (matchMode.apply(interval, dataSegment.getDataSegment().getInterval())) {
                return true;
              }
            }

            return false;
          }
        }
    );
  }

  private static int computeNumChangedSegments(List<String> segmentIds, int[] segmentChanges)
  {
    int numChangedSegments = 0;
    for (int i = 0; i < segmentChanges.length; i++) {
      int numUpdatedRows = segmentChanges[i];
      if (numUpdatedRows < 0) {
        log.error(
            "ASSERTION_ERROR: Negative number of rows updated for segment id [%s]: %d",
            segmentIds.get(i),
            numUpdatedRows
        );
      } else if (numUpdatedRows > 1) {
        log.error(
            "More than one row updated for segment id [%s]: %d, "
            + "there may be more than one row for the segment id in the database",
            segmentIds.get(i),
            numUpdatedRows
        );
      }
      if (numUpdatedRows > 0) {
        numChangedSegments += 1;
      }
    }
    return numChangedSegments;
  }

  /**
   * @return a parameterized {@code IN} clause for the specified {@code columnName}. The column values need to be bound
   * to a query by calling {@link #bindColumnValuesToQueryWithInCondition(String, List, SQLStatement)}.
   *
   * @implNote JDBI 3.x has better support for binding {@code IN} clauses directly.
   */
  public static String getParameterizedInConditionForColumn(final String columnName, final List<String> values)
  {
    if (values == null) {
      return "";
    }

    final StringBuilder sb = new StringBuilder();

    sb.append(StringUtils.format(" AND %s IN (", columnName));
    for (int i = 0; i < values.size(); i++) {
      sb.append(StringUtils.format(":%s%d", columnName, i));
      if (i != values.size() - 1) {
        sb.append(",");
      }
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Binds the provided list of {@code values} to the specified {@code columnName} in the given SQL {@code query} that
   * contains an {@code IN} clause.
   *
   * @see #getParameterizedInConditionForColumn(String, List)
   */
  public static void bindColumnValuesToQueryWithInCondition(
      final String columnName,
      final List<String> values,
      final SQLStatement<?> query
  )
  {
    if (values == null) {
      return;
    }

    for (int i = 0; i < values.size(); i++) {
      query.bind(StringUtils.format("%s%d", columnName, i), values.get(i));
    }
  }

  enum IntervalMode
  {
    CONTAINS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        // 2 range conditions are used on different columns, but not all SQL databases properly optimize it.
        // Some databases can only use an index on one of the columns. An additional condition provides
        // explicit knowledge that 'start' cannot be greater than 'end'.
        return StringUtils.format(
            "(start >= %2$s and start <= %3$s and %1$send%1$s <= %3$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.contains(b);
      }
    },
    OVERLAPS {
      @Override
      public String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder)
      {
        return StringUtils.format(
            "(start < %3$s AND %1$send%1$s > %2$s)",
            quoteString,
            startPlaceholder,
            endPlaceholder
        );
      }

      @Override
      public boolean apply(Interval a, Interval b)
      {
        return a.overlaps(b);
      }
    };

    public abstract String makeSqlCondition(String quoteString, String startPlaceholder, String endPlaceholder);

    public abstract boolean apply(Interval a, Interval b);
  }
}
