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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.TransactionStatus;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataTransactionImpl implements SqlSegmentsMetadataTransaction
{
  private static final int MAX_SEGMENTS_PER_BATCH = 100;

  private final Handle handle;
  private final TransactionStatus transactionStatus;
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig dbTables;
  private final ObjectMapper jsonMapper;

  private final SqlSegmentsMetadataQuery query;

  public SqlSegmentsMetadataTransactionImpl(
      Handle handle,
      TransactionStatus transactionStatus,
      SQLMetadataConnector connector,
      MetadataStorageTablesConfig dbTables,
      ObjectMapper jsonMapper
  )
  {
    this.handle = handle;
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.query = SqlSegmentsMetadataQuery.forHandle(handle, connector, dbTables, jsonMapper);
  }

  @Override
  public Handle getHandle()
  {
    return handle;
  }

  @Override
  public void setRollbackOnly()
  {
    transactionStatus.setRollbackOnly();
  }

  @Override
  public Set<String> findExistingSegmentIds(Set<DataSegment> segments)
  {
    final Set<String> existingSegmentIds = new HashSet<>();
    final String sql = "SELECT id FROM %s WHERE id in (%s)";

    List<List<DataSegment>> partitions = Lists.partition(new ArrayList<>(segments), MAX_SEGMENTS_PER_BATCH);
    for (List<DataSegment> segmentList : partitions) {
      String segmentIds = segmentList.stream().map(
          segment -> "'" + StringUtils.escapeSql(segment.getId().toString()) + "'"
      ).collect(Collectors.joining(","));

      existingSegmentIds.addAll(
          handle.createQuery(StringUtils.format(sql, dbTables.getSegmentsTable(), segmentIds))
                .mapTo(String.class)
                .list()
      );
    }

    return existingSegmentIds;
  }

  @Override
  public Set<SegmentId> findUsedSegmentIds(String dataSource, Interval interval)
  {
    return query.retrieveUsedSegmentIds(dataSource, interval);
  }

  @Override
  public CloseableIterator<DataSegment> findUsedSegments(String dataSource, List<Interval> intervals)
  {
    return query.retrieveUsedSegments(dataSource, intervals);
  }

  @Override
  public CloseableIterator<DataSegmentPlus> findUsedSegmentsPlus(String dataSource, List<Interval> intervals)
  {
    return query.retrieveUsedSegmentsPlus(dataSource, intervals);
  }

  @Override
  public DataSegment findSegment(String segmentId)
  {
    return query.retrieveSegmentForId(segmentId);
  }

  @Override
  public DataSegment findUsedSegment(String segmentId)
  {
    return query.retrieveUsedSegmentForId(segmentId);
  }

  @Override
  public List<DataSegmentPlus> findSegments(String dataSource, Set<String> segmentIds)
  {
    return query.retrieveSegmentsById(dataSource, segmentIds);
  }

  @Override
  public List<DataSegmentPlus> findSegmentsWithSchema(String dataSource, Set<String> segmentIds)
  {
    return query.retrieveSegmentsWithSchemaById(dataSource, segmentIds);
  }

  @Override
  public CloseableIterator<DataSegment> findUnusedSegments(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return query.retrieveUnusedSegments(
        dataSource,
        List.of(interval),
        versions,
        limit,
        null,
        null,
        maxUsedStatusLastUpdatedTime
    );
  }

  @Override
  public void insertSegments(Set<DataSegmentPlus> segments) throws Exception
  {
    insertSegmentsInBatches(
        segments,
        "INSERT INTO %1$s "
        + "(id, dataSource, created_date, start, %2$send%2$s, partitioned, "
        + "version, used, payload, used_status_last_updated, upgraded_from_segment_id) "
        + "VALUES "
        + "(:id, :dataSource, :created_date, :start, :end, :partitioned, "
        + ":version, :used, :payload, :used_status_last_updated, :upgraded_from_segment_id)"
    );
  }

  @Override
  public void insertSegmentsWithMetadata(Set<DataSegmentPlus> segments) throws Exception
  {
    insertSegmentsInBatches(
        segments,
        "INSERT INTO %1$s "
        + "(id, dataSource, created_date, start, %2$send%2$s, partitioned, "
        + "version, used, payload, used_status_last_updated, upgraded_from_segment_id, "
        + "schema_fingerprint, num_rows) "
        + "VALUES "
        + "(:id, :dataSource, :created_date, :start, :end, :partitioned, "
        + ":version, :used, :payload, :used_status_last_updated, :upgraded_from_segment_id, "
        + ":schema_fingerprint, :num_rows)"
    );
  }

  @Override
  public int markSegmentsUnused(String dataSource, Interval interval)
  {
    return query.markSegmentsUnused(dataSource, interval);
  }

  private void insertSegmentsInBatches(
      Set<DataSegmentPlus> segments,
      String insertSql
  ) throws Exception
  {
    final List<List<DataSegmentPlus>> partitionedSegments = Lists.partition(
        new ArrayList<>(segments),
        MAX_SEGMENTS_PER_BATCH
    );

    final boolean persistAdditionalMetadata = insertSql.contains(":schema_fingerprint");

    // SELECT -> INSERT can fail due to races; callers must be prepared to retry.
    // Avoiding ON DUPLICATE KEY since it's not portable.
    // Avoiding try/catch since it may cause inadvertent transaction-splitting.
    final PreparedBatch batch = handle.prepareBatch(
        StringUtils.format(insertSql, dbTables.getSegmentsTable(), connector.getQuoteString())
    );

    for (List<DataSegmentPlus> partition : partitionedSegments) {
      for (DataSegmentPlus segmentPlus : partition) {
        final DataSegment segment = segmentPlus.getDataSegment();
        PreparedBatchPart preparedBatchPart =
            batch.add()
                 .bind("id", segment.getId().toString())
                 .bind("dataSource", segment.getDataSource())
                 .bind("created_date", nullSafeString(segmentPlus.getCreatedDate()))
                 .bind("start", segment.getInterval().getStart().toString())
                 .bind("end", segment.getInterval().getEnd().toString())
                 .bind("partitioned", true)
                 .bind("version", segment.getVersion())
                 .bind("used", Boolean.TRUE.equals(segmentPlus.getUsed()))
                 .bind("payload", jsonMapper.writeValueAsBytes(segment))
                 .bind("used_status_last_updated", nullSafeString(segmentPlus.getUsedStatusLastUpdatedDate()))
                 .bind("upgraded_from_segment_id", segmentPlus.getUpgradedFromSegmentId());

        if (persistAdditionalMetadata) {
          preparedBatchPart
              .bind("num_rows", segmentPlus.getNumRows())
              .bind("schema_fingerprint", segmentPlus.getSchemaFingerprint());
        }
      }

      // Execute the batch and ensure that all the segments were inserted
      final int[] affectedRows = batch.execute();

      final List<DataSegment> failedInserts = new ArrayList<>();
      for (int i = 0; i < partition.size(); ++i) {
        if (affectedRows[i] != 1) {
          failedInserts.add(partition.get(i).getDataSegment());
        }
      }
      if (!failedInserts.isEmpty()) {
        throw InternalServerError.exception(
            "Failed to insert segments in metadata store: %s",
            SegmentUtils.commaSeparatedIdentifiers(failedInserts)
        );
      }
    }
  }

  private static String nullSafeString(DateTime time)
  {
    return time == null ? null : time.toString();
  }
}
