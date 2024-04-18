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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.sql.ResultSet;

/**
 * Represents a record in the pending segments metadata table.
 */
public class PendingSegmentRecord
{
  private final SegmentIdWithShardSpec id;
  private final String sequenceName;
  private final String sequencePrevId;
  private final String upgradedFromSegmentId;
  private final String taskAllocatorId;

  public PendingSegmentRecord(
      SegmentIdWithShardSpec id,
      String sequenceName,
      String sequencePrevId,
      @Nullable String upgradedFromSegmentId,
      @Nullable String taskAllocatorId
  )
  {
    this.id = id;
    this.sequenceName = sequenceName;
    this.sequencePrevId = sequencePrevId;
    this.upgradedFromSegmentId = upgradedFromSegmentId;
    this.taskAllocatorId = taskAllocatorId;
  }

  /**
   * Unique ID of this pending segment.
   */
  public SegmentIdWithShardSpec getId()
  {
    return id;
  }

  public String getSequenceName()
  {
    return sequenceName;
  }

  /**
   * ID of the last segment allocated using the same {@link #sequenceName}.
   */
  public String getSequencePrevId()
  {
    return sequencePrevId;
  }

  /**
   * ID of the original pending segment which was upgraded to obtain this
   * pending segment.
   *
   * @return null for pending segments that were freshly allocated and not
   * upgraded from any other pending segment.
   */
  @Nullable
  public String getUpgradedFromSegmentId()
  {
    return upgradedFromSegmentId;
  }

  /**
   * Unique string used by an appending task (or its sub-tasks and replicas)
   * to allocate pending segments and identify pending segments allocated to it.
   */
  @Nullable
  public String getTaskAllocatorId()
  {
    return taskAllocatorId;
  }

  @SuppressWarnings("UnstableApiUsage")
  public String computeSequenceNamePrevIdSha1(boolean skipSegmentLineageCheck)
  {
    final Hasher hasher = Hashing.sha1().newHasher()
                                 .putBytes(StringUtils.toUtf8(getSequenceName()))
                                 .putByte((byte) 0xff);

    if (skipSegmentLineageCheck) {
      final Interval interval = getId().getInterval();
      hasher
          .putLong(interval.getStartMillis())
          .putLong(interval.getEndMillis());
    } else {
      hasher
          .putBytes(StringUtils.toUtf8(getSequencePrevId()));
    }

    hasher.putByte((byte) 0xff);
    hasher.putBytes(StringUtils.toUtf8(getId().getVersion()));

    return BaseEncoding.base16().encode(hasher.hash().asBytes());
  }

  public static PendingSegmentRecord fromResultSet(ResultSet resultSet, ObjectMapper jsonMapper)
  {
    try {
      final byte[] payload = resultSet.getBytes("payload");
      return new PendingSegmentRecord(
          jsonMapper.readValue(payload, SegmentIdWithShardSpec.class),
          resultSet.getString("sequence_name"),
          resultSet.getString("sequence_prev_id"),
          resultSet.getString("upgraded_from_segment_id"),
          resultSet.getString("task_allocator_id")
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
