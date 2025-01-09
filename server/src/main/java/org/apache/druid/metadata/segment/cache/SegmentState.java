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

import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.sql.ResultSet;
import java.util.Optional;

public class SegmentState
{
  private final String segmentId;
  private final String dataSource;
  private final boolean used;
  private final DateTime lastUpdatedTime;

  public SegmentState(String segmentId, String dataSource, boolean used, DateTime lastUpdatedTime)
  {
    this.segmentId = segmentId;
    this.dataSource = dataSource;
    this.used = used;
    this.lastUpdatedTime = lastUpdatedTime == null ? DateTimes.EPOCH : lastUpdatedTime;
  }

  public String getSegmentId()
  {
    return segmentId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public boolean isUsed()
  {
    return used;
  }

  public DateTime getLastUpdatedTime()
  {
    return lastUpdatedTime;
  }

  /**
   * Tries to create a SegmentState from the given result set.
   *
   * @return An empty optional if an exception is encountered while reading the result set.
   */
  public static Optional<SegmentState> fromResultSet(ResultSet resultSet)
  {
    try {
      final String updatedColValue = resultSet.getString("used_status_last_updated");
      final DateTime lastUpdatedTime
          = updatedColValue == null ? null : DateTimes.of(updatedColValue);

      return Optional.of(
          new SegmentState(
              resultSet.getString("id"),
              resultSet.getString("dataSource"),
              resultSet.getBoolean("used"),
              lastUpdatedTime
          )
      );
    }
    catch (Exception e) {
      return Optional.empty();
    }
  }
}
