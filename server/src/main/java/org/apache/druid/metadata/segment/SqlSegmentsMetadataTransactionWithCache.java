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

import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.metadata.segment.cache.SegmentsMetadataCache;

public class SqlSegmentsMetadataTransactionWithCache implements SqlSegmentsMetadataTransaction
{
  private final SqlSegmentsMetadataQuery delegate;
  private final SegmentsMetadataCache metadataCache;
  private final DruidLeaderSelector leaderSelector;

  private final int startTerm;

  public SqlSegmentsMetadataTransactionWithCache(
      SqlSegmentsMetadataQuery delegate,
      SegmentsMetadataCache metadataCache,
      DruidLeaderSelector leaderSelector
  )
  {
    this.delegate = delegate;
    this.metadataCache = metadataCache;
    this.leaderSelector = leaderSelector;

    if (leaderSelector.isLeader()) {
      throw InternalServerError.exception("Not leader anymore");
    } else {
      this.startTerm = leaderSelector.localTerm();
    }
  }

  private boolean isLeaderWithSameTerm()
  {
    return leaderSelector.isLeader() && startTerm == leaderSelector.localTerm();
  }


}
