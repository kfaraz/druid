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

package org.apache.druid.indexing.overlord.supervisor;

import org.joda.time.Interval;

/**
 * ETL template to create a {@link BatchIndexingJob} that indexes data from an
 * {@link IndexingSource} into an {@link IndexingTarget}.
 * <p>
 * Templates must allow interpolation of the source, target and interval and any
 * other parameters as applicable.
 */
public interface BatchIndexingJobTemplate
{
  /**
   * Creates a new job with this template.
   */
  BatchIndexingJob createTask(
      IndexingSource source,
      IndexingTarget target,
      Interval interval
  );

  /**
   * Given an interval, just create a compaction task with all the required params.
   * In essence, the same stuff as {@code CompactSegments.submitTasks()}.
   */
  class InlineCompactionTemplate {

  }

  /**
   * A specific {@code TableId} can be given. Otherwise, use the target {@code TableId.datasource(target)}
   * Resolve the table id to get the table spec / table definition.
   * Then create a {@code CompactionTask}, pretty much the same as a {@code CatalogDataSourceCompactionConfig}.
   */
  class CatalogCompactionTemplate {

  }
}
