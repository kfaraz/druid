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

package org.apache.druid.indexing.compact;

public class MetadataSegmentsWatcher
{
  // Keep polling for any updated segments
  // Delta poll will fetch new segments and segments whose used status has been toggled
  // The filter on used_status_last_updated will have to account for segments whose update transactions started earlier but finished late

  // SELECT * FROM druid_segments WHERE used_status_last_updated >= previous max - 10 minutes;

  // It becomes imperative to perform a deduplication
  // Once we know the exact set of segments that has been newly added or removed, we can just forward it to the compaction scheduler

  // Does it make sense to deduplicate only with the previous?

  // Super Previous fetch has segments with min0 <= updatedTime <= max0 <= t0
  // Previous fetch has segments with min1 <= updatedTime <= max1 <= t1
  // Current fetch has segments with  (max1 - 10 mins) <= updatedTime <= max2 <= t2



}
