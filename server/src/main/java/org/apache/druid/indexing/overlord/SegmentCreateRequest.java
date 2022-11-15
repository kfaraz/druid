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

package org.apache.druid.indexing.overlord;

import org.apache.druid.timeline.partition.PartialShardSpec;

public class SegmentCreateRequest
{
  private final String sequenceName;
  private final String previousSegmentId;
  private final String version;
  private final PartialShardSpec partialShardSpec;

  public SegmentCreateRequest(
      String sequenceName,
      String previousSegmentId,
      String version,
      PartialShardSpec partialShardSpec
  )
  {
    this.sequenceName = sequenceName;
    this.previousSegmentId = previousSegmentId;
    this.version = version;
    this.partialShardSpec = partialShardSpec;
  }

  public String getSequenceId()
  {
    return getSequenceId(sequenceName, previousSegmentId);
  }

  public String getSequenceName()
  {
    return sequenceName;
  }

  public String getPreviousSegmentId()
  {
    return previousSegmentId;
  }

  public String getVersion()
  {
    return version;
  }

  public PartialShardSpec getPartialShardSpec()
  {
    return partialShardSpec;
  }

  public static String getSequenceId(String sequenceName, String previousSegmentId)
  {
    return sequenceName + "####" + previousSegmentId;
  }
}
