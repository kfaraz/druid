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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class QueuedSegment
{
  private final DataSegment segment;
  private final DataSegmentChangeRequest changeRequest;
  private final SegmentAction action;
  // Guaranteed to store only non-null elements
  private final List<LoadPeonCallback> callbacks = new ArrayList<>();

  QueuedSegment(
      DataSegment segment,
      SegmentAction action,
      @Nullable LoadPeonCallback callback
  )
  {
    this.segment = segment;
    this.action = action;
    this.changeRequest = (action == SegmentAction.DROP)
                         ? new SegmentChangeRequestDrop(segment)
                         : new SegmentChangeRequestLoad(segment);
    if (callback != null) {
      callbacks.add(callback);
    }
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  public SegmentAction getAction()
  {
    return action;
  }

  public String getSegmentIdentifier()
  {
    return segment.getId().toString();
  }

  public long getSegmentSize()
  {
    return segment.getSize();
  }

  public void addCallback(@Nullable LoadPeonCallback callback)
  {
    if (callback != null) {
      synchronized (callbacks) {
        callbacks.add(callback);
      }
    }
  }

  List<LoadPeonCallback> snapshotCallbacks()
  {
    synchronized (callbacks) {
      // Return an immutable copy so that callers don't have to worry about concurrent modification
      return ImmutableList.copyOf(callbacks);
    }
  }

  public DataSegmentChangeRequest getChangeRequest()
  {
    return changeRequest;
  }

  @Override
  public String toString()
  {
    return changeRequest.toString();
  }
}
