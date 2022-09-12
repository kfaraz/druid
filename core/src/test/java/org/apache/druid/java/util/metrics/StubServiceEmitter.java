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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.ArrayList;
import java.util.List;

public class StubServiceEmitter extends ServiceEmitter
{
  private final List<Event> events = new ArrayList<>();

  public StubServiceEmitter(String service, String host)
  {
    super(service, host, null);
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof AlertEvent) {
      final AlertEvent alertEvent = (AlertEvent) event;
      System.out.printf(
          "[%s] [%s] [%s]: %s%n",
          alertEvent.getSeverity(),
          alertEvent.getService(),
          alertEvent.getFeed(),
          alertEvent.getDescription()
      );
    } else {
      events.add(event);
    }
  }

  public List<Event> getEvents()
  {
    return events;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void flush()
  {
    events.clear();
  }

  @Override
  public void close()
  {
  }
}
