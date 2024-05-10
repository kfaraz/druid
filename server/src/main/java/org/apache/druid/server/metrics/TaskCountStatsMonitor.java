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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.server.stats.Dimension;
import org.apache.druid.server.stats.DruidRunStats;
import org.apache.druid.server.stats.DruidStat;

import java.util.Map;

public class TaskCountStatsMonitor extends AbstractMonitor
{
  private final TaskCountStatsProvider statsProvider;

  @Inject
  public TaskCountStatsMonitor(
      TaskCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final DruidRunStats stats = statsProvider.getTaskCountStats();
    if (stats != null) {
      stats.forEachStat(
          (stat, dimensions, statValue)
              -> emit(emitter, stat, dimensions.getValues(), statValue)
      );
    }

    return true;
  }

  private void emit(ServiceEmitter emitter, DruidStat stat, Map<Dimension, String> dimensionValues, long value)
  {
    if (!stat.shouldEmit()) {
      return;
    }

    ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder();
    dimensionValues.forEach(
        (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
    );
    emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
  }

}
