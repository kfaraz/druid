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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

import java.util.concurrent.atomic.AtomicReference;

public class DatasourceCompactionQueue
{
  private static final Logger log = new Logger(DatasourceCompactionQueue.class);

  private final String datasource;
  private final AtomicReference<DataSourceCompactionConfig> config = new AtomicReference<>();

  public DatasourceCompactionQueue(String datasource)
  {
    this.datasource = datasource;
  }

  public void updateConfig(DataSourceCompactionConfig latestConfig)
  {
    final DataSourceCompactionConfig previousConfig = config.getAndSet(latestConfig);

    // TODO: check if anything has changed, then reset the corresponding stuff
  }

  public void stop()
  {
    log.info("Stopping compaction scheduling for datasource[%s].", datasource);
  }
}
