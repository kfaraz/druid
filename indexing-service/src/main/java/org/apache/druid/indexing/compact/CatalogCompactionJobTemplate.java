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

import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.server.coordinator.CatalogDataSourceCompactionConfig;

import java.util.List;

/**
 * TODO:
 *  - For now, let's just start with a catalogTable type template
 *  which simply refers to a table definition and we use it as is.
 *  - Should the catalog contain only the table definition?
 *    - If yes, then we
 */
public class CatalogCompactionJobTemplate implements CompactionJobTemplate
{
  private final CatalogDataSourceCompactionConfig config;

  public CatalogCompactionJobTemplate(CatalogDataSourceCompactionConfig config)
  {
    this.config = config;
  }

  @Override
  public List<CompactionJob> createJobs(
      InputSource source,
      OutputDestination target,
      CompactionJobParams params
  )
  {
    return null;
  }
}
