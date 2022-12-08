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

package org.apache.druid.segment.incremental;

import java.util.Map;

public class MutableRowIngestionMeters implements RowIngestionMeters
{
  private long processed;
  private long processedWithError;
  private long unparseable;
  private long thrownAway;
  private long processedBytes;

  @Override
  public long getProcessed()
  {
    return processed;
  }

  @Override
  public void incrementProcessed()
  {
    processed++;
  }

  @Override
  public void incrementProcessedBytes(long increment)
  {
    processedBytes += increment;
  }

  @Override
  public long getProcessedBytes()
  {
    return processedBytes;
  }

  @Override
  public long getProcessedWithError()
  {
    return processedWithError;
  }

  @Override
  public void incrementProcessedWithError()
  {
    processedWithError++;
  }

  @Override
  public long getUnparseable()
  {
    return unparseable;
  }

  @Override
  public void incrementUnparseable()
  {
    unparseable++;
  }

  @Override
  public long getThrownAway()
  {
    return thrownAway;
  }

  @Override
  public void incrementThrownAway()
  {
    thrownAway++;
  }

  @Override
  public RowIngestionMetersTotals getTotals()
  {
    return new RowIngestionMetersTotals(
        processed,
        processedBytes,
        processedWithError,
        thrownAway,
        unparseable
    );
  }

  @Override
  public Map<String, Object> getMovingAverages()
  {
    throw new UnsupportedOperationException();
  }

  public void addRowIngestionMetersTotals(RowIngestionMetersTotals rowIngestionMetersTotals)
  {
    this.processed += rowIngestionMetersTotals.getProcessed();
    this.processedWithError += rowIngestionMetersTotals.getProcessedWithError();
    this.unparseable += rowIngestionMetersTotals.getUnparseable();
    this.thrownAway += rowIngestionMetersTotals.getThrownAway();
    this.processedBytes += rowIngestionMetersTotals.getProcessedBytes();
  }
}
