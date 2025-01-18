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

package org.apache.druid.metadata.segment.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataCache implements SegmentsMetadataCache
{
  private static final EmittingLogger log = new EmittingLogger(SqlSegmentsMetadataCache.class);
  private static final String METRIC_PREFIX = "segment/metadataCache/";

  private enum CacheState
  {
    STOPPED, STARTING, READY
  }

  private final ObjectMapper jsonMapper;
  private final Duration pollDuration;
  private final boolean isCacheEnabled;
  private final Supplier<MetadataStorageTablesConfig> tablesConfig;
  private final SQLMetadataConnector connector;

  private final ScheduledExecutorService pollExecutor;
  private final ServiceEmitter emitter;

  private final AtomicReference<CacheState> currentCacheState
      = new AtomicReference<>(CacheState.STOPPED);

  private final ConcurrentHashMap<String, DatasourceSegmentCache> datasourceToSegmentCache
      = new ConcurrentHashMap<>();
  private final AtomicReference<DateTime> pollFinishTime = new AtomicReference<>();

  @Inject
  public SqlSegmentsMetadataCache(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> tablesConfig,
      SQLMetadataConnector connector,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter
  )
  {
    this.jsonMapper = jsonMapper;
    this.isCacheEnabled = config.get().isUseCache();
    this.pollDuration = config.get().getPollDuration().toStandardDuration();
    this.tablesConfig = tablesConfig;
    this.connector = connector;
    this.pollExecutor = isCacheEnabled ? executorFactory.create(1, "SegmentsMetadataCache-%s") : null;
    this.emitter = emitter;
  }


  @Override
  @LifecycleStart
  public synchronized void start()
  {
    if (isCacheEnabled && currentCacheState.compareAndSet(CacheState.STOPPED, CacheState.STARTING)) {
      // Clean up any stray entries in the cache left over due to race conditions
      tearDown();
      pollExecutor.schedule(this::pollSegments, pollDuration.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  @LifecycleStop
  public synchronized void stop()
  {
    if (isCacheEnabled) {
      currentCacheState.set(CacheState.STOPPED);
      tearDown();
    }

    // TODO: Handle race conditions
    // T1: sees cache as ready
    // T2: stops the cache
    // T1: tries to read some value from the cache and fails
    // Question: Should leader term validation logic live in the cache itself?
    // It could simplify certain operations
    // But then every method called on the cache will require the expected leader term to be passed to it?
    // I am not sure if that is such a great idea
    // I think it makes more sense to have that info at the transaction level or something

    // Should start-stop wait on everything else?
    // When does stop happen?
    // 1. Leadership changes: If leadership has changed, no point continuing the operation?
    //    In the current implementation, a task action would continue executing even if leadership has been lost?
    //    Yes, I do think so.
    //    Solution: If leadership has changed, transaction would fail, we wouldn't need to read or write anymore

    // 2. Service start-stop. Again no point worrying about the cache
  }

  @Override
  public boolean isReady()
  {
    return currentCacheState.get() == CacheState.READY;
  }

  @Override
  public Set<String> findExistingSegmentIds(String dataSource, Set<DataSegment> segments)
  {
    verifyCacheIsReady();

    Set<String> segmentIdsToFind = segments.stream()
                                           .map(s -> s.getId().toString())
                                           .collect(Collectors.toSet());
    return datasourceToSegmentCache
        .getOrDefault(dataSource, DatasourceSegmentCache.empty())
        .getSegmentIdsIn(segmentIdsToFind);
  }

  @Override
  public void addSegments(String dataSource, Set<DataSegmentPlus> segments)
  {
    verifyCacheIsReady();

    datasourceToSegmentCache
        .computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache())
        .addSegments(segments);
  }

  @Override
  public Map<String, SegmentTimeline> getDataSourceToUsedSegmentTimeline()
  {
    verifyCacheIsReady();
    return Map.of();
  }

  private void verifyCacheIsReady()
  {
    if (!isReady()) {
      throw DruidException.defensive("Segment metadata cache is not ready yet.");
    }
  }

  private boolean isStopped()
  {
    return currentCacheState.get() == CacheState.STOPPED;
  }

  private void tearDown()
  {
    datasourceToSegmentCache.forEach((datasource, state) -> state.clear());
    datasourceToSegmentCache.clear();
  }

  private void pollSegments()
  {
    final Stopwatch sincePollStart = Stopwatch.createStarted();
    if (isStopped()) {
      tearDown();
      return;
    }

    final Map<String, Set<String>> datasourceToKnownSegmentIds = new HashMap<>();
    final Map<String, Set<String>> datasourceToRefreshSegmentIds = new HashMap<>();

    final AtomicInteger countOfRefreshedUnusedSegments = new AtomicInteger(0);

    final String getAllIdsSql = "SELECT id, dataSource, used, used_status_last_updated FROM %s";
    connector.inReadOnlyTransaction(
        (handle, status) -> handle
            .createQuery(StringUtils.format(getAllIdsSql, getSegmentsTable()))
            .setFetchSize(connector.getStreamingFetchSize())
            .map((index, r, ctx) -> {
              try {
                final String segmentId = r.getString("id");
                final boolean isUsed = r.getBoolean("used");
                final String dataSource = r.getString("dataSource");
                final String updatedColValue = r.getString("used_status_last_updated");
                final DateTime lastUpdatedTime
                    = updatedColValue == null ? null : DateTimes.of(updatedColValue);

                final SegmentState metadataState
                    = new SegmentState(segmentId, dataSource, isUsed, lastUpdatedTime);

                final DatasourceSegmentCache cache
                    = datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());

                if (cache.shouldRefreshSegment(metadataState)) {
                  if (metadataState.isUsed()) {
                    datasourceToRefreshSegmentIds.computeIfAbsent(dataSource, ds -> new HashSet<>())
                                                 .add(segmentId);
                  } else if (cache.refreshUnusedSegment(metadataState)) {
                    countOfRefreshedUnusedSegments.incrementAndGet();
                    emitDatasourceMetric(dataSource, "refreshed/unused", 1);
                  }
                }

                datasourceToKnownSegmentIds.computeIfAbsent(dataSource, ds -> new HashSet<>())
                                           .add(segmentId);
                return 0;
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to read segments from metadata store.").emit();
                // Do not throw an exception so that polling continues
                return 1;
              }
            })
    );

    if (countOfRefreshedUnusedSegments.get() > 0) {
      log.info("Refreshed total [%d] unused segments from metadata store.", countOfRefreshedUnusedSegments.get());
    }

    // TODO: handle changes made to the metadata store between these two database calls

    // Remove unknown segment IDs from cache
    // This is safe to do since updates are always made first to metadata store and then to cache
    datasourceToSegmentCache.forEach((dataSource, cache) -> {
      final Set<String> unknownSegmentIds = cache.getSegmentIdsNotIn(
          datasourceToKnownSegmentIds.getOrDefault(dataSource, Set.of())
      );
      final int numSegmentsRemoved = cache.removeSegmentIds(unknownSegmentIds);
      if (numSegmentsRemoved > 0) {
        log.info(
            "Removed [%d] unknown segment IDs from cache of datasource[%s].",
            numSegmentsRemoved, dataSource
        );
      }
    });

    if (isStopped()) {
      tearDown();
      return;
    }

    final AtomicInteger countOfRefreshedUsedSegments = new AtomicInteger(0);
    datasourceToRefreshSegmentIds.forEach((dataSource, segmentIds) -> {
      long numUpdatedUsedSegments = connector.inReadOnlyTransaction((handle, status) -> {
        final DatasourceSegmentCache cache
            = datasourceToSegmentCache.computeIfAbsent(dataSource, ds -> new DatasourceSegmentCache());

        return SqlSegmentsMetadataQuery
            .forHandle(handle, connector, tablesConfig.get(), jsonMapper)
            .retrieveSegmentsById(dataSource, segmentIds)
            .stream()
            .map(cache::refreshUsedSegment)
            .filter(updated -> updated)
            .count();
      });

      emitDatasourceMetric(dataSource, "refresh/used", numUpdatedUsedSegments);
      countOfRefreshedUsedSegments.addAndGet((int) numUpdatedUsedSegments);
    });

    if (countOfRefreshedUsedSegments.get() > 0) {
      log.info(
          "Refreshed total [%d] used segments from metadata store.",
          countOfRefreshedUnusedSegments.get()
      );
    }

    // TODO: poll pending segments

    emitMetric("poll/time", sincePollStart.millisElapsed());
    pollFinishTime.set(DateTimes.nowUtc());

    if (isStopped()) {
      tearDown();
    } else {
      currentCacheState.compareAndSet(CacheState.STARTING, CacheState.READY);

      // Schedule the next poll
      final long nextPollDelay = Math.max(pollDuration.getMillis() - sincePollStart.millisElapsed(), 0);
      pollExecutor.schedule(this::pollSegments, nextPollDelay, TimeUnit.MILLISECONDS);
    }
  }

  private String getSegmentsTable()
  {
    return tablesConfig.get().getSegmentsTable();
  }

  private void emitMetric(String metric, long value)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setMetric(METRIC_PREFIX + metric, value)
    );
  }

  private void emitDatasourceMetric(String datasource, String metric, long value)
  {
    emitter.emit(
        ServiceMetricEvent.builder()
                          .setDimension(DruidMetrics.DATASOURCE, datasource)
                          .setMetric(METRIC_PREFIX + metric, value)
    );
  }

}
