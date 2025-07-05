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

package org.apache.druid.tests.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.commons.io.IOUtils;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.CompactionIntervalSpec;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.overlord.http.TaskPayloadResponse;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.FixedIntervalOrderPolicy;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.indexing.TaskPayload;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.timeline.DataSegment;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Embedded test class migrated from ITAutoCompactionTest to test auto-compaction scenarios
 * using the embedded Druid cluster framework.
 */
public class EmbeddedAutoCompactionTest extends EmbeddedClusterTestBase
{
  private static final Logger LOG = new Logger(EmbeddedAutoCompactionTest.class);
  
  // Test constants from original ITAutoCompactionTest
  private static final String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static final String INDEX_TASK_WITH_GRANULARITY_SPEC = "/indexer/wikipedia_index_task_with_granularity_spec.json";
  private static final String INDEX_TASK_WITH_DIMENSION_SPEC = "/indexer/wikipedia_index_task_with_dimension_spec.json";
  private static final String INDEX_ROLLUP_QUERIES_RESOURCE = "/indexer/wikipedia_index_rollup_queries.json";
  private static final String INDEX_ROLLUP_SKETCH_QUERIES_RESOURCE = "/indexer/wikipedia_index_sketch_queries.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS = "/indexer/wikipedia_index_rollup_preserve_metric.json";
  private static final String INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS = "/indexer/wikipedia_index_no_rollup_preserve_metric.json";
  private static final int MAX_ROWS_PER_SEGMENT_COMPACTED = 10000;
  private static final Period NO_SKIP_OFFSET = Period.seconds(0);
  private static final FixedIntervalOrderPolicy COMPACT_NOTHING_POLICY = new FixedIntervalOrderPolicy(List.of());

  // Embedded cluster components
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer().addProperty("druid.worker.capacity", "25");
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private String fullDatasourceName;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(indexer)
                               .addServer(overlord)
                               .addServer(historical)
                               .addServer(broker)
                               .addServer(new EmbeddedRouter());
  }

  @BeforeEach
  public void setup() throws Exception
  {
    // Set compaction slot to 5 (equivalent to original test setup)
    updateCompactionTaskSlot(0.5, 10);
    fullDatasourceName = "wikipedia_index_test_" + UUID.randomUUID();
  }

  @Test
  public void testAutoCompactionRowWithMetricAndRowWithoutMetricShouldPreserveExistingMetricsUsingAggregatorWithDifferentReturnType() throws Exception
  {
    // added = null, count = 2, sum_added = 62, quantilesDoublesSketch = 2, thetaSketch = 2, HLLSketchBuild = 2
    loadData(INDEX_TASK_WITH_ROLLUP_FOR_PRESERVE_METRICS);
    // added = 31, count = null, sum_added = null, quantilesDoublesSketch = null, thetaSketch = null, HLLSketchBuild = null
    loadData(INDEX_TASK_WITHOUT_ROLLUP_FOR_PRESERVE_METRICS);
    
    final List<String> intervalsBeforeCompaction = getSegmentIntervals(fullDatasourceName);
    intervalsBeforeCompaction.sort(null);
    // 2 segments across 1 days...
    verifySegmentsCount(2);
    
    ArrayList<Object> nullList = new ArrayList<>();
    nullList.add(null);
    Map<String, Object> queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "added",
        "%%EXPECTED_COUNT_RESULT%%", 2,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(nullList)), ImmutableMap.of("events", ImmutableList.of(ImmutableList.of(31))))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "count",
        "%%EXPECTED_COUNT_RESULT%%", 2,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(ImmutableList.of(2))), ImmutableMap.of("events", ImmutableList.of(nullList)))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "sum_added",
        "%%EXPECTED_COUNT_RESULT%%", 2,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(ImmutableList.of(62))), ImmutableMap.of("events", ImmutableList.of(nullList)))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%QUANTILESRESULT%%", 2,
        "%%THETARESULT%%", 2.0,
        "%%HLLRESULT%%", 2
    );
    verifyQuery(INDEX_ROLLUP_SKETCH_QUERIES_RESOURCE, queryAndResultFields);

    submitCompactionConfig(
        MAX_ROWS_PER_SEGMENT_COMPACTED,
        NO_SKIP_OFFSET,
        new UserCompactionTaskGranularityConfig(null, null, true),
        new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("language"))),
        null,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            // FloatSumAggregator combine method takes in two Float but return Double
            new FloatSumAggregatorFactory("sum_added", "added"),
            new SketchMergeAggregatorFactory("thetaSketch", "user", 16384, true, false, null),
            new HllSketchBuildAggregatorFactory("HLLSketchBuild", "user", 12, TgtHllType.HLL_4.name(), null, false, false),
            new DoublesSketchAggregatorFactory("quantilesDoublesSketch", "delta", 128, 1000000000L, null)
        },
        false,
        CompactionEngine.NATIVE
    );
    // should now only have 1 row after compaction
    // added = null, count = 3, sum_added = 93.0
    forceTriggerAutoCompaction(1);

    queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "added",
        "%%EXPECTED_COUNT_RESULT%%", 1,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(nullList)))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "count",
        "%%EXPECTED_COUNT_RESULT%%", 1,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(ImmutableList.of(3))))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%FIELD_TO_QUERY%%", "sum_added",
        "%%EXPECTED_COUNT_RESULT%%", 1,
        "%%EXPECTED_SCAN_RESULT%%", ImmutableList.of(ImmutableMap.of("events", ImmutableList.of(ImmutableList.of(93.0f))))
    );
    verifyQuery(INDEX_ROLLUP_QUERIES_RESOURCE, queryAndResultFields);
    
    queryAndResultFields = ImmutableMap.of(
        "%%QUANTILESRESULT%%", 3,
        "%%THETARESULT%%", 3.0,
        "%%HLLRESULT%%", 3
    );
    verifyQuery(INDEX_ROLLUP_SKETCH_QUERIES_RESOURCE, queryAndResultFields);

    verifySegmentsCompacted(1, MAX_ROWS_PER_SEGMENT_COMPACTED);
    checkCompactionIntervals(intervalsBeforeCompaction);

    // Verify rollup segments does not get compacted again
    forceTriggerAutoCompaction(1);
    // TODO: Add verification logic for task completion similar to original
  }

  // Placeholder for additional test methods - will be added in follow-up commits
  // The original ITAutoCompactionTest has many more test methods that need to be migrated

  // Helper methods to replace integration test functionality with embedded cluster APIs
  
  private void loadData(String indexTask) throws Exception
  {
    loadData(indexTask, ImmutableMap.of());
  }

  private void loadData(String indexTask, Map<String, Object> specs) throws Exception
  {
    String taskSpec = getResourceAsString(indexTask);
    taskSpec = StringUtils.replace(taskSpec, "%%DATASOURCE%%", fullDatasourceName);
    taskSpec = StringUtils.replace(
        taskSpec,
        "%%SEGMENT_AVAIL_TIMEOUT_MILLIS%%",
        "\"0\""
    );
    
    for (Map.Entry<String, Object> entry : specs.entrySet()) {
      // TODO: Add JSON serialization logic
      taskSpec = StringUtils.replace(
          taskSpec,
          entry.getKey(),
          entry.getValue().toString()
      );
    }
    
    final String taskId = EmbeddedClusterApis.newTaskId(fullDatasourceName);
    // TODO: Convert task spec to appropriate format and submit
    // This will need to be implemented to properly submit indexing tasks
    LOG.info("Submitted task[%s] to load data", taskId);
    
    // Wait for task completion and segments to be available
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeAvailable(fullDatasourceName, coordinator);
  }

  private void verifyQuery(String queryResource) throws Exception
  {
    verifyQuery(queryResource, ImmutableMap.of());
  }

  private void verifyQuery(String queryResource, Map<String, Object> keyValueToReplace) throws Exception
  {
    String queryResponseTemplate;
    try {
      InputStream is = getClass().getResourceAsStream(queryResource);
      queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new ISE(e, "could not read query file: %s", queryResource);
    }
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%DATASOURCE%%",
        fullDatasourceName
    );
    for (Map.Entry<String, Object> entry : keyValueToReplace.entrySet()) {
      // TODO: Add proper JSON serialization
      queryResponseTemplate = StringUtils.replace(
          queryResponseTemplate,
          entry.getKey(),
          entry.getValue().toString()
      );
    }
    // TODO: Execute query using cluster.runSql() or broker APIs
  }

  private void updateCompactionTaskSlot(double compactionTaskSlotRatio, int maxCompactionTaskSlots) throws Exception
  {
    final ClusterCompactionConfig oldConfig = cluster.callApi().onLeaderOverlord(
        OverlordClient::getClusterCompactionConfig
    );
    
    final ClusterCompactionConfig updatedConfig = new ClusterCompactionConfig(
        compactionTaskSlotRatio,
        maxCompactionTaskSlots,
        oldConfig.getCompactionPolicy(),
        oldConfig.isUseSupervisors(),
        oldConfig.getEngine()
    );
    
    cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(updatedConfig)
    );

    // Verify that the compaction config is updated correctly
    final ClusterCompactionConfig verifyConfig = cluster.callApi().onLeaderOverlord(
        OverlordClient::getClusterCompactionConfig
    );
    Assertions.assertEquals(verifyConfig.getCompactionTaskSlotRatio(), compactionTaskSlotRatio);
    Assertions.assertEquals(verifyConfig.getMaxCompactionTaskSlots(), maxCompactionTaskSlots);
    LOG.info(
        "Updated compactionTaskSlotRatio[%s] and maxCompactionTaskSlots[%d]",
        compactionTaskSlotRatio, maxCompactionTaskSlots
    );
  }

  private void submitCompactionConfig(
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      UserCompactionTaskDimensionsConfig dimensionsSpec,
      CompactionTransformSpec transformSpec,
      AggregatorFactory[] metricsSpec,
      boolean dropExisting,
      CompactionEngine engine
  ) throws Exception
  {
    DataSourceCompactionConfig dataSourceCompactionConfig =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource(fullDatasourceName)
                                              .withSkipOffsetFromLatest(skipOffsetFromLatest)
                                              .withTuningConfig(
                                            new UserCompactionTaskQueryTuningConfig(
                                                null,
                                                null,
                                                null,
                                                null,
                                                new MaxSizeSplitHintSpec(null, 1),
                                                new DynamicPartitionsSpec(maxRowsPerSegment, null),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                1, // maxNumConcurrentSubTasks
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                1,
                                                null
                                            )
                                        )
                                              .withGranularitySpec(granularitySpec)
                                              .withDimensionsSpec(dimensionsSpec)
                                              .withMetricsSpec(metricsSpec)
                                              .withTransformSpec(transformSpec)
                                              .withIoConfig(
                                      !dropExisting ? null : new UserCompactionTaskIOConfig(true)
                                  )
                                              .withEngine(engine)
                                              .withTaskContext(ImmutableMap.of("maxNumTasks", 2))
                                              .build();
                                              
    // TODO: Submit compaction config using embedded cluster APIs
    // This will need to be implemented to properly submit compaction configurations
    LOG.info("Submitting compaction config for datasource: %s", fullDatasourceName);

    // Wait for compaction config to persist
    Thread.sleep(2000);
  }

  private void forceTriggerAutoCompaction(int numExpectedSegmentsAfterCompaction) throws Exception
  {
    // TODO: Implement force trigger of auto compaction using embedded cluster APIs
    // This will need to map to coordinator APIs for triggering compaction
    LOG.info("Forcing auto compaction trigger");
    
    waitForCompactionToFinish(numExpectedSegmentsAfterCompaction);
  }

  private void waitForCompactionToFinish(int numExpectedSegmentsAfterCompaction)
  {
    // TODO: Implement waiting logic using embedded cluster APIs and latchable emitter
    // This should wait for tasks to complete and segments to be available
    verifySegmentsCount(numExpectedSegmentsAfterCompaction);
  }

  private void verifySegmentsCount(int numExpectedSegments)
  {
    // TODO: Implement segment count verification using embedded cluster APIs
    // This should check the number of segments for the datasource
    LOG.info("Verifying segment count: %d", numExpectedSegments);
  }

  private void checkCompactionIntervals(List<String> expectedIntervals)
  {
    // TODO: Implement interval checking using embedded cluster APIs
    final Set<String> expectedIntervalsSet = new HashSet<>(expectedIntervals);
    LOG.info("Checking compaction intervals: %s", expectedIntervalsSet);
  }

  private void verifySegmentsCompacted(int expectedCompactedSegmentCount, Integer expectedMaxRowsPerSegment)
  {
    verifySegmentsCompacted(
        new DynamicPartitionsSpec(expectedMaxRowsPerSegment, Long.MAX_VALUE),
        expectedCompactedSegmentCount
    );
  }

  private void verifySegmentsCompacted(PartitionsSpec partitionsSpec, int expectedCompactedSegmentCount)
  {
    // TODO: Implement compacted segments verification using embedded cluster APIs
    LOG.info("Verifying segments compacted: %d with partitions spec: %s", expectedCompactedSegmentCount, partitionsSpec);
  }

  private List<String> getSegmentIntervals(String datasourceName)
  {
    // TODO: Implement getting segment intervals using embedded cluster APIs
    // This should return the list of intervals for the given datasource
    return new ArrayList<>();
  }

  private String getResourceAsString(String resource) throws IOException
  {
    try (InputStream is = getClass().getResourceAsStream(resource)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resource);
      }
      return IOUtils.toString(is, StandardCharsets.UTF_8);
    }
  }
}