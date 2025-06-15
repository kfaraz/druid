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

package org.apache.druid.simulate.overlord;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.simulate.EmbeddedDruidCluster;
import org.apache.druid.simulate.EmbeddedIndexer;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TODO:
 * - run a streaming supervisor with this setup
 * - run some existing tests with this setup
 * - write more tests
 */
public class OverlordTaskRunTest
{
  private static final EmbeddedOverlord OVERLORD = EmbeddedOverlord.create();
  private static final EmbeddedDruidCluster CLUSTER
      = EmbeddedDruidCluster.builder()
                            .with(EmbeddedIndexer.withProps(Map.of("druid.worker.capacity", "25")))
                            .with(OVERLORD)
                            .withDb()
                            .build();

  @ClassRule
  public static final RuleChain CLUSTER_RULE_CHAIN = CLUSTER.ruleChain();

  @Test(timeout = 60_000L)
  public void test_run10Tasks_concurrently()
  {
    runTasks(10);
  }

  @Test(timeout = 60_000L)
  public void test_run50Tasks_oneByOne()
  {
    for (int i = 0; i < 50; ++i) {
      runTasks(1);
    }
  }

  @Test(timeout = 60_000L)
  public void test_run25Tasks_concurrently()
  {
    runTasks(25);
  }

  @Test(timeout = 60_000L)
  public void test_run100Tasks_concurrently()
  {
    runTasks(100);
  }

  @Test
  public void test_runIndexTask_forInlineDatasource()
  {
    final String txnData10Days
        = "time,item,value"
          + "\n2025-06-01,shirt,105"
          + "\n2025-06-02,trousers,210"
          + "\n2025-06-03,jeans,150"
          + "\n2025-06-04,t-shirt,53"
          + "\n2025-06-05,microwave,1099"
          + "\n2025-06-06,spoon,11"
          + "\n2025-06-07,television,1100"
          + "\n2025-06-08,plant pots,75"
          + "\n2025-06-09,shirt,99"
          + "\n2025-06-10,toys,101";

    final String taskId = IdUtils.newTaskId("batch", TestDataSource.WIKI, null);
    final Task task = new IndexTask(
        taskId,
        null,
        new IndexTask.IndexIngestionSpec(
            DataSchema.builder()
                      .withTimestamp(new TimestampSpec("time", null, null))
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withDataSource(TestDataSource.WIKI)
                      .build(),
            new IndexTask.IndexIOConfig(
                new InlineInputSource(txnData10Days),
                new CsvInputFormat(null, null, null, true, 0, false),
                false,
                false
            ),
            null
        ),
        Map.of()
    );

    getResult(OVERLORD.client().runTask(taskId, task));
    verifyTaskHasSucceeded(taskId);

    final List<DataSegment> segments = new ArrayList<>(
        OVERLORD.segmentsMetadata().retrieveAllUsedSegments(TestDataSource.WIKI, null)
    );
    segments.sort(
        (o1, o2) -> Comparators.intervalsByStartThenEnd()
                               .compare(o1.getInterval(), o2.getInterval())
    );

    Assert.assertEquals(10, segments.size());

    DateTime start = DateTimes.of("2025-06-01");
    for (DataSegment segment : segments) {
      Assert.assertEquals(
          new Interval(start, Period.days(1)),
          segment.getInterval()
      );
      start = start.plusDays(1);
    }
  }

  private void runTasks(int count)
  {
    final List<String> taskIds = IntStream.range(0, count).mapToObj(
        i -> IdUtils.newTaskId("sim_test_" + i, "noop", null)
    ).collect(Collectors.toList());

    for (String taskId : taskIds) {
      getResult(
          OVERLORD.client().runTask(
              taskId,
              new NoopTask(taskId, null, null, 1L, 0L, Map.of())
          )
      );
    }

    for (String taskId : taskIds) {
      verifyTaskHasSucceeded(taskId);
    }
  }

  private static void verifyTaskHasSucceeded(String taskId)
  {
    OVERLORD.waitUntilTaskFinishes(taskId);
    final TaskStatusResponse currentStatus = getResult(
        OVERLORD.client().taskStatus(taskId)
    );
    Assert.assertNotNull(currentStatus.getStatus());
    Assert.assertEquals(
        StringUtils.format("Task[%s] has failed", taskId),
        TaskState.SUCCESS,
        currentStatus.getStatus().getStatusCode()
    );
  }


  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }
}
