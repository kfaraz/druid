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
import org.apache.druid.client.indexing.IndexingWorkerInfo;
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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.simulate.EmbeddedDruidCluster;
import org.apache.druid.simulate.EmbeddedIndexer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TODO:
 * - try to run a streaming supervisor with this setup
 * - try to run existing tests with this setup
 * - write more tests
 */
public class OverlordSimulationTest
{
  private static final EmbeddedOverlord OVERLORD = EmbeddedOverlord.create();

  @ClassRule
  public static final RuleChain cluster
      = EmbeddedDruidCluster.builder()
                            .with(EmbeddedIndexer.withProps(Map.of("druid.worker.capacity", "10")))
                            .with(OVERLORD)
                            .withDb()
                            .build();

  @Test
  public void test_getCurrentLeader()
  {
    final URI uri = run(OverlordClient::findCurrentLeader);
    Assert.assertEquals(8090, uri.getPort());
  }

  @Test
  public void test_getWorkers()
  {
    final List<IndexingWorkerInfo> workers = run(OverlordClient::getWorkers);
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(10, workers.get(0).getWorker().getCapacity());
  }

  @Test(timeout = 60_000L)
  public void test_run300Tasks()
  {
    runTasks(300);
  }

  @Test(timeout = 60_000L)
  public void test_run20Tasks()
  {
    runTasks(20);
  }

  @Test
  public void test_runBatchTask()
  {
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
                new InlineInputSource("time,name,value\n2024,a,1"),
                new CsvInputFormat(null, null, null, true, 0, false),
                false,
                false
            ),
            null
        ),
        Map.of()
    );
    run(
        client -> client.runTask(taskId, task)
    );

    OVERLORD.waitUntilTaskFinishes(taskId);
    verifyTaskHasSucceeded(taskId);
  }

  private void runTasks(int count)
  {
    final List<String> taskIds = IntStream.range(0, count)
                                          .mapToObj(i -> IdUtils.newTaskId("sim_test", "noop", null))
                                          .collect(Collectors.toList());

    for (String taskId : taskIds) {
      run(
          client -> client.runTask(
              taskId,
              new NoopTask(taskId, null, null, 1L, 0L, Map.of())
          )
      );
    }

    for (String taskId : taskIds) {
      OVERLORD.waitUntilTaskFinishes(taskId);
      verifyTaskHasSucceeded(taskId);
    }
  }

  private void verifyTaskHasSucceeded(String taskId)
  {
    final TaskStatusResponse currentStatus = run(
        client -> client.taskStatus(taskId)
    );
    Assert.assertNotNull(currentStatus.getStatus());
    Assert.assertEquals(
        StringUtils.format("Task[%s] has failed", taskId),
        TaskState.SUCCESS,
        currentStatus.getStatus().getStatusCode()
    );
  }


  private <T> T run(Function<OverlordClient, ListenableFuture<T>> function)
  {
    return FutureUtils.getUnchecked(function.apply(OVERLORD.client()), true);
  }
}
