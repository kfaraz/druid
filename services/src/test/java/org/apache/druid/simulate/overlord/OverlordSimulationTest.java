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
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.simulate.EmbeddedDruidCluster;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TODO:
 * - run a batch task
 * - try to run a streaming supervisor with this setup
 * - try to run existing tests with this setup
 * - write more tests
 */
public class OverlordSimulationTest
{
  private final EmbeddedOverlord overlord = EmbeddedOverlord.create();

  @Rule
  public final RuleChain cluster = EmbeddedDruidCluster.builder().withServer(overlord).withDb().build();

  @Test
  public void test_run5Tasks()
  {
    final List<String> taskIds = IntStream.range(0, 5)
                                          .mapToObj(i -> "test-task-" + i)
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
      overlord.waitUntilTaskFinishes(taskId);
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
  }

  private <T> T run(Function<OverlordClient, ListenableFuture<T>> function)
  {
    return FutureUtils.getUnchecked(function.apply(overlord.client()), true);
  }
}
