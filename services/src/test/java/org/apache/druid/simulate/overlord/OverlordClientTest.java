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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.http.SegmentsToUpdateFilter;
import org.apache.druid.simulate.EmbeddedDruidCluster;
import org.apache.druid.simulate.EmbeddedIndexer;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;

/**
 * Tests all the REST APIs exposed by the Overlord using the
 * {@link org.apache.druid.rpc.indexing.OverlordClient}.
 */
public class OverlordClientTest
{
  private static final String UNKNOWN_TASK_ID
      = IdUtils.newTaskId("sim_test_noop", "dummy", null);
  private static final String UNKNOWN_TASK_ERROR
      = StringUtils.format("Cannot find any task with id: [%s]", UNKNOWN_TASK_ID);

  private static final EmbeddedOverlord OVERLORD = EmbeddedOverlord.create();

  @ClassRule
  public static final RuleChain cluster
      = EmbeddedDruidCluster.builder()
                            .with(EmbeddedIndexer.create())
                            .with(OVERLORD)
                            .withDb()
                            .build();

  @Test
  public void test_findCurrentLeader()
  {
    URI currentLeader = getResult(OVERLORD.client().findCurrentLeader());
    Assert.assertEquals(8090, currentLeader.getPort());
  }

  @Test
  public void test_runTask_ofTypeNoop()
  {
    final String taskId = IdUtils.newTaskId("sim_test_noop", TestDataSource.WIKI, null);
    getResult(
        OVERLORD.client().runTask(taskId, new NoopTask(taskId, null, null, 1L, 0L, null))
    );

    verifyTaskHasSucceeded(taskId);
  }

  @Test
  public void test_runKillTask()
  {
    final String taskId = getResult(
        OVERLORD.client().runKillTask("sim_test", TestDataSource.WIKI, Intervals.ETERNITY, null, null, null)
    );
    verifyTaskHasSucceeded(taskId);
  }

  @Test
  public void test_cancelTask_withUnknownTaskId()
  {
    verifyFailsWith(
        OVERLORD.client().cancelTask(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  public void test_taskStatuses_all()
  {
    CloseableIterator<TaskStatusPlus> result = getResult(
        OVERLORD.client().taskStatuses(null, null, null)
    );
    assertNotNull(result);
  }

  @Test
  public void test_taskStatuses_byIds_returnsEmpty_forUnknownTaskIds()
  {
    Map<String, TaskStatus> result = getResult(
        OVERLORD.client().taskStatuses(Set.of(UNKNOWN_TASK_ID))
    );
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void test_taskStatus_fails_forUnknownTaskId()
  {
    verifyFailsWith(
        OVERLORD.client().taskStatus(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  public void test_taskPayload_fails_forUnknownTaskId()
  {
    verifyFailsWith(
        OVERLORD.client().taskPayload(UNKNOWN_TASK_ID),
        UNKNOWN_TASK_ERROR
    );
  }

  @Test
  public void test_supervisorStatuses()
  {
    CloseableIterator<SupervisorStatus> result = getResult(
        OVERLORD.client().supervisorStatuses()
    );
    assertNotNull(result);
  }

  @Test
  public void test_findLockedIntervals_fails_whenNoFilter()
  {
    verifyFailsWith(
        OVERLORD.client().findLockedIntervals(List.of()),
        "No filter provided"
    );
  }

  @Test
  public void test_killPendingSegments()
  {
    Integer numPendingSegmentsDeleted = getResult(
        OVERLORD.client().killPendingSegments(TestDataSource.WIKI, Intervals.ETERNITY)
    );
    Assert.assertEquals(0, numPendingSegmentsDeleted.intValue());
  }

  @Test
  public void test_getWorkers()
  {
    List<IndexingWorkerInfo> workers = getResult(OVERLORD.client().getWorkers());
    Assert.assertEquals(1, workers.size());
    Assert.assertEquals(3, workers.get(0).getWorker().getCapacity());
  }

  @Test
  public void test_getTotalWorkerCapacity()
  {
    IndexingTotalWorkerCapacityInfo result = getResult(
        OVERLORD.client().getTotalWorkerCapacity()
    );
    Assert.assertEquals(3, result.getCurrentClusterCapacity());
  }

  @Test
  public void test_isCompactionSupervisorEnabled()
  {
    Boolean result = getResult(OVERLORD.client().isCompactionSupervisorEnabled());
    assertNotNull(result);
  }

  @Test
  public void test_markNonOvershadowedSegmentsAsUsed_basic()
  {
    SegmentUpdateResponse result = getResult(OVERLORD.client().markNonOvershadowedSegmentsAsUsed(TestDataSource.WIKI));
    assertNotNull(result);
  }

  @Test
  public void test_markNonOvershadowedSegmentsAsUsed_filtered()
  {
    SegmentUpdateResponse result = getResult(
        OVERLORD.client().markNonOvershadowedSegmentsAsUsed(
            TestDataSource.WIKI,
            new SegmentsToUpdateFilter(Intervals.ETERNITY, null, null)
        )
    );
    assertNotNull(result);
  }

  @Test
  public void test_markSegmentAsUsed()
  {
    SegmentUpdateResponse result = getResult(
        OVERLORD.client().markSegmentAsUsed(SegmentId.dummy(TestDataSource.WIKI))
    );
    assertNotNull(result);
  }

  @Test
  public void test_markSegmentsAsUnused_basic()
  {
    final SegmentUpdateResponse result = getResult(
        OVERLORD.client().markSegmentsAsUnused(TestDataSource.WIKI)
    );
    assertNotNull(result);
  }

  @Test
  public void test_markSegmentsAsUnused_filtered()
  {
    SegmentUpdateResponse result = getResult(
        OVERLORD.client().markSegmentsAsUnused(
            TestDataSource.WIKI,
            new SegmentsToUpdateFilter(Intervals.ETERNITY, null, null)
        )
    );
    assertNotNull(result);
  }

  @Test
  public void test_markSegmentAsUnused()
  {
    SegmentUpdateResponse result = getResult(
        OVERLORD.client().markSegmentAsUnused(
            SegmentId.dummy(TestDataSource.WIKI)
        )
    );
    assertNotNull(result);
  }

  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }

  private static <T> void verifyFailsWith(ListenableFuture<T> future, String message)
  {
    final CountDownLatch isFutureDone = new CountDownLatch(1);
    final AtomicReference<Throwable> capturedError = new AtomicReference<>();
    Futures.addCallback(
        future,
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(T result)
          {
            isFutureDone.countDown();
          }

          @Override
          public void onFailure(Throwable t)
          {
            capturedError.set(t);
            isFutureDone.countDown();
          }
        },
        MoreExecutors.directExecutor()
    );

    try {
      isFutureDone.await();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    Assert.assertNotNull(capturedError.get());
    Assert.assertTrue(capturedError.get().getMessage().contains(message));
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
}
