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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.hrtr.WorkerHolder;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.WorkerHistoryItem;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Local worker used by {@link EmbeddedOverlord} that runs tasks on a thread pool
 * instead of sending them over HTTP to a middle manager or indexer.
 * The size of the thread pool is dictated by the {@link Worker#getCapacity()}.
 */
public class LocalWorkerHolder extends WorkerHolder
{
  private static final Logger log = new Logger(LocalWorkerHolder.class);

  private final TaskToolboxFactory taskToolboxFactory;
  private final ScheduledExecutorService taskExecutor;
  private final ConcurrentHashMap<String, RunningTask> runningTasks;

  /**
   * Used only to create a task location for a running task.
   */
  private final AtomicInteger taskDummyPort = new AtomicInteger(0);
  private final ChangeRequestHttpSyncer.Listener<WorkerHistoryItem> listener;

  public LocalWorkerHolder(
      ScheduledExecutorFactory executorFactory,
      TaskToolboxFactory taskToolboxFactory,
      ObjectMapper smileMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      Listener listener,
      Worker worker,
      List<TaskAnnouncement> knownAnnouncements
  )
  {
    super(smileMapper, httpClient, config, workersSyncExec, listener, worker, knownAnnouncements);

    this.taskExecutor = executorFactory.create(worker.getCapacity(), "TaskThread-%s");
    this.taskToolboxFactory = taskToolboxFactory;
    this.listener = createSyncListener();
    this.runningTasks = new ConcurrentHashMap<>();

    // Do a full sync to mark the worker as enabled
    this.listener.fullSync(
        List.of(new WorkerHistoryItem.Metadata(false))
    );
  }

  @Override
  protected ChangeRequestHttpSyncer<WorkerHistoryItem> createChangeRequestSyncer(
      ScheduledExecutorService workersSyncExec
  )
  {
    // Create a noop syncer since we just need the listener to notify changes to the task runner
    return new ChangeRequestHttpSyncer<>(
        null,
        null,
        null,
        TaskRunnerUtils.makeWorkerURL(getWorker(), "/"),
        null,
        null,
        0L,
        0L,
        null
    )
    {
      @Override
      public void start()
      {
      }

      @Override
      public void stop()
      {
      }

      @Override
      public boolean isInitialized()
      {
        return true;
      }

      @Override
      public boolean awaitInitialization()
      {
        return true;
      }

      @Override
      public Map<String, Object> getDebugInfo()
      {
        return Map.of();
      }

      @Override
      public boolean needsReset()
      {
        return false;
      }
    };
  }

  @Override
  public boolean assignTask(Task task)
  {
    if (runningTasks.size() >= getWorker().getCapacity()) {
      return false;
    }

    runningTasks.computeIfAbsent(
        task.getId(),
        t -> new RunningTask(
            taskExecutor.submit(() -> runTask(task, taskDummyPort.incrementAndGet()))
        )
    );
    return true;
  }

  @Override
  public void shutdownTask(String taskId)
  {
    runningTasks.compute(
        taskId,
        (t, entry) -> {
          if (entry != null && !entry.future.isDone()) {
            entry.future.cancel(true);
          }

          // Remove this entry from runningTasks
          return null;
        }
    );
    listener.deltaSync(
        List.of(new WorkerHistoryItem.TaskRemoval(taskId))
    );
  }

  @Override
  public void stop()
  {
    taskExecutor.shutdownNow();

    final Set<String> taskIds = Set.copyOf(runningTasks.keySet());
    for (String taskId : taskIds) {
      shutdownTask(taskId);
    }
    runningTasks.clear();
  }

  /**
   * Runs the given task. This method must be invoked on the {@link #taskExecutor}.
   */
  private TaskStatus runTask(Task task, int port)
  {
    final TaskLocation location = createLocation(port);
    final String taskId = task.getId();
    listener.deltaSync(
        List.of(
            new WorkerHistoryItem.TaskUpdate(
                TaskAnnouncement.create(task, TaskStatus.running(taskId), location)
            )
        )
    );

    TaskStatus status = null;
    try {
      status = task.run(taskToolboxFactory.build(task));
    }
    catch (Exception e) {
      log.error(e, "Error while running task[%s]", taskId);
      status = TaskStatus.failure(taskId, e.getMessage());
    }
    finally {
      listener.deltaSync(
          List.of(
              new WorkerHistoryItem.TaskUpdate(
                  TaskAnnouncement.create(task, status, location)
              )
          )
      );
      shutdownTask(taskId);
    }

    return status;
  }

  private TaskLocation createLocation(int port)
  {
    return TaskLocation.create(getWorker().getHost(), port, port);
  }

  /**
   * Entry for a task currently running on this worker.
   */
  private static class RunningTask
  {
    private final Future<TaskStatus> future;

    private RunningTask(Future<TaskStatus> future)
    {
      this.future = future;
    }
  }
}
