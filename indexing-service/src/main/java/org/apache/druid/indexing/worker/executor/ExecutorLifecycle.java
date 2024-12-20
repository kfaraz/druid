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

package org.apache.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.metadata.PasswordProviderRedactionMixIn;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

/**
 * Encapsulates the lifecycle of a task executor. Loads one task, runs it, writes its status, and all the while
 * monitors its parent process.
 */
public class ExecutorLifecycle
{
  private static final EmittingLogger log = new EmittingLogger(ExecutorLifecycle.class);

  private final ExecutorLifecycleConfig taskExecutorConfig;
  private final TaskConfig taskConfig;
  private final TaskActionClientFactory taskActionClientFactory;
  private final TaskRunner taskRunner;
  private final ObjectMapper jsonMapper;

  private final ExecutorService parentMonitorExec = Execs.singleThreaded("parent-monitor-%d");

  private volatile Task task = null;
  private volatile ListenableFuture<TaskStatus> statusFuture = null;
  private volatile FileChannel taskLockChannel;
  private volatile FileLock taskLockFileLock;

  @Inject
  public ExecutorLifecycle(
      ExecutorLifecycleConfig taskExecutorConfig,
      TaskConfig taskConfig,
      TaskActionClientFactory taskActionClientFactory,
      TaskRunner taskRunner,
      ObjectMapper jsonMapper
  )
  {
    this.taskExecutorConfig = taskExecutorConfig;
    this.taskConfig = taskConfig;
    this.taskActionClientFactory = taskActionClientFactory;
    this.taskRunner = taskRunner;
    this.jsonMapper = jsonMapper.copy().addMixIn(PasswordProvider.class, PasswordProviderRedactionMixIn.class);
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    final File taskFile = Preconditions.checkNotNull(taskExecutorConfig.getTaskFile(), "taskFile");
    final File statusFile = Preconditions.checkNotNull(taskExecutorConfig.getStatusFile(), "statusFile");
    final InputStream parentStream = Preconditions.checkNotNull(taskExecutorConfig.getParentStream(), "parentStream");

    try {
      task = jsonMapper.readValue(taskFile, Task.class);

      log.info(
          "Running with task: %s",
          jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task)
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    // if the parent stream is not defined, like running a k8s peon task, it will never run on the same
    // pod twice, no need to lock.
    if (taskExecutorConfig.isParentStreamDefined()) {
      // Avoid running the same task twice on the same machine by locking the task base directory.
      final File taskLockFile = taskConfig.getTaskLockFile(task.getId());
      try {
        synchronized (this) {
          if (taskLockChannel == null && taskLockFileLock == null) {
            taskLockChannel = FileChannel.open(
                taskLockFile.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
            );

            log.info("Attempting to lock file[%s].", taskLockFile);
            final long startLocking = System.currentTimeMillis();
            final long timeout = DateTimes.utc(startLocking).plus(taskConfig.getDirectoryLockTimeout()).getMillis();
            while (taskLockFileLock == null && System.currentTimeMillis() < timeout) {
              taskLockFileLock = taskLockChannel.tryLock();
              if (taskLockFileLock == null) {
                Thread.sleep(100);
              }
            }

            if (taskLockFileLock == null) {
              throw new ISE("Could not acquire lock file[%s] within %,dms.", taskLockFile, timeout - startLocking);
            } else {
              log.info("Acquired lock file[%s] in %,dms.", taskLockFile, System.currentTimeMillis() - startLocking);
            }
          } else {
            throw new ISE("Already started!");
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      // Spawn monitor thread to keep a watch on parent's stdin
      // If stdin reaches eof, the parent is gone, and we should shut down
      parentMonitorExec.submit(
          () -> {
            try {
              while (parentStream.read() != -1) {
                // Toss the byte
              }
            }
            catch (Exception e) {
              log.error(e, "Failed to read from stdin");
            }

            // Kind of gross, but best way to kill the JVM as far as I know
            log.info("Triggering JVM shutdown. Check overlord logs to see why the task is being shut down.");
            System.exit(2);
          }
      );
    }


    // Won't hurt in remote mode, and is required for setting up locks in local mode:
    try {
      if (!task.isReady(taskActionClientFactory.create(task))) {
        throw new ISE("Task[%s] is not ready to run yet!", task.getId());
      }
    }
    catch (Exception e) {
      throw new ISE(e, "Failed to run task[%s] isReady", task.getId());
    }

    statusFuture = Futures.transform(
        taskRunner.run(task),
        new Function<>()
        {
          @Override
          public TaskStatus apply(TaskStatus taskStatus)
          {
            try {
              log.info(
                  "Task completed with status: %s",
                  jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(taskStatus)
              );

              final File statusFileParent = statusFile.getParentFile();
              if (statusFileParent != null) {
                FileUtils.mkdirp(statusFileParent);
              }
              jsonMapper.writeValue(statusFile, taskStatus);

              return taskStatus;
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        },
        MoreExecutors.directExecutor()
    );
  }

  public void join()
  {
    try {
      statusFuture.get();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @LifecycleStop
  public void stop() throws Exception
  {
    parentMonitorExec.shutdown();

    synchronized (this) {
      if (taskLockFileLock != null) {
        taskLockFileLock.release();
      }

      if (taskLockChannel != null) {
        taskLockChannel.close();
      }
    }
  }
}
