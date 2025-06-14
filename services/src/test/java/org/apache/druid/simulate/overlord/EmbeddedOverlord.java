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
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.cli.CliOverlord;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexer.report.TaskReportFileWriter;
import org.apache.druid.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.WorkerHolder;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.simulate.EmbeddedDruidServer;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Embedded Overlord service used in simulation tests.
 */
public class EmbeddedOverlord extends EmbeddedDruidServer
{
  private static final Logger log = new Logger(EmbeddedOverlord.class);

  private static final Map<String, String> STANDARD_PROPERTIES = Map.of(
      "druid.indexer.runner.type", "threading",
      "druid.indexer.queue.startDelay", "PT0S",
      "druid.indexer.queue.restartDelay", "PT0S"
  );

  private final TaskRunnerListener taskRunnerListener;
  private final OverlordClientReference clientReference;
  private final ConcurrentHashMap<String, CountDownLatch> taskHasCompleted;

  public static EmbeddedOverlord create()
  {
    return withProps(Map.of());
  }

  public static EmbeddedOverlord withProps(
      Map<String, String> properties
  )
  {
    final Map<String, String> overrideProps = new HashMap<>(STANDARD_PROPERTIES);
    overrideProps.putAll(properties);

    return new EmbeddedOverlord(overrideProps);
  }

  private EmbeddedOverlord(Map<String, String> serverProperties)
  {
    super("Overlord", serverProperties);
    this.clientReference = new OverlordClientReference();
    this.taskHasCompleted = new ConcurrentHashMap<>();
    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "TestRunnerFactory";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {

      }

      @Override
      public void statusChanged(String taskId, TaskStatus status)
      {
        if (status.isComplete()) {
          taskHasCompleted.compute(
              taskId,
              (t, existingLatch) -> {
                final CountDownLatch latch = Objects.requireNonNullElse(
                    existingLatch,
                    new CountDownLatch(1)
                );
                latch.countDown();
                return latch;
              }
          );
        }
      }
    };
  }

  @Override
  protected ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Overlord(handler);
  }

  /**
   * Client to communicate with this Overlord.
   */
  public OverlordClient client()
  {
    return clientReference.client;
  }

  public void waitUntilTaskFinishes(String taskId)
  {
    try {
      final CountDownLatch latch = taskHasCompleted.computeIfAbsent(taskId, t -> new CountDownLatch(1));
      if (!latch.await(10, TimeUnit.SECONDS)) {
        log.error("Timed out waiting for task[%s] to finish.", taskId);
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extends {@link CliOverlord} to allow passing extra modules, properties and
   * getting a reference to the lifecycle.
   */
  private class Overlord extends CliOverlord
  {
    private final LifecycleInitHandler handler;

    private Overlord(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(handler.getInitModules());
      modules.addAll(super.getModules());
      modules.add(
          binder -> binder.bind(TaskToolboxFactory.class).to(TestTaskToolboxFactory.class)
      );
      modules.add(
          binder -> binder.bind(TaskReportFileWriter.class).toInstance(
              new MultipleFileTaskReportFileWriter()
              {
                @Override
                public void write(String id, TaskReport.ReportMap reports)
                {
                }

                @Override
                public OutputStream openReportOutputStream(String taskId)
                {
                  // Stream to nowhere.
                  return new ByteArrayOutputStream();
                }

                @Override
                public void setObjectMapper(ObjectMapper objectMapper)
                {

                }

                @Override
                public void add(String taskId, File reportsFile)
                {

                }
              }
          )
      );
      modules.add(
          binder -> binder.bind(OverlordClientReference.class).toInstance(clientReference)
      );
      modules.add(
          binder -> binder.bind(TaskRunnerListener.class).toInstance(taskRunnerListener)
      );
      modules.add(
          binder -> PolyBind.optionBinder(binder, Key.get(TaskRunnerFactory.class))
                            .addBinding("threading")
                            .to(TestHttpRemoteTaskRunnerFactory.class)
                            .in(LazySingleton.class)
      );
      return modules;
    }
  }

  private static class TestHttpRemoteTaskRunnerFactory
      extends HttpRemoteTaskRunner
      implements TaskRunnerFactory<HttpRemoteTaskRunner>
  {
    private final TaskToolboxFactory taskToolboxFactory;
    private final ScheduledExecutorFactory executorFactory;

    @Inject
    public TestHttpRemoteTaskRunnerFactory(
        @Smile ObjectMapper smileMapper,
        HttpRemoteTaskRunnerConfig config,
        @EscalatedGlobal HttpClient httpClient,
        Supplier<WorkerBehaviorConfig> workerConfigRef,
        ProvisioningStrategy provisioningStrategy,
        DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        TaskStorage taskStorage,
        @Nullable CuratorFramework cf,
        IndexerZkConfig indexerZkConfig,
        ServiceEmitter emitter,
        // Fields not needed by HttpRemoteTaskRunner but used by LocalWorkerHolder
        TaskRunnerListener listener,
        ScheduledExecutorFactory executorFactory,
        TaskToolboxFactory taskToolboxFactory
    )
    {
      super(
          smileMapper,
          config,
          httpClient,
          workerConfigRef,
          provisioningStrategy,
          druidNodeDiscoveryProvider,
          taskStorage,
          cf,
          indexerZkConfig,
          emitter
      );
      registerListener(listener, MoreExecutors.directExecutor());
      this.executorFactory = executorFactory;
      this.taskToolboxFactory = taskToolboxFactory;

      addWorker(
          new Worker(null, "LocalWorker", "007", 1000, "v1", WorkerConfig.DEFAULT_CATEGORY)
      );
    }

    @Override
    protected WorkerHolder createWorkerHolder(
        ObjectMapper smileMapper,
        HttpClient httpClient,
        HttpRemoteTaskRunnerConfig config,
        ScheduledExecutorService workersSyncExec,
        WorkerHolder.Listener listener,
        Worker worker,
        List<TaskAnnouncement> knownAnnouncements
    )
    {
      return new LocalWorkerHolder(
          executorFactory,
          taskToolboxFactory,
          smileMapper,
          httpClient,
          config,
          workersSyncExec,
          listener,
          worker,
          knownAnnouncements
      );
    }

    @Override
    public HttpRemoteTaskRunner build()
    {
      return this;
    }

    @Override
    public HttpRemoteTaskRunner get()
    {
      return this;
    }
  }

  private static class TestTaskToolboxFactory extends TaskToolboxFactory
  {
    private final TaskActionClientFactory taskActionClientFactory;
    private final TaskReportFileWriter taskReportFileWriter;
    private final ServiceEmitter serviceEmitter;
    private final TaskConfig taskConfig;
    private final ObjectMapper mapper;
    private final IndexIO indexIO;

    @Inject
    TestTaskToolboxFactory(
        TaskActionClientFactory taskActionClientFactory,
        TaskReportFileWriter taskReportFileWriter,
        ServiceEmitter serviceEmitter,
        TaskConfig taskConfig,
        ObjectMapper mapper,
        IndexIO indexIO
    )
    {
      super(
          null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null,
          indexIO, null, null, null, null, null, null, null, null, null, null,
          null, null, null, null, null, null, null, null, null, null, null
      );
      this.taskActionClientFactory = taskActionClientFactory;
      this.taskReportFileWriter = taskReportFileWriter;
      this.serviceEmitter = serviceEmitter;
      this.taskConfig = taskConfig;
      this.mapper = mapper;
      this.indexIO = indexIO;
    }

    @Override
    public TaskToolbox build(Task task)
    {
      return new TaskToolbox.Builder()
          .taskActionClient(taskActionClientFactory.create(task))
          .taskReportFileWriter(taskReportFileWriter)
          .indexIO(indexIO)
          .indexMergerV9(new IndexMergerV9(mapper, indexIO, TmpFileSegmentWriteOutMediumFactory.instance(), false))
          .config(taskConfig)
          .emitter(serviceEmitter)
          .build();
    }
  }

  private static class OverlordClientReference
  {
    private OverlordClient client;

    @Inject
    void setOverlordClient(OverlordClient client)
    {
      this.client = client;
    }
  }
}
