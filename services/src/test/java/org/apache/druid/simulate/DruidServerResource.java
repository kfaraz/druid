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

package org.apache.druid.simulate;

import com.amazonaws.util.Throwables;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.utils.JvmUtils;
import org.apache.druid.utils.RuntimeInfo;
import org.junit.rules.ExternalResource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JUnit {@link ExternalResource} for an {@link EmbeddedDruidServer} server.
 * This class can be used with {@code Rule}, {@code ClassRule} or a {@code RuleChain}.
 */
public class DruidServerResource extends ExternalResource
{
  private static final Logger log = new Logger(DruidServerResource.class);

  private final EmbeddedDruidServer server;
  private final EmbeddedZookeeper zk;
  private final TestDerbyConnector.DerbyConnectorRule dbRule;

  private ExecutorService executorService;
  private final AtomicReference<Lifecycle> lifecycle = new AtomicReference<>();

  DruidServerResource(
      EmbeddedDruidServer server,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  )
  {
    this.server = server;
    this.zk = zk;
    this.dbRule = dbRule;
  }

  @Override
  protected void before() throws Throwable
  {
    log.info("Starting server[%s] ...", server.getName());

    // Create and start the ServerRunnable
    final CountDownLatch lifecycleCreated = new CountDownLatch(1);
    final ServerRunnable serverRunnable = server.createRunnable(
        new EmbeddedDruidServer.LifecycleInitHandler()
        {
          @Override
          public List<? extends Module> getInitModules()
          {
            return dbRule == null ? List.of() : List.of(new TestDerbyModule(dbRule.getConnector()));
          }

          @Override
          public void onLifecycleInit(Lifecycle lifecycle)
          {
            lifecycleCreated.countDown();
            DruidServerResource.this.lifecycle.set(lifecycle);
          }
        }
    );

    executorService = Execs.multiThreaded(1, "Lifecycle-EmbeddedServer-" + server.getName());
    executorService.submit(() -> runServer(serverRunnable));

    // Wait for lifecycle to be created and started
    lifecycleCreated.await();
    awaitLifecycleStart();
  }

  @Override
  protected void after()
  {
    log.info("Stopping server[%s] ...", server.getName());

    lifecycle.get().stop();
    lifecycle.set(null);
    executorService.shutdownNow();
    executorService = null;
  }

  /**
   * Waits for the server lifecycle created in {@link #before()} to start.
   */
  private void awaitLifecycleStart()
  {
    try {
      final CountDownLatch started = new CountDownLatch(1);

      lifecycle.get().addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start()
            {
              started.countDown();
            }

            @Override
            public void stop()
            {

            }
          }
      );

      started.await();
      log.info("Server[%s] is now running.", server.getName());
    }
    catch (Exception e) {
      log.error(e, "Exception while waiting for server[%s] to start.", server.getName());
    }
  }

  /**
   * Runs the server. This method must be invoked on {@link #executorService},
   * as it blocks and does not return until {@link #executorService} is terminated.
   */
  private void runServer(ServerRunnable runnable)
  {
    try {
      final Properties serverProperties = new Properties();
      serverProperties.putAll(server.getProperties());

      // Add properties for Zookeeper and metadata store
      serverProperties.setProperty("druid.zk.service.host", zk.getConnectString());
      if (dbRule != null) {
        serverProperties.setProperty("druid.metadata.storage.type", TestDerbyModule.TYPE);
        serverProperties.setProperty(
            "druid.metadata.storage.tables.base",
            dbRule.getConnector().getMetadataTablesConfig().getBase()
        );
      }

      final Injector injector = new StartupInjectorBuilder()
          .withProperties(serverProperties)
          .add(new TestRuntimeInfoModule())
          .build();

      injector.injectMembers(runnable);
      runnable.run();
    }
    catch (Exception e) {
      Throwable rootCause = Throwables.getRootCause(e);
      if (rootCause instanceof InterruptedException) {
        log.warn("Interrupted while running server[%s].", server.getName());
      } else {
        log.error(e, "Error while running server[%s]", server.getName());
      }
    }
    finally {
      log.info("Stopped server[%s].", server.getName());
    }
  }

  /**
   * Module to limit the resources used by an embedded server.
   */
  private static class TestRuntimeInfoModule implements Module
  {
    static final long XMX_1_GB = 1_000_000_000;
    static final int NUM_PROCESSORS = 4;

    @Override
    public void configure(Binder binder)
    {
      binder.bind(RuntimeInfo.class).toInstance(
          new DruidProcessingConfigTest.MockRuntimeInfo(NUM_PROCESSORS, XMX_1_GB, XMX_1_GB)
      );
      binder.requestStaticInjection(JvmUtils.class);
    }
  }

}
