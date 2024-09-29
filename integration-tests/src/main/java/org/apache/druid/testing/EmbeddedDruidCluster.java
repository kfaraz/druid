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

package org.apache.druid.testing;

import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.cli.CliBroker;
import org.apache.druid.cli.CliCoordinator;
import org.apache.druid.cli.CliHistorical;
import org.apache.druid.cli.CliMiddleManager;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * TODO later:
 *    - allow different tests to use different configs
 *    - see if the config can somehow be generated from the docker config
 *    - Put only as much effort as is required for migration to new ITs, no more
 *  TODO now:
 *    - reduce coordinator logs
 *    - print a summary every 30 mins
 *    - raise an alert if anything is amiss
 *    - run some new ITs with embedded cluster
 *    - understand the new IT framework
 *    - write up a comparison of the two frameworks
 *    - merge the two frameworks into one
 *    - migrate all tests to the unified framework
 *    - why does coordinator + indexer not stop immediately?
 */
public class EmbeddedDruidCluster
{
  private static final Logger log = new Logger(EmbeddedDruidCluster.class);

  private final ExecutorService executorService;
  private final EmbeddedZookeeper zookeeper;

  @Inject
  public EmbeddedDruidCluster()
  {
    this.zookeeper = new EmbeddedZookeeper();
    this.executorService = Execs.multiThreaded(5, "druid-node-%s");
  }

  @LifecycleStart
  public void start()
  {
    startEmbeddedZookeeper();
    startServers();
  }

  @LifecycleStop
  public void stop()
  {
    stopServers();
    stopEmbeddedZookeeper();
  }

  private void startServers()
  {
    log.info("Starting embedded Druid cluster ...");
    try {
      final Properties commonProps = createCommonProperties();

      executorService.submit(() -> runServer("c1", new CliCoordinator(), commonProps));
      executorService.submit(() -> runServer("b1", new CliBroker(), commonProps));
      executorService.submit(() -> runServer("m1", new CliMiddleManager(), commonProps));
      executorService.submit(() -> runServer("h1", new CliHistorical(), commonProps));
    }
    catch (Exception e) {
      log.error(e, "Error while starting embedded Druid cluster");
    }
  }

  private void stopServers()
  {
    log.info("Stopping embedded Druid cluster ...");
    try {
      executorService.shutdownNow();
      executorService.awaitTermination(30, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      log.error(e, "Error while stopping embedded cluster executor");
    }
  }

  private void startEmbeddedZookeeper()
  {
    try {
      zookeeper.setupServerAndCurator();
    }
    catch (Exception e) {
      log.error(e, "Error while starting embedded zookeeper");
    }
  }

  private void stopEmbeddedZookeeper()
  {
    try {
      zookeeper.tearDownServerAndCurator();
    }
    catch (Exception e) {
      log.error(e, "Error while stopping embedded zookeeper");
    }
    finally {
      log.info("Stopped embedded zookeeper");
    }
  }

  public String getZkConnectionString()
  {
    return zookeeper.getConnectionString();
  }

  private Properties createCommonProperties()
  {
    final Properties commonProps = new Properties();
    commonProps.setProperty("druid.zk.service.host", zookeeper.getConnectionString());

    commonProps.setProperty(
        "druid.segmentCache.locations",
        "[{\"path\":\"var/druid/segment-cache\",\"maxSize\":\"5g\"}]"
    );
    commonProps.setProperty(
        "druid.metadata.storage.connector.connectURI",
        "jdbc:derby://localhost:1527/var/druid/metadata.db;create=true"
    );

    commonProps.setProperty("druid.lookup.lookupStartRetries", "1");
    commonProps.setProperty("druid.lookup.coordinatorFetchRetries", "1");
    commonProps.setProperty("druid.processing.buffer.sizeBytes", "1MiB");

    final URL trustStoreUrl = EmbeddedDruidCluster.class.getClassLoader().getResource("truststore.jks");
    commonProps.setProperty("druid.emitter.http.ssl.trustStorePath", trustStoreUrl.getPath());
    commonProps.setProperty("druid.client.https.trustStorePath", trustStoreUrl.getPath());

    commonProps.setProperty("druid.indexer.logs.directory", "var/log");

    commonProps.setProperty("druid.coordinator.startDelay", "PT1S");
    commonProps.setProperty("druid.coordinator.asOverlord.enabled", "true");
    commonProps.setProperty("druid.coordinator.asOverlord.overlordService", "druid/overlord");
    commonProps.setProperty("druid.coordinator.period", "PT5S");

    return commonProps;
  }

  private void runServer(String serverName, ServerRunnable runnable, Properties properties)
  {
    try {
      log.info("Starting server[%s].", serverName);
      final Injector injector
          = new StartupInjectorBuilder().forEmbeddedTestServer().withProperties(properties).build();
      injector.injectMembers(runnable);
      runnable.run();
    }
    catch (Exception e) {
      log.noStackTrace().error(e, "Error while running server[%s].", serverName);
    }
    finally {
      log.info("Stopped server[%s].", serverName);
    }
  }

  /**
   * Extends CuratorTestBase to allow access to protected methods.
   */
  private static class EmbeddedZookeeper extends CuratorTestBase
  {
    @Override
    protected void setupServerAndCurator() throws Exception
    {
      super.setupServerAndCurator();
    }

    @Override
    protected void tearDownServerAndCurator()
    {
      super.tearDownServerAndCurator();
    }

    String getConnectionString()
    {
      return server.getConnectString();
    }
  }
}
