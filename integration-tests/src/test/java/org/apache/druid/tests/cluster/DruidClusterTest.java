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

package org.apache.druid.tests.cluster;

import com.google.inject.Injector;
import org.apache.druid.cli.CliBroker;
import org.apache.druid.cli.CliCoordinator;
import org.apache.druid.cli.CliHistorical;
import org.apache.druid.cli.CliMiddleManager;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class DruidClusterTest extends CuratorTestBase
{
  private static final Logger log = new Logger(DruidClusterTest.class);
  private ExecutorService executorService;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();

    final Properties commonProps = new Properties();
    System.out.printf("Kashif: Zookeeper connection string is: %s%n", server.getConnectString());
    commonProps.setProperty("druid.zk.service.host", server.getConnectString());

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

    final URL trustStoreUrl = DruidClusterTest.class.getClassLoader().getResource("truststore.jks");
    commonProps.setProperty("druid.emitter.http.ssl.trustStorePath", trustStoreUrl.getPath());
    commonProps.setProperty("druid.client.https.trustStorePath", trustStoreUrl.getPath());

    commonProps.setProperty("druid.indexer.logs.directory", "var/log");

    commonProps.setProperty("druid.coordinator.startDelay", "PT1S");
    commonProps.setProperty("druid.coordinator.asOverlord.enabled", "true");
    commonProps.setProperty("druid.coordinator.asOverlord.overlordService", "druid/overlord");
    commonProps.setProperty("druid.coordinator.period", "PT5S");

    executorService = Execs.multiThreaded(5, "druid-node-%s");

    executorService.submit(() -> runServer("c1", new CliCoordinator(), commonProps));
    // executorService.submit(() -> runServer("router"));
    executorService.submit(() -> runServer("b1", new CliBroker(), commonProps));
    // executorService.submit(() -> runServer("i1", new CliIndexer(), commonProps));
    executorService.submit(() -> runServer("m1", new CliMiddleManager(), commonProps));
    executorService.submit(() -> runServer("h1", new CliHistorical(), commonProps));
  }

  private void runServer(String serverName, ServerRunnable runnable, Properties properties)
  {
    try {
      log.info("Starting server[%s].", serverName);
      final Injector injector
          = new StartupInjectorBuilder().forEmbeddedTestServer().withProperties(properties).build();
      injector.injectMembers(runnable);
      runnable.run();
    } catch (Exception e) {
      log.error("Error while running server[%s].", serverName);
    } finally {
      log.info("Stopped server[%s].", serverName);
    }
  }

  @After
  public void tearDown() throws InterruptedException
  {
    executorService.shutdownNow();
    executorService.awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  public void testServerRun() throws Exception
  {
    Thread.sleep(60 * 60_000);
    System.out.println("Kashif: tests are done");
  }
}
