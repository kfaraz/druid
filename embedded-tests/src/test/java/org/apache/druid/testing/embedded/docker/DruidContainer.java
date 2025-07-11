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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.DruidDocker;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * TODO:
 * - add extensions
 * - add common properties? such as using a specific emitter or minio storage stuff
 * - how to watch events
 */
public class DruidContainer extends TestcontainerResource<DruidContainer.ContainerImpl>
{
  private static final Logger log = new Logger(DruidContainer.class);

  private static final String BASE_MOUNT_DIR = "target/embedded/";

  private static final String APACHE_DRUID_33 = "apache/druid:33.0.0";

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final Map<String, String> properties = new HashMap<>();
  private String imageName = APACHE_DRUID_33;

  private final String name;
  private final NodeRole nodeRole;
  private final String baseContainerDirPath;

  private int port;

  private EmbeddedDruidCluster cluster;

  DruidContainer(NodeRole nodeRole, int port)
  {
    this.name = StringUtils.format(
        "%s-%s-%d",
        getClass().getSimpleName(),
        nodeRole.getJsonName(),
        SERVER_ID.incrementAndGet()
    );
    this.nodeRole = nodeRole;
    this.port = port;
    this.baseContainerDirPath = BASE_MOUNT_DIR + name;
  }

  public DruidContainer withPort(int port)
  {
    this.port = port;
    return this;
  }

  /**
   * Uses the {@link #APACHE_DRUID_33} image for this container.
   */
  public DruidContainer withApache33Image()
  {
    this.imageName = APACHE_DRUID_33;
    return this;
  }

  /**
   * Uses the docker test image specified by the system property
   * {@link DruidDocker#PROPERTY_TEST_IMAGE} for this container.
   */
  public DruidContainer withDockerTestImage()
  {
    this.imageName = Objects.requireNonNull(
        System.getProperty(DruidDocker.PROPERTY_TEST_IMAGE),
        StringUtils.format("System property[%s] is not set", DruidDocker.PROPERTY_TEST_IMAGE)
    );
    return this;
  }

  public DruidContainer addProperty(String key, String value)
  {
    properties.put(key, value);
    return this;
  }

  public String getName()
  {
    return name;
  }

  public String getBaseMountDir()
  {
    return baseContainerDirPath;
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  @Override
  protected ContainerImpl createContainer()
  {
    // TODO: Just override the stuff that we don't need, rest should be okay
    addProperty(
        "druid.zk.service.host",
        cluster.getCommonProperty(DruidDocker.PROPERTY_ZK_CONNECT_STRING)
    );
    addProperty(
        "druid.metadata.storage.connector.connectURI",
        cluster.getCommonProperty(DruidDocker.PROPERTY_METADATA_STORE_CONNECT_URI)
    );
    addProperty(
        "druid.extensions.loadList",
        cluster.getCommonProperty("druid.extensions.loadList")
    );

    addProperty("druid.host", DruidDocker.hostMachine());
    addProperty("druid.plaintextPort", String.valueOf(port));

    log.info(
        "Starting Druid container[%s] on port[%d] with properties[%s]. Mounting directory[%s].",
        name, port, properties, baseContainerDirPath
    );

    mkdirpUnchecked(new File(baseContainerDirPath));
    mkdirpUnchecked(new File(baseContainerDirPath, "/shared/conf"));

    // Write out an empty common properties file for the time being
    // All needed properties will just go to the service specific properties file
    final String commonPropertiesFile = "conf/common.runtime.properties";
    writePropertiesToFile(
        new Properties(),
        baseContainerDirPath + "/shared/" + commonPropertiesFile
    );

    final String serverPropertiesFile = "conf/runtime.properties";
    final Properties serverProperties = new Properties();
    serverProperties.putAll(properties);
    writePropertiesToFile(
        serverProperties,
        baseContainerDirPath + "/shared/" + serverPropertiesFile
    );

    return new ContainerImpl(imageName, port)
        .withNetwork(Network.newNetwork())
        .withNetworkAliases(nodeRole.getJsonName())
        .withCommand(nodeRole.getJsonName())
        .withFileSystemBind(baseContainerDirPath + "/shared", "/opt/shared", BindMode.READ_WRITE)
        .withFileSystemBind(baseContainerDirPath + "/service", "/opt/druid/var", BindMode.READ_WRITE)
        .withFileSystemBind(baseContainerDirPath + "/log", "/opt/druid/log", BindMode.READ_WRITE)
        .withEnv(
            Map.of(
                "DRUID_CONFIG_COMMON",
                "/opt/shared/" + commonPropertiesFile,
                StringUtils.format("DRUID_CONFIG_%s", nodeRole.getJsonName()),
                "/opt/shared/" + serverPropertiesFile,
                "DRUID_SET_HOST_IP", "0"
            )
        )
        .withExposedPorts(port)
        .waitingFor(Wait.forHttp("/status/health").forPort(port));
  }

  @Override
  public String toString()
  {
    return name;
  }

  private static void writePropertiesToFile(Properties properties, String file)
  {
    try (FileOutputStream out = new FileOutputStream(file)) {
      properties.store(out, "Druid config");
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void mkdirpUnchecked(File dir)
  {
    try {
      FileUtils.mkdirp(dir);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Druid implementation of {@link GenericContainer}.
   */
  public static class ContainerImpl extends GenericContainer<ContainerImpl>
  {
    public ContainerImpl(String imageName, int port)
    {
      super(DockerImageName.parse(imageName));

      // The port needs to be bound statically (rather than using a mapped port)
      // so that we can set `druid.plaintextPort` and make this node discoverable
      // by other services (both embedded and dockerized)
      setPortBindings(
          List.of(StringUtils.format("%d:%d", port, port))
      );
    }
  }
}
