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

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.DruidImage;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * {@link TestcontainerResource} to run Druid services.
 * Depending on the image used, some extensions can be used out-of-the-box
 * such as {@code druid-s3-extensions} or {@code postgresql-metadata-storage},
 * simply by adding them to {@code druid.extensions.loadList}.
 * <p>
 * {@link DruidContainers} should be used only for testing backward compatiblity
 * or a Docker-specific feature. For all other testing needs, use plain old
 * {@code EmbeddedDruidServer} as they are much faster, allow easy debugging and
 * do not require downloading any images.
 */
public class DruidContainerResource extends TestcontainerResource<DruidContainer>
{
  /**
   * Java system property to specify the name of the Docker test image.
   */
  public static final String PROPERTY_TEST_IMAGE = "druid.testing.docker.image";

  private static final Logger log = new Logger(DruidContainerResource.class);

  /**
   * Forbidden server properties that may be used by EmbeddedDruidServers but
   * interfere with the functioning of DruidContainer-based services.
   */
  private static final Set<String> FORBIDDEN_PROPERTIES = Set.of(
      "druid.extensions.modulesForEmbeddedTests",
      "druid.emitter"
  );

  /**
   * A static incremental ID is used instead of a random number to ensure that
   * tests are more deterministic and easier to debug.
   */
  private static final AtomicInteger SERVER_ID = new AtomicInteger(0);

  private final String name;
  private final DruidCommand command;
  private final Map<String, String> properties = new HashMap<>();

  private int port;
  private String imageName = DruidImage._33_0_0.getName();
  private EmbeddedDruidCluster cluster;

  private String containerDirectory;

  private MountedDir clusterLogsDirectory;
  private MountedDir serviceLogsDirectory;
  private MountedDir deepStorageDirectory;
  private MountedDir clusterConfDirectory;

  DruidContainerResource(DruidCommand command, int port)
  {
    this.name = StringUtils.format(
        "container_%s_%d",
        command.getName(),
        SERVER_ID.incrementAndGet()
    );
    this.command = command;
    this.port = port;
  }

  public DruidContainerResource withPort(int port)
  {
    this.port = port;
    return this;
  }

  /**
   * Uses the {@link DruidImage#_33_0_0 Apache Druid 33.0.0 image} for this container.
   */
  public DruidContainerResource withApache33Image()
  {
    this.imageName = DruidImage._33_0_0.getName();
    return this;
  }

  /**
   * Uses the {@link DruidImage#_32_0_1 Apache Druid 32.0.1 image} for this container.
   */
  public DruidContainerResource withApache32Image()
  {
    this.imageName = DruidImage._32_0_1.getName();
    return this;
  }

  /**
   * Uses the {@link DruidImage#_31_0_2 Apache Druid 31.0.2 image} for this container.
   */
  public DruidContainerResource withApache31Image()
  {
    this.imageName = DruidImage._31_0_2.getName();
    return this;
  }

  /**
   * Uses the Docker test image specified by the system property
   * {@link #PROPERTY_TEST_IMAGE} for this container.
   */
  public DruidContainerResource withTestImage()
  {
    this.imageName = Objects.requireNonNull(
        System.getProperty(PROPERTY_TEST_IMAGE),
        StringUtils.format("System property[%s] is not set", PROPERTY_TEST_IMAGE)
    );
    return this;
  }

  public DruidContainerResource addProperty(String key, String value)
  {
    properties.put(key, value);
    return this;
  }

  public String getContainerMountedDir()
  {
    return containerDirectory;
  }

  @Override
  public void beforeStart(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;

    // Set up directories used by the entire cluster (including embedded servers)
    this.deepStorageDirectory = new MountedDir(
        "/tmp/druid/deep-store",
        cluster.getTestFolder().getOrCreateFolder("deep-store").getAbsolutePath()
    );
    this.clusterLogsDirectory = new MountedDir(
        "/tmp/druid/indexer-logs",
        cluster.getTestFolder().getOrCreateFolder("indexer-logs").getAbsolutePath()
    );
    this.clusterConfDirectory = new MountedDir(
        "/opt/shared/conf",
        cluster.getTestFolder().getOrCreateFolder("conf").getAbsolutePath()
    );

    // Set up directories used by this container
    this.containerDirectory = cluster.getTestFolder().getOrCreateFolder(name).getAbsolutePath();
    this.serviceLogsDirectory = new MountedDir(
        "/opt/druid/log",
        containerDirectory + "/log"
    );

    // Create the log directory upfront to avoid permission issues
    mkdirpUnchecked(new File(containerDirectory, "log"));
  }

  @Override
  protected DruidContainer createContainer()
  {
    log.info(
        "Starting Druid container[%s] on port[%d] with mounted directory[%s].",
        name, port, containerDirectory
    );

    final DruidContainer container = new DruidContainer(command, imageName)
        .withNetwork(Network.newNetwork())
        .withNetworkAliases(command.getName())
        .withExposedPorts(port)
        .withFileSystemBind(clusterConfDirectory.hostPath, clusterConfDirectory.containerPath, BindMode.READ_WRITE)
        .withFileSystemBind(deepStorageDirectory.hostPath, deepStorageDirectory.containerPath, BindMode.READ_WRITE)
        .withFileSystemBind(clusterLogsDirectory.hostPath, clusterLogsDirectory.containerPath, BindMode.READ_WRITE)
        .withFileSystemBind(serviceLogsDirectory.hostPath, serviceLogsDirectory.containerPath, BindMode.READ_WRITE)
        .withEnv(
            Map.of(
                "DRUID_CONFIG_COMMON",
                clusterConfDirectory.containerPath + "/" + writeCommonPropertiesToFile(),
                StringUtils.format("DRUID_CONFIG_%s", command.getName()),
                clusterConfDirectory.containerPath + "/" + writeServerPropertiesToFile(),
                "DRUID_SET_HOST_IP", "0",
                "DRUID_SET_HOST", "0"
            )
        )
        .waitingFor(Wait.forHttp("/status/health").forPort(port));

    // Bind the port statically (rather than using a mapped port) to the same
    // value used in `druid.plaintextPort` to make this node discoverable
    // by other services (both embedded and dockerized).
    container.setPortBindings(List.of(StringUtils.format("%d:%d", port, port)));
    return container;
  }

  /**
   * Writes the common properties to a file.
   *
   * @return Name of the file.
   */
  private String writeCommonPropertiesToFile()
  {
    final String fileName = "common.runtime.properties";
    final File commonPropertiesFile = new File(clusterConfDirectory.hostPath, fileName);
    if (commonPropertiesFile.exists()) {
      return fileName;
    }

    final Properties commonProperties = new Properties();
    commonProperties.putAll(cluster.getCommonProperties());
    FORBIDDEN_PROPERTIES.forEach(commonProperties::remove);

    commonProperties.setProperty(
        "druid.zk.service.host",
        getConnectUrlForContainer(commonProperties.getProperty("druid.zk.service.host"))
    );
    commonProperties.setProperty(
        "druid.metadata.storage.connector.connectURI",
        getConnectUrlForContainer(commonProperties.getProperty("druid.metadata.storage.connector.connectURI"))
    );
    commonProperties.setProperty(
        "druid.s3.endpoint.url",
        getConnectUrlForContainer(commonProperties.getProperty("druid.s3.endpoint.url"))
    );

    commonProperties.setProperty("druid.storage.storageDirectory", deepStorageDirectory.containerPath);
    commonProperties.setProperty("druid.indexer.logs.directory", clusterLogsDirectory.containerPath);

    log.info(
        "Writing common properties for Druid containers to file[%s]: [%s]",
        commonPropertiesFile, commonProperties
    );
    writePropertiesToFile(commonProperties, commonPropertiesFile);

    return fileName;
  }

  /**
   * Writes the server properties to a file.
   *
   * @return Name of the file.
   */
  private String writeServerPropertiesToFile()
  {
    FORBIDDEN_PROPERTIES.forEach(properties::remove);
    addProperty("druid.host", hostMachine());
    addProperty("druid.plaintextPort", String.valueOf(port));

    final String fileName = StringUtils.format("%s.runtime.properties", name);
    final File propertiesFile = new File(clusterConfDirectory.hostPath, fileName);

    final Properties serverProperties = new Properties();
    serverProperties.putAll(properties);

    log.info(
        "Writing runtime properties for Druid container[%s] to file[%s]: [%s]",
        name, propertiesFile, serverProperties
    );
    writePropertiesToFile(serverProperties, propertiesFile);

    return fileName;
  }

  private static void writePropertiesToFile(Properties properties, File file)
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

  private static String getConnectUrlForContainer(String connectUrl)
  {
    if (connectUrl.contains("localhost")) {
      return StringUtils.replace(connectUrl, "localhost", "host.docker.internal");
    } else if (connectUrl.contains("127.0.0.1")) {
      return StringUtils.replace(connectUrl, "127.0.0.1", "host.docker.internal");
    } else {
      throw new IAE(
          "Connect URL[%s] must have 'localhost' or '127.0.0.1' as host to be"
          + " reachable by DruidContainers.",
          connectUrl
      );
    }
  }

  /**
   * Hostname for the host machine running the containers. Using this hostname
   * instead of "localhost" allows all the Druid containers to talk to each
   * other and also other EmbeddedDruidServers.
   */
  private static String hostMachine()
  {
    return DruidNode.getDefaultHost();
  }

  @Override
  public String toString()
  {
    return name;
  }

  private static class MountedDir
  {
    final String hostPath;
    final String containerPath;

    MountedDir(String containerPath, String hostPath)
    {
      this.hostPath = hostPath;
      this.containerPath = containerPath;
    }
  }
}
