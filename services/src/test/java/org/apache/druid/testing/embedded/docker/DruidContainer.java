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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.TestcontainerResource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * TODO:
 * - things we need to support
 * - run each service independently
 * - add custom properties for each server
 * - add extensions
 * - add common properties?
 * - choose the image
 * - in a cluster with all docker images, how do we wait for events?
 * - get the docker image to emit stuff to this jvm, without using any special extension right now.
 * - we do have Kafka emitter, we could emit stuff to Kafka and then have a task
 * that reads those events and then wait on that?
 * - I don't want to do retry utils. So not sure what we can do here.
 * - Maybe just Dockerize one service at a time? where's the fun in that.
 * - A full docker thing should still be doable
 *
 * @see DockerizedCoordinator
 * @see DockerizedOverlord
 * @see DockerizedIndexer
 * @see DockerizedMiddleManager
 * @see DockerizedHistorical
 * @see DockerizedRouter
 */
public class DruidContainer<C extends DruidContainer<C>> extends TestcontainerResource<DruidContainer.ContainerImpl>
{
  public static final String ZK_SERVICE_HOST = "druid.testing.embedded.docker.zk.host";
  public static final String METADATA_STORAGE_CONNECT_URI = "druid.testing.embedded.docker.storage.connectURI";

  private static final String IMAGE_APACHE_32 = "apache/druid:33.0.0";
  private static final String IMAGE_LOCAL = "apache/druid:tang";

  private final Map<String, String> properties = new HashMap<>();

  private final NodeRole nodeRole;
  private int port;

  private EmbeddedDruidCluster cluster;

  DruidContainer(NodeRole nodeRole, int port)
  {
    this.nodeRole = nodeRole;
    this.port = port;
  }

  @SuppressWarnings("unchecked")
  public C setPort(int port)
  {
    this.port = port;
    return (C) this;
  }

  @SuppressWarnings("unchecked")
  public C addProperty(String key, String value)
  {
    properties.put(key, value);
    return (C) this;
  }

  @Override
  protected ContainerImpl createContainer()
  {
    addProperty(
        "druid.zk.service.host",
        cluster.getCommonProperty(ZK_SERVICE_HOST)
    );
    addProperty(
        "druid.metadata.storage.connector.connectURI",
        cluster.getCommonProperty(METADATA_STORAGE_CONNECT_URI)
    );
    addProperty("druid.extensions.loadList", "[]");
    addProperty("druid.host", "localhost");

    final Network network = Network.newNetwork();

    // Write out an empty common properties file for the time being
    // All needed properties will just go to the service specific properties file

    // TODO: use this directory for all the paths
    final String baseServiceDir = nodeRole.getJsonName() + "_" + IdUtils.getRandomId();

    final String commonPropertiesFile = "conf/common.runtime.properties";
    writePropertiesToFile(new Properties(), "var/shared/" + commonPropertiesFile);

    final String serverPropertiesFile = "conf/coordinator.runtime.properties";
    final Properties serverProperties = new Properties();
    serverProperties.putAll(properties);
    writePropertiesToFile(serverProperties, "var/shared/" + serverPropertiesFile);

    return new ContainerImpl(IMAGE_LOCAL, port)
        .withNetwork(network)
        .withNetworkAliases(nodeRole.getJsonName())
        .withCommand(nodeRole.getJsonName())
        .withFileSystemBind("var/shared", "/opt/shared", BindMode.READ_WRITE)
        .withFileSystemBind("var/coordinator", "/opt/druid/var", BindMode.READ_WRITE)
        .withFileSystemBind("var/log", "/opt/druid/log", BindMode.READ_WRITE)
        .withEnv(
            Map.of(
                "DRUID_CONFIG_COMMON", "/opt/shared/" + commonPropertiesFile,
                "DRUID_CONFIG_coordinator", "/opt/shared/" + serverPropertiesFile,
                "DRUID_SET_HOST_IP", "0"
            )
        )
        .withExposedPorts(port)
        .waitingFor(Wait.forHttp("/status/health").forPort(port));
  }

  @Override
  public void onAddedToCluster(EmbeddedDruidCluster cluster)
  {
    this.cluster = cluster;
  }

  private void writePropertiesToFile(Properties properties, String file)
  {
    try (FileOutputStream out = new FileOutputStream(file)) {
      properties.store(out, "Druid config");
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a connect String that can be used by Druid Docker containers to talk
   * to services running on the host machine.
   */
  public static String connectStringForPort(int port)
  {
    return StringUtils.format("host.docker.internal:%s", port);
  }

  /**
   * Druid implementation of {@link GenericContainer}.
   */
  public static class ContainerImpl extends GenericContainer<ContainerImpl>
  {
    public ContainerImpl(String imageName, int port)
    {
      super(DockerImageName.parse(imageName));
      setPortBindings(
          List.of(StringUtils.format("%d:%d", port, port))
      );
    }
  }
}
