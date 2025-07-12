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

package org.apache.druid.testing.embedded;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.DruidNode;

/**
 * Contains utility methods and constants for running a {@code DruidContainer}.
 */
public class DruidDocker
{
  /**
   * Used by {@code DruidContainer} to set the value of {@code druid.zk.service.host}.
   */
  public static final String PROPERTY_ZK_CONNECT_STRING = "druid.testing.docker.zk.connectURI";

  /**
   * Used by {@code DruidContainer} to set the value of {@code druid.metadata.storage.connector.connectURI}.
   */
  public static final String PROPERTY_METADATA_STORE_CONNECT_URI = "druid.testing.docker.metadata.connectURI";

  /**
   * Java system property to specify the name of the Docker test image.
   */
  public static final String PROPERTY_TEST_IMAGE = "druid.testing.docker.image";

  /**
   * Creates a connect String that can be used by Druid Docker containers to talk
   * to services running on the host machine.
   */
  public static String connectStringForPort(int port)
  {
    return StringUtils.format("%s:%d", "host.docker.internal", port);
  }

  /**
   * Hostname for the host machine running the containers. Using this hostname
   * instead of "localhost" allows all the Druid containers to talk to each
   * other and also other EmbeddedDruidServers.
   */
  public static String hostMachine()
  {
    return DruidNode.getDefaultHost();
  }
}
