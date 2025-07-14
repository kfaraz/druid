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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Testcontainer for running Apache Druid services.
 * <p>
 * Usage:
 * <pre>
 *
 * </pre>
 * Dependencies of Druid services:
 * <ul>
 * <li>All Druid services need to be able to access </li>
 * </ul>
 */
public class DruidContainer extends GenericContainer<DruidContainer>
{
  /**
   * Creates a new {@link DruidContainer} which uses the given image name.
   *
   * @param command   Druid command to run. e.g. "coordinator", "overlord".
   * @param imageName Name of the Druid image to use
   * @see DruidCommand for standard Druid commands.
   * @see DruidImage for standard images.
   */
  public DruidContainer(DruidCommand command, String imageName)
  {
    super(DockerImageName.parse(imageName));
    setCommand(command.getName());
  }

  /**
   * Creates a new {@link DruidContainer} which uses the given standard image.
   *
   * @param command Druid command to run. e.g. "coordinator", "overlord".
   * @param image   Druid image to use for the Testcontainer.
   * @see DruidCommand for standard Druid commands.
   * @see DruidImage for standard images.
   */
  public static DruidContainer forCommandWithImage(DruidCommand command, DruidImage image)
  {
    return new DruidContainer(command, image.getName());
  }
}
