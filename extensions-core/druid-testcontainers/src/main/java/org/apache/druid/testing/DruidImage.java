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

/**
 * Standard images for {@link DruidContainer}. Other images may also be used
 * with the constructor {@link DruidContainer#DruidContainer(DruidCommand, String)}.
 */
public enum DruidImage
{
  _31_0_2("apache/druid:31.0.2"),
  _32_0_1("apache/druid:32.0.1"),
  _33_0_0("apache/druid:33.0.0");

  private final String name;

  DruidImage(String name)
  {
    this.name = name;
  }

  public String getName()
  {
    return name;
  }
}
