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

package org.apache.druid.grpc.guice;

import com.google.inject.Binder;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.grpc.server.GrpcEndpointInitializer;
import org.apache.druid.grpc.server.GrpcQueryConfig;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.initialization.DruidModule;

@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class GrpcQueryModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, GrpcQueryConfig.CONFIG_BASE, GrpcQueryConfig.class);
    LifecycleModule.register(binder, GrpcEndpointInitializer.class);
  }
}
