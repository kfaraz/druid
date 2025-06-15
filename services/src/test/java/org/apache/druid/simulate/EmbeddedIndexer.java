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

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliIndexer;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EmbeddedIndexer extends EmbeddedDruidServer
{
  private static final Map<String, String> STANDARD_PROPERTIES = Map.of(
      "druid.lookup.enableLookupSyncOnStartup", "false"
      //"druid.server.http.gracefulShutdownTimeout", "PT0.1s"
  );

  public static EmbeddedIndexer create()
  {
    return new EmbeddedIndexer(STANDARD_PROPERTIES);
  }

  private EmbeddedIndexer(Map<String, String> properties)
  {
    super("Indexer", properties);
  }

  @Override
  protected ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Indexer(handler);
  }

  private static class Indexer extends CliIndexer
  {
    private final LifecycleInitHandler handler;

    private Indexer(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(handler.getInitModules());
      modules.addAll(super.getModules());
      return modules;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }
  }
}
