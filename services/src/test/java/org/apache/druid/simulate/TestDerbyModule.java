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

import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.SQLMetadataStorageDruidModule;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorage;
import org.apache.druid.metadata.MetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageProvider;
import org.apache.druid.metadata.NoopMetadataStorageProvider;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorageProvider;

/**
 * Guice module to bind {@link SQLMetadataConnector} to {@link TestDerbyConnector}.
 * Used in Coordinator and Overlord simulations to connect to an in-memory Derby
 * database.
 */
public class TestDerbyModule extends SQLMetadataStorageDruidModule
{
  public static final String TYPE = "derbyInMemory";
  private final TestDerbyConnector connector;

  public TestDerbyModule(TestDerbyConnector connector)
  {
    super(TYPE);
    this.connector = connector;
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    binder.bind(MetadataStorage.class).toProvider(NoopMetadataStorageProvider.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageProvider.class))
            .addBinding(TYPE)
            .to(DerbyMetadataStorageProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(TYPE)
            .toInstance(connector);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(TYPE)
            .toInstance(connector);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(TYPE)
            .to(DerbyMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);
  }
}
