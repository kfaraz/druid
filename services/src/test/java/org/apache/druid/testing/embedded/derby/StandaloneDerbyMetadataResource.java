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

package org.apache.druid.testing.embedded.derby;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.storage.derby.DerbyMetadataStorage;
import org.apache.druid.testing.embedded.DruidDocker;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedResource;

import java.util.Map;

/**
 * Resource to run a Derby metadata store in this JVM but functioning as a
 * standalone process.
 */
public class StandaloneDerbyMetadataResource implements EmbeddedResource
{
  /**
   * This must be the same as the database name used in
   * {@link MetadataStorageConnectorConfig#getConnectURI()}.
   */
  private static final String DATABASE_PATH = "target/derby";

  private final DerbyMetadataStorage storage;
  private final MetadataStorageConnectorConfig connectorConfig;

  public StandaloneDerbyMetadataResource()
  {
    this.connectorConfig = MetadataStorageConnectorConfig.create(
        StringUtils.format(
            "jdbc:derby://%s:%s/%s;create=true",
            "localhost", 1527, DATABASE_PATH
        ),
        null,
        null,
        Map.of()
    );
    this.storage = new DerbyMetadataStorage(connectorConfig);
  }

  @Override
  public void start() throws Exception
  {
    storage.start();
  }

  @Override
  public void stop() throws Exception
  {
    storage.stop();
  }

  @Override
  public void onStarted(EmbeddedDruidCluster cluster)
  {
    cluster.addCommonProperty("druid.metadata.storage.connector.connectURI", connectorConfig.getConnectURI());
    cluster.addCommonProperty(DruidDocker.PROPERTY_METADATA_STORE_CONNECT_URI, getConnectUriForDocker());
  }

  public String getConnectUriForDocker()
  {
    return StringUtils.format(
        "jdbc:derby://%s/%s;create=true",
        DruidDocker.connectStringForPort(connectorConfig.getPort()),
        DATABASE_PATH
    );
  }
}
