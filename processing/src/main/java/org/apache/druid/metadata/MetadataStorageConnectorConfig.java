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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 *
 */
public class MetadataStorageConnectorConfig
{
  public static final String PROPERTY_BASE = "druid.metadata.storage.connector";

  private final String derbyHost;
  private final int derbyPort;
  private final String connectURI;

  private final String user;
  private final PasswordProvider passwordProvider;

  private final boolean createTables;
  private final Properties dbcpProperties;

  /**
   * Creates a {@code MetadataStorageConnectorConfig}.
   *
   * @param connectURI     Required when not using a Derby metadata store
   * @param derbyHost      Required only when using a Derby metadata store, default is "localhost"
   * @param derbyPort      Required only when using a Derby metadata store, default is 1527
   * @param dbcpProperties Connection properties
   * @param createTables   Whether metadata tables should be freshly created, defaults is true
   */
  @JsonCreator
  public MetadataStorageConnectorConfig(
      @JsonProperty("connectURI") @Nullable String connectURI,
      @JsonProperty("host") @Nullable String derbyHost,
      @JsonProperty("port") @Nullable Integer derbyPort,
      @JsonProperty("user") @Nullable String user,
      @JsonProperty("password") @Nullable PasswordProvider passwordProvider,
      @JsonProperty("dbcp") Properties dbcpProperties,
      @JsonProperty("createTables") Boolean createTables
  )
  {
    // Default host and port to Derby running locally
    this.derbyHost = Configs.valueOrDefault(derbyHost, "localhost");
    this.derbyPort = Configs.valueOrDefault(derbyPort, 1527);
    this.connectURI = Configs.valueOrDefault(
        connectURI,
        StringUtils.format("jdbc:derby://%s:%s/druid;create=true", this.derbyHost, this.derbyPort)
    );

    this.user = user;
    this.passwordProvider = passwordProvider;
    this.dbcpProperties = dbcpProperties;
    this.createTables = Configs.valueOrDefault(createTables, true);
  }

  public static MetadataStorageConnectorConfig create(String connectURI)
  {
    return create(connectURI, null, null, null);
  }

  public static MetadataStorageConnectorConfig create(
      String connectUri,
      String user,
      String password,
      Map<String, String> properties
  )
  {
    Properties dbcpProperties = new Properties();
    if (properties != null) {
      dbcpProperties.putAll(properties);
    }
    return new MetadataStorageConnectorConfig(
        connectUri,
        null,
        null,
        user,
        DefaultPasswordProvider.fromString(password),
        dbcpProperties,
        null
    );
  }

  public boolean isCreateTables()
  {
    return createTables;
  }

  public String getDerbyHost()
  {
    return derbyHost;
  }

  public int getDerbyPort()
  {
    return derbyPort;
  }

  public String getConnectURI()
  {
    return connectURI;
  }

  public String getUser()
  {
    return user;
  }

  public String getPassword()
  {
    return passwordProvider == null ? null : passwordProvider.getPassword();
  }

  public Properties getDbcpProperties()
  {
    return dbcpProperties;
  }

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + connectURI + '\'' +
           ", user='" + user + '\'' +
           ", passwordProvider=" + passwordProvider +
           ", dbcpProperties=" + dbcpProperties +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetadataStorageConnectorConfig that = (MetadataStorageConnectorConfig) o;
    return createTables == that.createTables
           && derbyPort == that.derbyPort
           && Objects.equals(derbyHost, that.derbyHost)
           && Objects.equals(connectURI, that.connectURI)
           && Objects.equals(user, that.user)
           && Objects.equals(passwordProvider, that.passwordProvider)
           && Objects.equals(dbcpProperties, that.dbcpProperties);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createTables, derbyPort, derbyHost, connectURI, user, passwordProvider, dbcpProperties);
  }
}
