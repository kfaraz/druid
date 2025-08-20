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

package org.apache.druid.testing.embedded.k8s;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexing.IngestionSmokeTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

public class KubernetesTaskRunnerTest extends IngestionSmokeTest
{
  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    // Create a K3s cluster with all the required services
    final K3sClusterResource k3sCluster = new K3sClusterResource()
        .addService(new K3sDruidService(DruidCommand.Server.COORDINATOR))
        .addService(new K3sDruidService(DruidCommand.Server.OVERLORD))
        .addService(new K3sDruidService(DruidCommand.Server.HISTORICAL))
        .addService(new K3sDruidService(DruidCommand.Server.MIDDLE_MANAGER))
        .addService(new K3sDruidService(DruidCommand.Server.ROUTER))
        .addService(
            new K3sDruidService(DruidCommand.Server.BROKER)
                .addProperty("druid.sql.planner.metadataRefreshPeriod", "PT1s")
        );

    // Add an EmbeddedOverlord and EmbeddedBroker to use their client and mapper bindings.
    overlord.addProperty("druid.plaintextPort", "7090");
    broker.addProperty("druid.plaintextPort", "7082");

    return cluster
        .useContainerFriendlyHostname()
        .addResource(k3sCluster)
        .addServer(overlord)
        .addServer(broker)
        .addServer(eventCollector)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\","
            + "\"druid-multi-stage-query\", \"postgresql-metadata-storage\"]"
        );
  }

  @BeforeEach
  public void verifyOverlordLeader()
  {
    // Verify that the EmbeddedOverlord is not leader i.e. the pod Overlord is leader
    Assertions.assertFalse(
        overlord.bindings().overlordLeaderSelector().isLeader()
    );
  }

  @Override
  protected int markSegmentsAsUnused(String dataSource)
  {
    // For old Druid versions, use Coordinator to mark segments as unused
    final ServiceClient coordinatorClient =
        overlord.bindings().getInstance(ServiceClient.class, Coordinator.class);

    try {
      RequestBuilder req = new RequestBuilder(
          HttpMethod.DELETE,
          StringUtils.format("/druid/coordinator/v1/datasources/%s", dataSource)
      );
      BytesFullResponseHolder responseHolder = coordinatorClient.request(
          req,
          new BytesFullResponseHandler()
      );

      final ObjectMapper mapper = overlord.bindings().jsonMapper();
      return mapper.readValue(responseHolder.getContent(), SegmentUpdateResponse.class).getNumChangedSegments();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
