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
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.derby.EmbeddedDerbyMetadataResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.timeline.DataSegment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class IngestionDockerTest extends EmbeddedClusterTestBase
{
  static {
    System.setProperty(DruidContainerResource.PROPERTY_TEST_IMAGE, "apache/druid:tang");
  }

  // Docker containers
  private final DruidContainerResource overlordLeader = DruidContainers.newOverlord().withTestImage();
  private final DruidContainerResource coordinator = DruidContainers.newCoordinator().withTestImage();
  private final DruidContainerResource historical = DruidContainers.newHistorical().withTestImage();
  private final DruidContainerResource broker = DruidContainers.newBroker().withTestImage();
  private final DruidContainerResource middleManager = DruidContainers
      .newMiddleManager()
      .withTestImage()
      .addProperty("druid.worker.capacity", "5");

  // Follower EmbeddedOverlord to watch segment publish events
  private final EmbeddedOverlord overlordFollower = new EmbeddedOverlord()
      .addProperty("druid.plaintextPort", "7090")
      .addProperty("druid.manager.segments.useIncrementalCache", "always")
      .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withZookeeper()
                               .useLatchableEmitter()
                               .useDruidContainers()
                               .addResource(new EmbeddedDerbyMetadataResource())
                               .addResource(new MinIOStorageResource())
                               .addCommonProperty("druid.extensions.loadList", "[\"druid-s3-extensions\"]")
                               .addResource(coordinator)
                               .addResource(overlordLeader)
                               .addResource(middleManager)
                               .addResource(historical)
                               .addResource(broker)
                               .addServer(overlordFollower)
                               .addServer(new EmbeddedRouter());
  }

  @Test
  public void test_runIndexTask_andKillData()
  {

  }

  @Test
  public void test_runIndexParallelTask_andCompactData()
  {

  }

  @Test
  public void test_runMsqTask_andQueryData()
  {

  }

  @Test
  public void test_runKafkaSupervisor_andConcurrentCompactionSupervisor()
  {

  }

  @Test
  public void test_runATask()
  {
    final String taskId = IdUtils.getRandomId();
    final Object task = createIndexTaskForInlineData(
        taskId,
        StringUtils.replace(Resources.CSV_DATA_10_DAYS, "\n", "\\n")
    );

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));

    // Wait for follower Overlord to add the new segments to its cache
    overlordFollower.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/metadataCache/used/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValue(10)
    );

    final Set<DataSegment> allUsedSegments = overlordFollower
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED);
    Assertions.assertEquals(10, allUsedSegments.size());
  }

  private Object createIndexTaskForInlineData(String taskId, String inlineDataCsv)
  {
    return EmbeddedClusterApis.createTaskFromPayload(
        taskId,
        StringUtils.format(Resources.INDEX_TASK_PAYLOAD_WITH_INLINE_DATA, inlineDataCsv, dataSource)
    );
  }
}
