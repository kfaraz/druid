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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.embedded.DruidDocker;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.derby.StandaloneDerbyMetadataResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.junit.jupiter.api.Test;

public class EmbeddedDockerBackwardCompatibilityTest extends EmbeddedClusterTestBase
{
  static {
    System.setProperty(DruidDocker.PROPERTY_TEST_IMAGE, "apache/druid:tang");
  }

  private final DruidContainer overlord = DruidDockerContainers.newOverlord().withDockerTestImage();
  private final DruidContainer coordinator = DruidDockerContainers.newCoordinator().withDockerTestImage();
  private final DruidContainer indexer = DruidDockerContainers.newIndexer().withDockerTestImage();
  private final DruidContainer historical = DruidDockerContainers.newHistorical().withDockerTestImage();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    // TODO: support minio properly
    //  that would need supporting extensions and common props
    return EmbeddedDruidCluster.withZookeeper()
                               .useLatchableEmitter()
                               .useDruidContainers()
                               .addResource(new StandaloneDerbyMetadataResource())
                               .addResource(new MinIOStorageResource())
                               .addResource(coordinator)
                               .addResource(overlord)
                               .addResource(indexer)
                               .addResource(historical)
                               .addServer(new EmbeddedBroker())
                               .addServer(new EmbeddedRouter());
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
    System.out.println("Finish off the things");

    // TODO: We can extend an existing test as long as none of the servers are being referenced directly
    //  but then how do we wait for events
  }

  private Object createIndexTaskForInlineData(String taskId, String inlineDataCsv)
  {
    return EmbeddedClusterApis.createTaskFromPayload(
        taskId,
        StringUtils.format(Resources.INDEX_TASK_PAYLOAD_WITH_INLINE_DATA, inlineDataCsv, dataSource)
    );
  }
}
