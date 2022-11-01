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

package org.apache.druid.server.coordinator.simulate;

import io.vavr.collection.Stream;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class CoordinatorDutiesTest extends CoordinatorSimulationBaseTest
{
  private final List<DruidServer> servers;

  /**
   * segments = 10k * 24H (1MB)
   */
  private final List<DataSegment> segments =
      CreateDataSegments.ofDatasource(DS.WIKI)
                        .forIntervals(24, Granularities.HOUR)
                        .startingAt("2022-01-01")
                        .withNumPartitions(10000)
                        .eachOfSizeInMb(1);

  public CoordinatorDutiesTest()
  {
    long startTime = System.currentTimeMillis();
    servers = createHistoricalTier(Tier.T1, 100, 20000);
    System.out.println("SETUP TIME: " + (System.currentTimeMillis() - startTime));
  }

  @Override
  public void setUp()
  {

  }

  @Test
  public void testMarkingThing()
  {
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(0)
                                .withMaxSegmentsInNodeLoadingQueue(0)
                                .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(0)
                                .build();

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(servers)
                             .withRules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(dynamicConfig)
                             .withImmediateSegmentLoading(true)
                             .withBalancerStrategy("random")
                             .build();

    // Assign the segments to the servers in a round robin manner
    // Best way to do it
    // Run for one cycle with immediate loading

    startSimulation(sim);

    runCoordinatorCycle();
    printStuff();
    verifyDatasourceIsFullyLoaded(DS.WIKI);

    //runCoordinatorCycle();
    //printStuff();
  }

  private void printStuff()
  {
    // System.out.println("assignedCount: " + getValue("segment/assigned/count", null));
    System.out.printf(
        "Duty times: %s %s%n, %s %s %s%n %s%n",
        getDutyTime("duty.LogUsedSegments"),
        getDutyTime("duty.RunRules"),
        getAdhocDutyTime("updateReplicasInTier"),
        getAdhocDutyTime("createSegmentStatus"),
        getAdhocDutyTime("loadReplicas"),
        //getAdhocDutyTime("dropReplicas")
        //getDutyTime("duty.BalanceSegments"),
        //getDutyTime("duty.MarkAsUnusedOvershadowedSegments"),
        getDutyTime("DruidCoordinator$UpdateCoordinatorStateAndPrepareCluster")
        //getDutyTime("duty.UnloadUnusedSegments")
    );
    System.out.println(
        "Historical duties run time: " + getValue(
            "coordinator/global/time",
            filter(DruidMetrics.DUTY_GROUP, "HistoricalManagementDuties")
        )
    );
    System.out.println(
        "Metadata duties run time: " + getValue(
            "coordinator/global/time",
            filter(DruidMetrics.DUTY_GROUP, "MetadataStoreManagementDuties")
        )
    );
  }

  private String getDutyTime(String dutyName)
  {
    return String.format(
        "%s [%d]",
        dutyName,
        getValue(
            "coordinator/time",
            filter(DruidMetrics.DUTY, "org.apache.druid.server.coordinator." + dutyName)
        ).longValue()
    );
  }

  private String getAdhocDutyTime(String dutyName)
  {
    return String.format(
        "%s [%d]",
        dutyName,
        getValue(
            "coordinator/adhoc/time",
            filter(DruidMetrics.DUTY, dutyName)
        ).longValue()
    );
  }

  private List<DruidServer> createHistoricalTier(String tier, int numServers, long serverSizeMb)
  {
    return Stream.range(0, numServers)
                 .map(serverId -> createHistorical(serverId, tier, serverSizeMb))
                 .collect(Collectors.toList());
  }
}
