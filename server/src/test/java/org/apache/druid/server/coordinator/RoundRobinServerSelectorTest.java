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

package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

public class RoundRobinServerSelectorTest
{
  private static final String TIER = "normal";

  private final DataSegment segment = new DataSegment(
      "wiki",
      Intervals.of("2022-01-01/2022-01-02"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new NumberedShardSpec(1, 10),
      IndexIO.CURRENT_VERSION_ID,
      100
  );

  @Test
  public void testSingleIterator()
  {
    DruidCluster cluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            TIER,
            createHistorical("server1", 1000),
            createHistorical("server2", 900),
            createHistorical("server3", 10),
            createHistorical("server4", 800)
        )
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(1000, eligibleServers.next().getAvailableSize());

    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(900, eligibleServers.next().getAvailableSize());

    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(800, eligibleServers.next().getAvailableSize());

    Assert.assertFalse(eligibleServers.hasNext());
  }

  @Test
  public void testNextIteratorContinuesFromSamePosition()
  {
    DruidCluster cluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            TIER,
            createHistorical("server1", 1000),
            createHistorical("server2", 900),
            createHistorical("server3", 10),
            createHistorical("server4", 800)
        )
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(1000, eligibleServers.next().getAvailableSize());

    // Second iterator picks up from previous position
    eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(900, eligibleServers.next().getAvailableSize());

    Assert.assertTrue(eligibleServers.hasNext());
    Assert.assertEquals(800, eligibleServers.next().getAvailableSize());

    Assert.assertFalse(eligibleServers.hasNext());
  }

  @Test
  public void testNoServersInTier()
  {
    DruidCluster cluster = DruidClusterBuilder
        .newBuilder()
        .addTier(TIER)
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertFalse(eligibleServers.hasNext());
  }

  @Test
  public void testNoEligibleServerInTier()
  {
    DruidCluster cluster = DruidClusterBuilder
        .newBuilder()
        .addTier(
            TIER,
            createHistorical("server1", 40),
            createHistorical("server2", 30),
            createHistorical("server3", 10),
            createHistorical("server4", 20)
        )
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertFalse(eligibleServers.hasNext());
  }

  private ServerHolder createHistorical(String name, long size)
  {
    return new ServerHolder(
        new DruidServer(name, name, null, size, ServerType.HISTORICAL, TIER, 1).toImmutableDruidServer(),
        new LoadQueuePeonTester()
    );
  }

}