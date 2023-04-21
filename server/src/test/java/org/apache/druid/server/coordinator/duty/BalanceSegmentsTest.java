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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeonTester;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class BalanceSegmentsTest
{
  private SegmentLoadQueueManager loadQueueManager;

  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  private DataSegment segment5;

  private DataSegment[] allSegments;

  private ListeningExecutorService balancerStrategyExecutor;
  private BalancerStrategy balancerStrategy;
  private Set<String> broadcastDatasources;

  private DruidServer server1;
  private DruidServer server2;
  private DruidServer server3;
  private DruidServer server4;

  @Before
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null, null);

    // Create test segments for multiple datasources
    final DateTime start1 = DateTimes.of("2012-01-01");
    final DateTime start2 = DateTimes.of("2012-02-01");
    final String version = DateTimes.of("2012-03-01").toString();

    segment1 = createHourlySegment("datasource1", start1, version);
    segment2 = createHourlySegment("datasource1", start2, version);
    segment3 = createHourlySegment("datasource2", start1, version);
    segment4 = createHourlySegment("datasource2", start2, version);
    segment5 = createHourlySegment("datasourceBroadcast", start2, version);
    allSegments = new DataSegment[]{segment1, segment2, segment3, segment4, segment5};

    server1 = new DruidServer("server1", "server1", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server2 = new DruidServer("server2", "server2", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server3 = new DruidServer("server3", "server3", null, 100L, ServerType.HISTORICAL, "normal", 0);
    server4 = new DruidServer("server4", "server4", null, 100L, ServerType.HISTORICAL, "normal", 0);

    balancerStrategyExecutor = MoreExecutors.listeningDecorator(Execs.multiThreaded(1, "BalanceSegmentsTest-%d"));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(balancerStrategyExecutor);

    broadcastDatasources = Collections.singleton("datasourceBroadcast");
  }

  @After
  public void tearDown()
  {
    balancerStrategyExecutor.shutdownNow();
  }

  @Test
  public void testMoveToEmptyServerBalancer()
  {
    final ServerHolder serverHolder1 = createHolder(server1, allSegments);
    final ServerHolder serverHolder2 = createHolder(server2);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2)
            .withBalancerStrategy(balancerStrategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertEquals(2, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1"));
    Assert.assertEquals(1, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2"));
  }

  /**
   * Server 1 has 2 segments.
   * Server 2 (decommissioning) has 2 segments.
   * Server 3 is empty.
   * Decommissioning percent is 60.
   * Max segments to move is 3.
   * 2 (of 2) segments should be moved from Server 2 and 1 (of 2) from Server 1.
   */
  @Test
  public void testMoveDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    final ServerHolder serverHolder1 = createHolder(server1, false, segment1, segment2);
    final ServerHolder serverHolder2 = createHolder(server2, true, segment3, segment4);
    final ServerHolder serverHolder3 = createHolder(server3, false);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    expectPickLoadingSegmentsAndReturnEmpty(strategy).times(2);
    expectPickLoadedSegmentsAndReturn(
        strategy,
        new BalancerSegmentHolder(serverHolder2, segment3),
        new BalancerSegmentHolder(serverHolder2, segment4)
    ).once();
    expectPickLoadedSegmentsAndReturn(
        strategy,
        new BalancerSegmentHolder(serverHolder1, segment1),
        new BalancerSegmentHolder(serverHolder1, segment2)
    ).once();
    expectFindDestinationAndReturn(strategy, serverHolder3);
    EasyMock.replay(strategy);

    // ceil(3 * 0.6) = 2 segments from decommissioning servers
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(3)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(60)
                                .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2, serverHolder3)
            .withDynamicConfigs(dynamicConfig)
            .withBalancerStrategy(strategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);

    EasyMock.verify(strategy);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1"));
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2"));
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment3, segment4),
        serverHolder3.getPeon().getSegmentsToLoad()
    );
  }

  @Test
  public void testZeroDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    final ServerHolder holder1 = createHolder(server1, false, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, true, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3, false);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder().withDecommissioningMaxPercentOfMaxSegmentsToMove(0)
                                .withMaxSegmentsToMove(1).build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3).withDynamicConfigs(dynamicConfig).build();

    CoordinatorRunStats stats = runBalancer(params);

    // Verify that either segment1 or segment2 is chosen for move
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", segment1.getDataSource()));
    DataSegment movingSegment = holder3.getPeon().getSegmentsToLoad().iterator().next();
    Assert.assertEquals(segment1.getDataSource(), movingSegment.getDataSource());
  }

  @Test
  public void testMaxDecommissioningMaxPercentOfMaxSegmentsToMove()
  {
    final ServerHolder holder1 = createHolder(server1, false, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, true, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3, false);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(100)
                                .withMaxSegmentsToMove(1).build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3).withDynamicConfigs(dynamicConfig).build();

    CoordinatorRunStats stats = runBalancer(params);

    // Verify that either segment3 or segment4 is chosen for move
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", segment3.getDataSource()));
    DataSegment movingSegment = holder3.getPeon().getSegmentsToLoad().iterator().next();
    Assert.assertEquals(segment3.getDataSource(), movingSegment.getDataSource());
  }

  /**
   * Should balance segments as usual (ignoring percent) with empty decommissioningNodes.
   */
  @Test
  public void testMoveDecommissioningMaxPercentOfMaxSegmentsToMoveWithNoDecommissioning()
  {
    final ServerHolder serverHolder1 = createHolder(server1, segment1, segment2);
    final ServerHolder serverHolder2 = createHolder(server2, segment3, segment4);
    final ServerHolder serverHolder3 = createHolder(server3);
    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    expectPickLoadingSegmentsAndReturnEmpty(strategy).once();
    expectPickLoadedSegmentsAndReturn(
        strategy,
        new BalancerSegmentHolder(serverHolder1, segment2),
        new BalancerSegmentHolder(serverHolder2, segment3),
        new BalancerSegmentHolder(serverHolder2, segment4)
    ).once();
    expectFindDestinationAndReturn(strategy, serverHolder3);
    EasyMock.replay(strategy);

    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(3)
                                .withDecommissioningMaxPercentOfMaxSegmentsToMove(9)
                                .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2, serverHolder3)
            .withDynamicConfigs(dynamicConfig)
            .withBalancerStrategy(strategy)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    EasyMock.verify(strategy);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1"));
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2"));
    Assert.assertEquals(
        ImmutableSet.of(segment2, segment3, segment4),
        serverHolder3.getPeon().getSegmentsToLoad()
    );
  }

  /**
   * Shouldn't move segments to a decommissioning server.
   */
  @Test
  public void testMoveToDecommissioningServer()
  {
    final ServerHolder serverHolder1 = createHolder(server1, false, allSegments);
    final ServerHolder serverHolder2 = createHolder(server2, true);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    expectPickLoadingSegmentsAndReturnEmpty(strategy).times(2);
    expectPickLoadedSegmentsAndReturn(
        strategy,
        new BalancerSegmentHolder(serverHolder1, segment1)
    ).times(2);
    EasyMock.expect(
        strategy.findDestinationServerToMoveSegment(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andAnswer(() -> ((List<ServerHolder>) EasyMock.getCurrentArguments()[2]).get(0)).anyTimes();
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(serverHolder1, serverHolder2)
            .withBalancerStrategy(strategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    EasyMock.verify(strategy);
    Assert.assertFalse(stats.hasStat(Stats.Segments.MOVED));
  }

  @Test
  public void testMoveFromDecommissioningServer()
  {
    final ServerHolder holder1 = createHolder(server1, allSegments);
    final ServerHolder holder2 = createHolder(server2);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    expectPickLoadingSegmentsAndReturnEmpty(strategy).once();
    expectPickLoadedSegmentsAndReturn(strategy, new BalancerSegmentHolder(holder1, segment1)).once();
    expectFindDestinationAndReturn(strategy, holder2);
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(holder1, holder2)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(1).build())
        .withBalancerStrategy(strategy)
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    CoordinatorRunStats stats = runBalancer(params);
    EasyMock.verify(strategy);
    Assert.assertEquals(1, stats.getSegmentStat(Stats.Segments.MOVED, "normal", segment1.getDataSource()));
    Assert.assertEquals(0, holder1.getPeon().getNumberOfSegmentsToLoad());
    Assert.assertEquals(1, holder2.getPeon().getNumberOfSegmentsToLoad());
  }

  @Test
  public void testMoveMaxLoadQueueServerBalancer()
  {
    final int maxSegmentsInQueue = 1;
    final ServerHolder holder1 = createHolder(server1, maxSegmentsInQueue, false, allSegments);
    final ServerHolder holder2 = createHolder(server2, maxSegmentsInQueue, false);

    final CoordinatorDynamicConfig dynamicConfig = CoordinatorDynamicConfig
        .builder()
        .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInQueue)
        .build();
    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2)
            .withDynamicConfigs(dynamicConfig)
            .build();

    CoordinatorRunStats stats = runBalancer(params);

    // max to move is 5, all segments on server 1, but only expect to move 1 to server 2 since max node load queue is 1
    Assert.assertEquals(maxSegmentsInQueue, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1"));
  }

  @Test
  public void testRun1()
  {
    // Mock some servers of different usages
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2)
    ).build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertTrue(stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1") > 0);
  }

  @Test
  public void testRun2()
  {
    // Mock some servers of different usages
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2),
        createHolder(server3),
        createHolder(server4)
    ).build();

    CoordinatorRunStats stats = runBalancer(params);
    Assert.assertTrue(stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1") > 0);
  }

  @Test
  public void testThatMaxSegmentsToMoveIsHonored()
  {
    // Move from non-decomissioning servers
    final ServerHolder holder1 = createHolder(server1, segment1, segment2);
    final ServerHolder holder2 = createHolder(server2, segment3, segment4);
    final ServerHolder holder3 = createHolder(server3);

    BalancerStrategy strategy = EasyMock.createMock(BalancerStrategy.class);
    expectPickLoadingSegmentsAndReturnEmpty(strategy).once();
    expectPickLoadedSegmentsAndReturn(strategy, new BalancerSegmentHolder(holder2, segment3)).once();
    expectFindDestinationAndReturn(strategy, holder3);
    EasyMock.replay(strategy);

    DruidCoordinatorRuntimeParams params =
        defaultRuntimeParamsBuilder(holder1, holder2, holder3)
            .withDynamicConfigs(
                CoordinatorDynamicConfig.builder()
                                        .withMaxSegmentsToMove(1)
                                        .withUseBatchedSegmentSampler(true)
                                        .withPercentOfSegmentsToConsiderPerMove(40)
                                        .build()
            )
            .withBalancerStrategy(strategy)
            .withBroadcastDatasources(broadcastDatasources)
            .build();

    CoordinatorRunStats stats = runBalancer(params);
    EasyMock.verify(strategy);
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2"));
    Assert.assertEquals(ImmutableSet.of(segment3), holder3.getPeon().getSegmentsToLoad());
  }

  @Test
  public void testUseBatchedSegmentSampler()
  {
    DruidCoordinatorRuntimeParams params = defaultRuntimeParamsBuilder(
        createHolder(server1, allSegments),
        createHolder(server2),
        createHolder(server3),
        createHolder(server4)
    )
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withMaxSegmentsToMove(2)
                                    .withUseBatchedSegmentSampler(true)
                                    .build()
        )
        .withBroadcastDatasources(broadcastDatasources)
        .build();

    CoordinatorRunStats stats = runBalancer(params);
    long totalMoved = stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource1")
                      + stats.getSegmentStat(Stats.Segments.MOVED, "normal", "datasource2");
    Assert.assertEquals(2L, totalMoved);
  }

  private CoordinatorRunStats runBalancer(DruidCoordinatorRuntimeParams params)
  {
    params = new BalanceSegments(loadQueueManager).run(params);
    if (params == null) {
      Assert.fail("BalanceSegments duty returned null params");
      return new CoordinatorRunStats();
    } else {
      return params.getCoordinatorStats();
    }
  }

  private DruidCoordinatorRuntimeParams.Builder defaultRuntimeParamsBuilder(
      ServerHolder... servers
  )
  {
    return CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withDruidCluster(DruidCluster.builder().addTier("normal", servers).build())
        .withUsedSegmentsInTest(allSegments)
        .withBroadcastDatasources(broadcastDatasources)
        .withBalancerStrategy(balancerStrategy)
        .withReplicationManager(createReplicationThrottler());
  }

  private ServerHolder createHolder(DruidServer server, DataSegment... loadedSegments)
  {
    return createHolder(server, false, loadedSegments);
  }

  private ServerHolder createHolder(DruidServer server, boolean isDecommissioning, DataSegment... loadedSegments)
  {
    return createHolder(server, 0, isDecommissioning, loadedSegments);
  }

  private ServerHolder createHolder(
      DruidServer server,
      int maxSegmentsInLoadQueue,
      boolean isDecommissioning,
      DataSegment... loadedSegments
  )
  {
    for (DataSegment segment : loadedSegments) {
      server.addDataSegment(segment);
    }

    return new ServerHolder(
        server.toImmutableDruidServer(),
        new LoadQueuePeonTester(),
        isDecommissioning,
        maxSegmentsInLoadQueue
    );
  }

  private IExpectationSetters<Iterator<BalancerSegmentHolder>> expectPickLoadingSegmentsAndReturnEmpty(
      BalancerStrategy strategy
  )
  {
    return EasyMock.expect(
        strategy.pickSegmentsToMove(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyInt(),
            EasyMock.eq(true)
        )
    ).andReturn(Collections.emptyIterator());
  }

  private IExpectationSetters<Iterator<BalancerSegmentHolder>> expectPickLoadedSegmentsAndReturn(
      BalancerStrategy strategy,
      BalancerSegmentHolder... pickedLoadedSegments
  )
  {
    return EasyMock.expect(
        strategy.pickSegmentsToMove(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyInt(),
            EasyMock.eq(false)
        )
    ).andReturn(Arrays.asList(pickedLoadedSegments).iterator());
  }

  private void expectFindDestinationAndReturn(BalancerStrategy strategy, ServerHolder chosenServer)
  {
    EasyMock.expect(
        strategy.findDestinationServerToMoveSegment(
            EasyMock.anyObject(),
            EasyMock.anyObject(),
            EasyMock.anyObject()
        )
    ).andReturn(chosenServer).anyTimes();
  }

  private ReplicationThrottler createReplicationThrottler()
  {
    CoordinatorDynamicConfig dynamicConfig = CoordinatorDynamicConfig.builder().build();
    return new ReplicationThrottler(
        Collections.singleton("normal"),
        dynamicConfig.getReplicationThrottleLimit(),
        dynamicConfig.getReplicantLifetime(),
        dynamicConfig.getMaxNonPrimaryReplicantsToLoad()
    );
  }

  private DataSegment createHourlySegment(String datasource, DateTime start, String version)
  {
    return new DataSegment(
        datasource,
        new Interval(start, start.plusHours(1)),
        version,
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        8L
    );
  }
}
