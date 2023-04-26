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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.CoordinatorIndexingServiceDuty;
import org.apache.druid.guice.annotations.CoordinatorMetadataStoreManagementDuty;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.druid.server.coordinator.balancer.UniformIntervalBalancerStrategy;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.CollectSegmentAndServerStats;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.duty.CompactionSegmentSearchPolicy;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.druid.server.coordinator.duty.CoordinatorDuty;
import org.apache.druid.server.coordinator.duty.LogUsedSegments;
import org.apache.druid.server.coordinator.duty.MarkAsUnusedOvershadowedSegments;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.duty.UnloadUnusedSegments;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeon;
import org.apache.druid.server.coordinator.loadqueue.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.server.initialization.jetty.ServiceUnavailableException;
import org.apache.druid.server.lookup.cache.LookupCoordinatorManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *
 */
@ManageLifecycle
public class DruidCoordinator
{
  /**
   * This comparator orders "freshest" segments first, i. e. segments with most recent intervals.
   * <p>
   * It is used in historical nodes' {@link LoadQueuePeon}s to make historicals load more recent segment first.
   * <p>
   * It is also used in {@link DruidCoordinatorRuntimeParams} for {@link
   * DruidCoordinatorRuntimeParams#getUsedSegments()} - a collection of segments to be considered during some
   * coordinator run for different {@link CoordinatorDuty}s. The order matters only for {@link
   * RunRules}, which tries to apply the rules while iterating the segments in the order imposed by
   * this comparator. In {@link LoadRule} the throttling limit may be hit (via {@link ReplicationThrottler}; see
   * {@link CoordinatorDynamicConfig#getReplicationThrottleLimit()}). So before we potentially hit this limit, we want
   * to schedule loading the more recent segments (among all of those that need to be loaded).
   * <p>
   * In both {@link LoadQueuePeon}s and {@link RunRules}, we want to load more recent segments first
   * because presumably they are queried more often and contain are more important data for users, so if the Druid
   * cluster has availability problems and struggling to make all segments available immediately, at least we try to
   * make more "important" (more recent) segments available as soon as possible.
   */
  public static final Ordering<DataSegment> SEGMENT_COMPARATOR_RECENT_FIRST = Ordering
      .from(Comparators.intervalsByEndThenStart())
      .onResultOf(DataSegment::getInterval)
      .compound(Ordering.<DataSegment>natural())
      .reverse();

  private static final EmittingLogger log = new EmittingLogger(DruidCoordinator.class);

  private final Object lock = new Object();
  private final DruidCoordinatorConfig config;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentsMetadataManager;
  private final ServerInventoryView serverInventoryView;
  private final MetadataRuleManager metadataRuleManager;

  private final ServiceEmitter emitter;
  private final IndexingServiceClient indexingServiceClient;
  private final ScheduledExecutorService exec;
  private final LoadQueueTaskMaster taskMaster;
  private final ConcurrentHashMap<String, LoadQueuePeon> loadManagementPeons
      = new ConcurrentHashMap<>();
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServiceAnnouncer serviceAnnouncer;
  private final DruidNode self;
  private final Set<CoordinatorDuty> indexingServiceDuties;
  private final Set<CoordinatorDuty> metadataStoreManagementDuties;
  private final CoordinatorCustomDutyGroups customDutyGroups;
  private final BalancerStrategyFactory factory;
  private final LookupCoordinatorManager lookupCoordinatorManager;
  private final DruidLeaderSelector coordLeaderSelector;
  private final CompactSegments compactSegments;

  private volatile boolean started = false;
  private volatile SegmentReplicantLookup segmentReplicantLookup = null;
  private volatile DruidCluster cluster = null;

  private int cachedBalancerThreadNumber;
  private ListeningExecutorService balancerExec;

  public static final String HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP = "HistoricalManagementDuties";
  private static final String METADATA_STORE_MANAGEMENT_DUTIES_DUTY_GROUP = "MetadataStoreManagementDuties";
  private static final String INDEXING_SERVICE_DUTIES_DUTY_GROUP = "IndexingServiceDuties";
  private static final String COMPACT_SEGMENTS_DUTIES_DUTY_GROUP = "CompactSegmentsDuties";

  @Inject
  public DruidCoordinator(
      DruidCoordinatorConfig config,
      JacksonConfigManager configManager,
      SegmentsMetadataManager segmentsMetadataManager,
      ServerInventoryView serverInventoryView,
      MetadataRuleManager metadataRuleManager,
      ServiceEmitter emitter,
      ScheduledExecutorFactory scheduledExecutorFactory,
      IndexingServiceClient indexingServiceClient,
      LoadQueueTaskMaster taskMaster,
      SegmentLoadQueueManager loadQueueManager,
      ServiceAnnouncer serviceAnnouncer,
      @Self DruidNode self,
      @CoordinatorMetadataStoreManagementDuty Set<CoordinatorDuty> metadataStoreManagementDuties,
      @CoordinatorIndexingServiceDuty Set<CoordinatorDuty> indexingServiceDuties,
      CoordinatorCustomDutyGroups customDutyGroups,
      BalancerStrategyFactory factory,
      LookupCoordinatorManager lookupCoordinatorManager,
      @Coordinator DruidLeaderSelector coordLeaderSelector,
      CompactionSegmentSearchPolicy compactionSegmentSearchPolicy
  )
  {
    this.config = config;
    this.configManager = configManager;

    this.segmentsMetadataManager = segmentsMetadataManager;
    this.serverInventoryView = serverInventoryView;
    this.metadataRuleManager = metadataRuleManager;
    this.emitter = emitter;
    this.indexingServiceClient = indexingServiceClient;
    this.taskMaster = taskMaster;
    this.serviceAnnouncer = serviceAnnouncer;
    this.self = self;
    this.indexingServiceDuties = indexingServiceDuties;
    this.metadataStoreManagementDuties = metadataStoreManagementDuties;
    this.customDutyGroups = customDutyGroups;

    this.exec = scheduledExecutorFactory.create(1, "Coordinator-Exec--%d");

    this.factory = factory;
    this.lookupCoordinatorManager = lookupCoordinatorManager;
    this.coordLeaderSelector = coordLeaderSelector;
    this.compactSegments = initializeCompactSegmentsDuty(compactionSegmentSearchPolicy);
    this.loadQueueManager = loadQueueManager;
  }

  public boolean isLeader()
  {
    return coordLeaderSelector.isLeader();
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  /**
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTier()
  {
    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    return computeUnderReplicationCountsPerDataSourcePerTierForSegmentsInternal(dataSegments, false);
  }

  /**
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTierUsingClusterView()
  {
    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();
    return computeUnderReplicationCountsPerDataSourcePerTierForSegmentsInternal(dataSegments, true);
  }

  /**
   * segmentReplicantLookup use in this method could potentially be stale since it is only updated on coordinator runs.
   * However, this is ok as long as the {@param dataSegments} is refreshed/latest as this would at least still ensure
   * that the stale data in segmentReplicantLookup would be under counting replication levels,
   * rather than potentially falsely reporting that everything is available.
   *
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTierForSegments(
      Iterable<DataSegment> dataSegments
  )
  {
    return computeUnderReplicationCountsPerDataSourcePerTierForSegmentsInternal(dataSegments, false);
  }

  /**
   * segmentReplicantLookup or cluster use in this method could potentially be stale since it is only updated on coordinator runs.
   * However, this is ok as long as the {@param dataSegments} is refreshed/latest as this would at least still ensure
   * that the stale data in segmentReplicantLookup and cluster would be under counting replication levels,
   * rather than potentially falsely reporting that everything is available.
   *
   * @return tier -> { dataSource -> underReplicationCount } map
   */
  public Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTierForSegmentsUsingClusterView(
      Iterable<DataSegment> dataSegments
  )
  {
    return computeUnderReplicationCountsPerDataSourcePerTierForSegmentsInternal(dataSegments, true);
  }

  public Object2IntMap<String> computeNumsUnavailableUsedSegmentsPerDataSource()
  {
    if (segmentReplicantLookup == null) {
      return Object2IntMaps.emptyMap();
    }

    final Object2IntOpenHashMap<String> numsUnavailableUsedSegmentsPerDataSource = new Object2IntOpenHashMap<>();

    final Iterable<DataSegment> dataSegments = segmentsMetadataManager.iterateAllUsedSegments();

    for (DataSegment segment : dataSegments) {
      if (segmentReplicantLookup.getTotalServedReplicas(segment.getId()) == 0) {
        numsUnavailableUsedSegmentsPerDataSource.addTo(segment.getDataSource(), 1);
      } else {
        numsUnavailableUsedSegmentsPerDataSource.addTo(segment.getDataSource(), 0);
      }
    }

    return numsUnavailableUsedSegmentsPerDataSource;
  }

  public Map<String, Double> getLoadStatus()
  {
    final Map<String, Double> loadStatus = new HashMap<>();
    final Collection<ImmutableDruidDataSource> dataSources =
        segmentsMetadataManager.getImmutableDataSourcesWithAllUsedSegments();

    for (ImmutableDruidDataSource dataSource : dataSources) {
      final Set<DataSegment> segments = Sets.newHashSet(dataSource.getSegments());
      final int numPublishedSegments = segments.size();

      // remove loaded segments
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        final DruidDataSource loadedView = druidServer.getDataSource(dataSource.getName());
        if (loadedView != null) {
          // This does not use segments.removeAll(loadedView.getSegments()) for performance reasons.
          // Please see https://github.com/apache/druid/pull/5632 and LoadStatusBenchmark for more info.
          for (DataSegment serverSegment : loadedView.getSegments()) {
            segments.remove(serverSegment);
          }
        }
      }
      final int numUnavailableSegments = segments.size();
      loadStatus.put(
          dataSource.getName(),
          100 * ((double) (numPublishedSegments - numUnavailableSegments) / (double) numPublishedSegments)
      );
    }

    return loadStatus;
  }

  @Nullable
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    return compactSegments.getTotalSizeOfSegmentsAwaitingCompaction(dataSource);
  }

  @Nullable
  public AutoCompactionSnapshot getAutoCompactionSnapshotForDataSource(String dataSource)
  {
    return compactSegments.getAutoCompactionSnapshot(dataSource);
  }

  public Map<String, AutoCompactionSnapshot> getAutoCompactionSnapshot()
  {
    return compactSegments.getAutoCompactionSnapshot();
  }

  public CoordinatorDynamicConfig getDynamicConfigs()
  {
    return CoordinatorDynamicConfig.current(configManager);
  }

  public CoordinatorCompactionConfig getCompactionConfig()
  {
    return CoordinatorCompactionConfig.current(configManager);
  }

  public void markSegmentsAsUnused(String datasource, Set<SegmentId> segmentIds)
  {
    log.debug("Marking [%d] segments of datasource [%s] as unused: %s", segmentIds.size(), datasource, segmentIds);
    int updatedCount = segmentsMetadataManager.markSegmentsAsUnused(segmentIds);
    log.info("Successfully marked [%d] segments of datasource [%s] as unused", updatedCount, datasource);
  }

  public String getCurrentLeader()
  {
    return coordLeaderSelector.getCurrentLeader();
  }

  @VisibleForTesting
  public int getCachedBalancerThreadNumber()
  {
    return cachedBalancerThreadNumber;
  }

  @VisibleForTesting
  public ListeningExecutorService getBalancerExec()
  {
    return balancerExec;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      coordLeaderSelector.registerListener(
          new DruidLeaderSelector.Listener()
          {
            @Override
            public void becomeLeader()
            {
              DruidCoordinator.this.becomeLeader();
            }

            @Override
            public void stopBeingLeader()
            {
              DruidCoordinator.this.stopBeingLeader();
            }
          }
      );
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      coordLeaderSelector.unregisterListener();

      started = false;

      exec.shutdownNow();

      if (balancerExec != null) {
        balancerExec.shutdownNow();
      }
    }
  }

  public void runCompactSegmentsDuty()
  {
    final int startingLeaderCounter = coordLeaderSelector.localTerm();
    DutiesRunnable compactSegmentsDuty = new DutiesRunnable(
        makeCompactSegmentsDuty(),
        startingLeaderCounter,
        COMPACT_SEGMENTS_DUTIES_DUTY_GROUP,
        null
    );
    compactSegmentsDuty.run();
  }

  private Map<String, Object2LongMap<String>> computeUnderReplicationCountsPerDataSourcePerTierForSegmentsInternal(
      Iterable<DataSegment> dataSegments,
      boolean computeUsingClusterView
  )
  {
    final Map<String, Object2LongMap<String>> underReplicationCountsPerDataSourcePerTier = new HashMap<>();

    if (segmentReplicantLookup == null) {
      return underReplicationCountsPerDataSourcePerTier;
    }

    if (computeUsingClusterView && cluster == null) {
      throw new ServiceUnavailableException(
          "coordinator hasn't populated information about cluster yet, try again later");
    }

    final DateTime now = DateTimes.nowUtc();

    for (final DataSegment segment : dataSegments) {
      final List<Rule> rules = metadataRuleManager.getRulesWithDefault(segment.getDataSource());

      for (final Rule rule : rules) {
        if (!rule.appliesTo(segment, now)) {
          // Rule did not match. Continue to the next Rule.
          continue;
        }
        if (!rule.canLoadSegments()) {
          // Rule matched but rule does not and cannot load segments.
          // Hence, there is no need to update underReplicationCountsPerDataSourcePerTier map
          break;
        }

        if (computeUsingClusterView) {
          rule.updateUnderReplicatedWithClusterView(
              underReplicationCountsPerDataSourcePerTier,
              segmentReplicantLookup,
              cluster,
              segment
          );
        } else {
          rule.updateUnderReplicated(underReplicationCountsPerDataSourcePerTier, segmentReplicantLookup, segment);
        }

        // Only the first matching rule applies. This is because the Coordinator cycle through all used segments
        // and match each segment with the first rule that applies. Each segment may only match a single rule.
        break;
      }
    }

    return underReplicationCountsPerDataSourcePerTier;
  }

  private void becomeLeader()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info(
          "I am the leader of the coordinators, all must bow! Starting coordination in [%s].",
          config.getCoordinatorStartDelay()
      );

      segmentsMetadataManager.startPollingDatabasePeriodically();
      metadataRuleManager.start();
      lookupCoordinatorManager.start();
      serviceAnnouncer.announce(self);
      final int startingLeaderCounter = coordLeaderSelector.localTerm();

      final List<DutiesRunnable> dutiesRunnables = new ArrayList<>();
      dutiesRunnables.add(
          new DutiesRunnable(
              makeHistoricalManagementDuties(),
              startingLeaderCounter,
              HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP,
              config.getCoordinatorPeriod()
          )
      );
      if (indexingServiceClient != null) {
        dutiesRunnables.add(
            new DutiesRunnable(
                makeIndexingServiceDuties(),
                startingLeaderCounter,
                INDEXING_SERVICE_DUTIES_DUTY_GROUP,
                config.getCoordinatorIndexingPeriod()
            )
        );
      }
      dutiesRunnables.add(
          new DutiesRunnable(
              makeMetadataStoreManagementDuties(),
              startingLeaderCounter,
              METADATA_STORE_MANAGEMENT_DUTIES_DUTY_GROUP,
              config.getCoordinatorMetadataStoreManagementPeriod()
          )
      );

      for (CoordinatorCustomDutyGroup customDutyGroup : customDutyGroups.getCoordinatorCustomDutyGroups()) {
        dutiesRunnables.add(
            new DutiesRunnable(
                customDutyGroup.getCustomDutyList(),
                startingLeaderCounter,
                customDutyGroup.getName(),
                customDutyGroup.getPeriod()
            )
        );
        log.info(
            "Done making custom coordinator duties %s for group %s",
            customDutyGroup.getCustomDutyList().stream()
                           .map(duty -> duty.getClass().getName()).collect(Collectors.toList()),
            customDutyGroup.getName()
        );
      }

      for (final DutiesRunnable dutiesRunnable : dutiesRunnables) {
        // CompactSegmentsDuty can takes a non trival amount of time to complete.
        // Hence, we schedule at fixed rate to make sure the other tasks still run at approximately every
        // config.getCoordinatorIndexingPeriod() period. Note that cautious should be taken
        // if setting config.getCoordinatorIndexingPeriod() lower than the default value.
        ScheduledExecutors.scheduleAtFixedRate(
            exec,
            config.getCoordinatorStartDelay(),
            dutiesRunnable.getPeriod(),
            () -> {
              if (coordLeaderSelector.isLeader()
                  && startingLeaderCounter == coordLeaderSelector.localTerm()) {
                dutiesRunnable.run();
              }

              // Check if we are still leader before re-scheduling
              if (coordLeaderSelector.isLeader()
                  && startingLeaderCounter == coordLeaderSelector.localTerm()) {
                return ScheduledExecutors.Signal.REPEAT;
              } else {
                return ScheduledExecutors.Signal.STOP;
              }
            }
        );
      }
    }
  }

  private void stopBeingLeader()
  {
    synchronized (lock) {

      log.info("I am no longer the leader...");

      for (String server : loadManagementPeons.keySet()) {
        LoadQueuePeon peon = loadManagementPeons.remove(server);
        peon.stop();
      }
      loadManagementPeons.clear();

      serviceAnnouncer.unannounce(self);
      lookupCoordinatorManager.stop();
      metadataRuleManager.stop();
      segmentsMetadataManager.stopPollingDatabasePeriodically();

      if (balancerExec != null) {
        balancerExec.shutdownNow();
        balancerExec = null;
      }
    }
  }

  @VisibleForTesting
  protected void initBalancerExecutor()
  {
    final int currentNumber = getDynamicConfigs().getBalancerComputeThreads();

    if (balancerExec == null) {
      balancerExec = createNewBalancerExecutor(currentNumber);
    } else if (cachedBalancerThreadNumber != currentNumber) {
      log.info(
          "balancerComputeThreads has changed from [%d] to [%d], recreating the thread pool.",
          cachedBalancerThreadNumber,
          currentNumber
      );
      balancerExec.shutdownNow();
      balancerExec = createNewBalancerExecutor(currentNumber);
    }
  }

  private ListeningExecutorService createNewBalancerExecutor(int numThreads)
  {
    cachedBalancerThreadNumber = numThreads;
    return MoreExecutors.listeningDecorator(
        Execs.multiThreaded(numThreads, "coordinator-cost-balancer-%s")
    );
  }

  private List<CoordinatorDuty> makeHistoricalManagementDuties()
  {
    return ImmutableList.of(
        new LogUsedSegments(),
        new UpdateCoordinatorStateAndPrepareCluster(),
        new RunRules(loadQueueManager),
        new UnloadUnusedSegments(loadQueueManager),
        new MarkAsUnusedOvershadowedSegments(DruidCoordinator.this),
        new BalanceSegments(loadQueueManager),
        new CollectSegmentAndServerStats(DruidCoordinator.this)
    );
  }

  @VisibleForTesting
  List<CoordinatorDuty> makeIndexingServiceDuties()
  {
    List<CoordinatorDuty> duties = new ArrayList<>();
    duties.add(new LogUsedSegments());
    duties.addAll(indexingServiceDuties);
    // CompactSegmentsDuty should be the last duty as it can take a long time to complete
    // We do not have to add compactSegments if it is already enabled in the custom duty group
    if (getCompactSegmentsDutyFromCustomGroups().isEmpty()) {
      duties.addAll(makeCompactSegmentsDuty());
    }
    log.debug(
        "Done making indexing service duties %s",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return ImmutableList.copyOf(duties);
  }

  private List<CoordinatorDuty> makeMetadataStoreManagementDuties()
  {
    List<CoordinatorDuty> duties = ImmutableList.copyOf(metadataStoreManagementDuties);
    log.debug(
        "Done making metadata store management duties %s",
        duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList())
    );
    return duties;
  }

  @VisibleForTesting
  CompactSegments initializeCompactSegmentsDuty(CompactionSegmentSearchPolicy compactionSegmentSearchPolicy)
  {
    List<CompactSegments> compactSegmentsDutyFromCustomGroups = getCompactSegmentsDutyFromCustomGroups();
    if (compactSegmentsDutyFromCustomGroups.isEmpty()) {
      return new CompactSegments(config, compactionSegmentSearchPolicy, indexingServiceClient);
    } else {
      if (compactSegmentsDutyFromCustomGroups.size() > 1) {
        log.warn(
            "More than one compactSegments duty is configured in the Coordinator Custom Duty Group."
            + " The first duty will be picked up."
        );
      }
      return compactSegmentsDutyFromCustomGroups.get(0);
    }
  }

  @VisibleForTesting
  List<CompactSegments> getCompactSegmentsDutyFromCustomGroups()
  {
    return customDutyGroups.getCoordinatorCustomDutyGroups()
                           .stream()
                           .flatMap(coordinatorCustomDutyGroup ->
                                        coordinatorCustomDutyGroup.getCustomDutyList().stream())
                           .filter(duty -> duty instanceof CompactSegments)
                           .map(duty -> (CompactSegments) duty)
                           .collect(Collectors.toList());
  }

  private List<CoordinatorDuty> makeCompactSegmentsDuty()
  {
    return ImmutableList.of(compactSegments);
  }

  private class DutiesRunnable implements Runnable
  {
    private final long startTimeNanos = System.nanoTime();
    private final List<? extends CoordinatorDuty> duties;
    private final int startingLeaderCounter;
    private final String dutyGroupName;
    private final Duration period;

    DutiesRunnable(
        List<? extends CoordinatorDuty> duties,
        final int startingLeaderCounter,
        String alias,
        Duration period
    )
    {
      this.duties = duties;
      this.startingLeaderCounter = startingLeaderCounter;
      this.dutyGroupName = alias;
      this.period = period;
    }

    @Override
    public void run()
    {
      try {
        log.info("Starting coordinator run for group [%s]", dutyGroupName);
        final long globalStart = System.currentTimeMillis();

        synchronized (lock) {
          if (!coordLeaderSelector.isLeader()) {
            log.info("LEGGO MY EGGO. [%s] is leader.", coordLeaderSelector.getCurrentLeader());
            stopBeingLeader();
            return;
          }
        }

        List<Boolean> allStarted = Arrays.asList(
            segmentsMetadataManager.isPollingDatabasePeriodically(),
            serverInventoryView.isStarted()
        );
        for (Boolean aBoolean : allStarted) {
          if (!aBoolean) {
            log.error("InventoryManagers not started[%s]", allStarted);
            stopBeingLeader();
            return;
          }
        }

        // Do coordinator stuff.
        DataSourcesSnapshot dataSourcesSnapshot = segmentsMetadataManager.getSnapshotOfDataSourcesWithAllUsedSegments();

        DruidCoordinatorRuntimeParams params =
            DruidCoordinatorRuntimeParams
                .newBuilder()
                .withDatabaseRuleManager(metadataRuleManager)
                .withStartTimeNanos(startTimeNanos)
                .withSnapshotOfDataSourcesWithAllUsedSegments(dataSourcesSnapshot)
                .withDynamicConfigs(getDynamicConfigs())
                .withCompactionConfig(getCompactionConfig())
                .withEmitter(emitter)
                .build();

        boolean coordinationPaused = getDynamicConfigs().getPauseCoordination();
        if (coordinationPaused
            && coordLeaderSelector.isLeader()
            && startingLeaderCounter == coordLeaderSelector.localTerm()) {

          log.info("Coordination has been paused. Duties will not run until coordination is resumed.");
        }

        for (CoordinatorDuty duty : duties) {
          // Don't read state and run state in the same duty otherwise racy conditions may exist
          if (!coordinationPaused
              && coordLeaderSelector.isLeader()
              && startingLeaderCounter == coordLeaderSelector.localTerm()) {

            final long start = System.currentTimeMillis();
            params = duty.run(params);
            final long end = System.currentTimeMillis();

            final String dutyName = duty.getClass().getName();
            if (params == null) {
              log.info("Finishing coordinator run since duty [%s] requested to stop run.", dutyName);
              return;
            } else {
              final RowKey rowKey = RowKey.builder().add(Dimension.DUTY, dutyName).build();
              params.getCoordinatorStats().add(Stats.Run.DUTY_TIME, rowKey, end - start);
            }
          }
        }

        // Emit stats collected from all duties
        CoordinatorRunStats allStats = params.getCoordinatorStats();
        if (allStats.rowCount() > 0) {
          final AtomicInteger emittedCount = new AtomicInteger();
          allStats.forEachStat(
              (dimensionValues, stat, value) -> {
                if (stat.shouldEmit()) {
                  emitStat(stat, dimensionValues, value);
                  emittedCount.incrementAndGet();
                }
              }
          );
          log.info(
              "Collected [%d] rows of stats for group [%s]. Emitted [%d] stats.",
              allStats.rowCount(), dutyGroupName, emittedCount.get()
          );
        }

        // Emit the runtime of the full DutiesRunnable
        final long runMillis = System.currentTimeMillis() - globalStart;
        emitStat(Stats.Run.TOTAL_TIME, Collections.emptyMap(), runMillis);
        log.info("Finished coordinator run for group [%s] in [%d] ms", dutyGroupName, runMillis);
      }
      catch (Exception e) {
        log.makeAlert(e, "Caught exception, ignoring so that schedule keeps going.").emit();
      }
    }

    private void emitStat(CoordinatorStat stat, Map<Dimension, String> dimensionValues, long value)
    {
      if (stat.equals(Stats.Balancer.NORMALIZED_COST_X_1000)) {
        value = value / 1000;
      }

      ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder()
          .setDimension(Dimension.DUTY_GROUP.reportedName(), dutyGroupName);
      dimensionValues.forEach(
          (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
      );
      emitter.emit(eventBuilder.build(stat.getMetricName(), value));
    }

    Duration getPeriod()
    {
      return period;
    }

    @Override
    public String toString()
    {
      return "DutiesRunnable{group='" + dutyGroupName + '\'' + '}';
    }
  }

  /**
   * Updates the enclosing {@link DruidCoordinator}'s state and prepares an immutable view of the cluster state (which
   * consists of {@link DruidCluster} and {@link SegmentReplicantLookup}) and feeds it into {@link
   * DruidCoordinatorRuntimeParams} for use in subsequent {@link CoordinatorDuty}s (see the order in {@link
   * #makeHistoricalManagementDuties()}).
   */
  private class UpdateCoordinatorStateAndPrepareCluster implements CoordinatorDuty
  {
    @Nullable
    @Override
    public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
    {
      List<ImmutableDruidServer> currentServers = prepareCurrentServers();

      startPeonsForNewServers(currentServers);
      stopPeonsForDisappearedServers(currentServers);

      cluster = prepareCluster(params.getCoordinatorDynamicConfig(), currentServers);
      cancelLoadsOnDecommissioningServers(cluster);

      final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
      segmentReplicantLookup =
          SegmentReplicantLookup.make(cluster, dynamicConfig.getReplicateAfterLoadTimeout());

      final Map<String, String> debugDimensions = dynamicConfig.getDebugDimensions();

      initBalancerExecutor();
      final BalancerStrategy balancerStrategy;
      if (debugDimensions != null && debugDimensions.containsKey("strategy")) {
        balancerStrategy = new UniformIntervalBalancerStrategy();
      } else {
        balancerStrategy = factory.createBalancerStrategy(balancerExec);
      }
      log.info(
          "Created balancer strategy [%s], round-robin assignment is [%s], debug dimensions are [%s].",
          balancerStrategy.getClass().getSimpleName(),
          dynamicConfig.isUseRoundRobinSegmentAssignment(),
          dynamicConfig.getDebugDimensions()
      );

      return params.buildFromExisting()
                   .withDruidCluster(cluster)
                   .withBalancerStrategy(balancerStrategy)
                   .withSegmentReplicantLookup(segmentReplicantLookup)
                   .build();
    }

    /**
     * Cancels all load/move operations on decommissioning servers. This is done
     * before initializing the SegmentReplicantLookup so that under-replicated
     * segments can be assigned in the current run itself.
     */
    private void cancelLoadsOnDecommissioningServers(DruidCluster cluster)
    {
      final AtomicInteger cancelledCount = new AtomicInteger(0);
      cluster.getAllServers().stream().filter(ServerHolder::isDecommissioning).forEach(
          server -> server.getQueuedSegments().forEach(
              (segment, action) -> {
                // Cancel the operation if it is a type of load action
                if (action.isLoad() && server.cancelOperation(action, segment)) {
                  cancelledCount.incrementAndGet();
                }
              }
          )
      );
      if (cancelledCount.get() > 0) {
        log.info("Cancelled [%d] load/move operations on decommissioning servers.", cancelledCount.get());
      }
    }

    List<ImmutableDruidServer> prepareCurrentServers()
    {
      List<ImmutableDruidServer> currentServers = serverInventoryView
          .getInventory()
          .stream()
          .filter(DruidServer::isSegmentReplicationOrBroadcastTarget)
          .map(DruidServer::toImmutableDruidServer)
          .collect(Collectors.toList());

      if (log.isDebugEnabled()) {
        // Display info about all segment-replicatable (historical and bridge) servers
        log.debug("Servers");
        for (ImmutableDruidServer druidServer : currentServers) {
          log.debug("  %s", druidServer);
          log.debug("    -- DataSources");
          for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
            log.debug("    %s", druidDataSource);
          }
        }
      }
      return currentServers;
    }

    void startPeonsForNewServers(List<ImmutableDruidServer> currentServers)
    {
      for (ImmutableDruidServer server : currentServers) {
        loadManagementPeons.computeIfAbsent(server.getName(), serverName -> {
          LoadQueuePeon loadQueuePeon = taskMaster.giveMePeon(server);
          loadQueuePeon.start();
          log.debug("Created LoadQueuePeon for server[%s].", server.getName());
          return loadQueuePeon;
        });
      }
    }

    DruidCluster prepareCluster(CoordinatorDynamicConfig dynamicConfig, List<ImmutableDruidServer> currentServers)
    {
      final Set<String> decommissioningServers = dynamicConfig.getDecommissioningNodes();
      final DruidCluster.Builder cluster = DruidCluster.builder();
      for (ImmutableDruidServer server : currentServers) {
        cluster.add(
            new ServerHolder(
                server,
                loadManagementPeons.get(server.getName()),
                decommissioningServers.contains(server.getHost()),
                dynamicConfig.getMaxSegmentsInNodeLoadingQueue(),
                dynamicConfig.getReplicantLifetime()
            )
        );
      }
      return cluster.build();
    }

    void stopPeonsForDisappearedServers(List<ImmutableDruidServer> servers)
    {
      final Set<String> disappeared = Sets.newHashSet(loadManagementPeons.keySet());
      for (ImmutableDruidServer server : servers) {
        disappeared.remove(server.getName());
      }
      for (String name : disappeared) {
        log.debug("Removing listener for server[%s] which is no longer there.", name);
        LoadQueuePeon peon = loadManagementPeons.remove(name);
        peon.stop();
      }
    }
  }
}

