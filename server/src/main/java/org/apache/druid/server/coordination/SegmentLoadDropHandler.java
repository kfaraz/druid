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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.ServerTypeConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.SegmentRowCountDistribution;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
@ManageLifecycle
public class SegmentLoadDropHandler implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoadDropHandler.class);

  /**
   * Synchronizes addition and removal from {@link #segmentsToDrop} to ensure that
   * concurrent Load and Drop of the same segment do not result in an invalid state.
   * <p>
   * For a given segment, the latest request handled by {@link #processRequest}
   * is the one which is honored.
   * <p>
   * Possible cases:
   * <ol>
   * <li>
   * Load after Drop:
   * <ul>
   * <li>Drop started before Load is queued: Load must wait for Drop to finish.</li>
   * <li>Drop started after Load is queued: There is nothing to do as Drop would not be
   * processed anyway due to the segment not being present in {@link #segmentsToDrop}</li>
   * </ul>
   * </li>
   * <li>Drop after Load: the Load must exit as soon as it realizes that the
   * segment is now marked for Drop and should not be loaded or announced anymore.</li>
   * </ol>
   */
  private final Object segmentDropLock = new Object();

  /**
   * Synchronizes start/stop of the SegmentLoadDropHandler.
   */
  private final Object startStopLock = new Object();

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentManager segmentManager;
  private final ScheduledExecutorService exec;
  private final ServerTypeConfig serverTypeConfig;
  private final ConcurrentSkipListSet<DataSegment> segmentsToDrop;
  private final SegmentCacheManager segmentCacheManager;

  private volatile boolean started = false;

  /**
   * Used to cache the status of a completed load or drop request until it has
   * been served to the (Coordinator) client exactly once.
   * <p>
   * The cache is used as follows:
   * <ol>
   * <li>An entry with state PENDING is added to the cache upon receiving a
   * request to load or drop a segment.</li>
   * <li>A duplicate request received at this point is immediately answered with PENDING.</li>
   * <li>Once the load/drop finishes, the entry is updated to either SUCCESS or FAILED.</li>
   * <li>A duplicate request received at this point is immediately answered with
   * SUCCESS or FAILED and the entry is removed from the cache.</li>
   * <li>If the original request itself finishes after the load or drop has already
   * completed, it is answered with a SUCCESS or FAILED and the entry is removed
   * from the cache.</li>
   * <li>If a request of a different type (e.g. load after drop) is received,
   * the entry from the cache is removed and the previous request is abandoned.</li>
   * </ol>
   * <p>
   * Maximum size of this cache must be significantly greater than the number of
   * pending load/drop requests. This is generally already the case because the
   * Coordinator sends load/drop requests in small batches and does not send new
   * requests until the previously submitted ones have either succeeded or failed.
   * <p>
   * The cache must be updated in a thread-safe manner so that stale statuses
   * are not served.
   */
  @GuardedBy("requestStatusesLock")
  private final Cache<DataSegment, AtomicReference<DataSegmentChangeResponse>> requestStatuses;
  private final Object requestStatusesLock = new Object();

  /**
   * List of unresolved futures returned to callers of {@link #processBatch}.
   * Each {@link CustomSettableFuture} corresponds to a single batch of requests.
   * A future is resolved as soon as a single request in the batch completes with
   * either success or failure.
   */
  private final LinkedHashSet<CustomSettableFuture> waitingFutures = new LinkedHashSet<>();

  @Inject
  public SegmentLoadDropHandler(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      SegmentCacheManager segmentCacheManager,
      ServerTypeConfig serverTypeConfig
  )
  {
    this(
        jsonMapper,
        config,
        announcer,
        serverAnnouncer,
        segmentManager,
        segmentCacheManager,
        Executors.newScheduledThreadPool(
            config.getNumLoadingThreads(),
            Execs.makeThreadFactory("SimpleDataSegmentChangeHandler-%s")
        ),
        serverTypeConfig
    );
  }

  @VisibleForTesting
  SegmentLoadDropHandler(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      DataSegmentAnnouncer announcer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentManager segmentManager,
      SegmentCacheManager segmentCacheManager,
      ScheduledExecutorService exec,
      ServerTypeConfig serverTypeConfig
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.announcer = announcer;
    this.serverAnnouncer = serverAnnouncer;
    this.segmentManager = segmentManager;
    this.segmentCacheManager = segmentCacheManager;
    this.exec = exec;
    this.serverTypeConfig = serverTypeConfig;

    this.segmentsToDrop = new ConcurrentSkipListSet<>();
    this.requestStatuses = CacheBuilder.newBuilder()
                                       .maximumSize(config.getStatusQueueMaxSize())
                                       .initialCapacity(8)
                                       .build();
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (startStopLock) {
      if (started) {
        return;
      }

      final Stopwatch stopwatch = Stopwatch.createStarted();
      log.info("Starting SegmentLoadDropHandler...");
      try {
        if (!config.getLocations().isEmpty()) {
          loadLocalCache();
        }

        if (shouldAnnounce()) {
          serverAnnouncer.announce();
        }
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      started = true;
      log.info("Started SegmentLoadDropHandler in [%d]ms.", stopwatch.millisElapsed());
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!started) {
        return;
      }

      log.info("Stopping SegmentLoadDropHandler...");
      try {
        if (shouldAnnounce()) {
          serverAnnouncer.unannounce();
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        started = false;
      }
      log.info("Stopped SegmentLoadDropHandler.");
    }
  }

  public boolean isStarted()
  {
    return started;
  }

  private void loadLocalCache() throws IOException
  {
    File baseDir = config.getInfoDir();
    FileUtils.mkdirp(baseDir);

    List<DataSegment> cachedSegments = new ArrayList<>();
    File[] segmentsToLoad = baseDir.listFiles();
    int ignored = 0;
    for (int i = 0; i < segmentsToLoad.length; i++) {
      File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i + 1, segmentsToLoad.length, file);
      try {
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);

        if (!segment.getId().toString().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
          ignored++;
        } else if (segmentCacheManager.isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getId());

          File segmentInfoCacheFile = new File(baseDir, segment.getId().toString());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segmentInfo file")
           .addData("file", file)
           .emit();
      }
    }

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    loadCachedSegments(cachedSegments);
  }

  /**
   * Downloads a single segment and creates a cache file for it in the info dir.
   * If the load fails at any step, {@link #cleanupFailedLoad} is called.
   *
   * @throws SegmentLoadingException if the load fails
   */
  private void loadSegment(
      DataSegment segment,
      boolean lazy,
      @Nullable ExecutorService loadSegmentIntoPageCacheExec
  ) throws SegmentLoadingException
  {
    final boolean loaded;
    try {
      loaded = segmentManager.loadSegment(
          segment,
          lazy,
          () -> cleanupFailedLoad(segment),
          loadSegmentIntoPageCacheExec
      );
    }
    catch (Exception e) {
      cleanupFailedLoad(segment);
      throw new SegmentLoadingException(e, "Could not load segment: %s", e.getMessage());
    }

    if (loaded) {
      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
      if (!segmentInfoCacheFile.exists()) {
        try {
          jsonMapper.writeValue(segmentInfoCacheFile, segment);
        }
        catch (IOException e) {
          cleanupFailedLoad(segment);
          throw new SegmentLoadingException(
              e,
              "Failed to write to disk segment info cache file[%s]",
              segmentInfoCacheFile
          );
        }
      }
    }
  }

  public Map<String, Long> getAverageNumOfRowsPerSegmentForDatasource()
  {
    return segmentManager.getAverageRowCountForDatasource();
  }

  public Map<String, SegmentRowCountDistribution> getRowCountDistributionPerDatasource()
  {
    return segmentManager.getRowCountDistribution();
  }

  @Override
  public void addSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    // Unmark the segment for drop
    synchronized (segmentDropLock) {
      segmentsToDrop.remove(segment);
    }

    // Load and announce the segment asynchronously
    exec.submit(() -> loadAndAnnounceSegment(segment));
  }

  /**
   * Loads the segment synchronously, announces it and updates the status of the
   * corresponding change request in the {@link #requestStatuses} cache.
   */
  private void loadAndAnnounceSegment(DataSegment segment)
  {
    DataSegmentChangeResponse.Status result = null;
    try {
      log.info("Loading segment[%s]", segment.getId());

      // Do not start with Load if there is a Drop in progress. This prevents a Drop
      // that has come before Load of the same segment from causing partial success.
      synchronized (segmentDropLock) {
        if (!shouldLoadSegment(segment)) {
          return;
        }
      }

      // Do not load segment if it has already been marked for drop
      if (shouldLoadSegment(segment)) {
        loadSegment(segment, false, null);
      } else {
        return;
      }

      // Do not announce segment if it has already been marked for drop
      if (shouldLoadSegment(segment)) {
        announceSegment(segment);
      } else {
        return;
      }

      result = DataSegmentChangeResponse.Status.SUCCESS;
    }
    catch (Throwable e) {
      log.makeAlert(e, "Failed to load segment for dataSource")
         .addData("segment", segment)
         .emit();
      result = DataSegmentChangeResponse.Status.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestLoad(segment), result);
      resolveWaitingFutures();
    }
  }

  /**
   * Announces the given segment, regardless of whether the segment files already
   * existed or have been freshly downloaded.
   */
  private void announceSegment(DataSegment segment) throws SegmentLoadingException
  {
    try {
      announcer.announceSegment(segment);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getId());
    }
  }

  /**
   * Bulk adding segments during bootstrap
   */
  private void loadCachedSegments(Collection<DataSegment> segments)
  {
    // Start a temporary thread pool to load segments into page cache during bootstrap
    final Stopwatch stopwatch = Stopwatch.createStarted();
    ExecutorService loadingExecutor = null;
    ExecutorService loadSegmentsIntoPageCacheOnBootstrapExec =
        config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap() != 0 ?
        Execs.multiThreaded(config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
                            "Load-Segments-Into-Page-Cache-On-Bootstrap-%s") : null;
    try (final BackgroundSegmentAnnouncer backgroundSegmentAnnouncer =
             new BackgroundSegmentAnnouncer(announcer, exec, config.getAnnounceIntervalMillis())) {

      backgroundSegmentAnnouncer.startAnnouncing();

      loadingExecutor = Execs.multiThreaded(config.getNumBootstrapThreads(), "Segment-Load-Startup-%s");

      final int numSegments = segments.size();
      final CountDownLatch latch = new CountDownLatch(numSegments);
      final AtomicInteger counter = new AtomicInteger(0);
      final CopyOnWriteArrayList<DataSegment> failedSegments = new CopyOnWriteArrayList<>();
      for (final DataSegment segment : segments) {
        loadingExecutor.submit(
            () -> {
              try {
                log.info(
                    "Loading segment[%d/%d][%s]",
                    counter.incrementAndGet(), numSegments, segment.getId()
                );
                loadSegment(segment, config.isLazyLoadOnStart(), loadSegmentsIntoPageCacheOnBootstrapExec);
                try {
                  backgroundSegmentAnnouncer.announceSegment(segment);
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new SegmentLoadingException(e, "Loading Interrupted");
                }
              }
              catch (SegmentLoadingException e) {
                log.error(e, "[%s] failed to load", segment.getId());
                failedSegments.add(segment);
              }
              finally {
                latch.countDown();
              }
            }
        );
      }

      try {
        latch.await();

        if (failedSegments.size() > 0) {
          log.makeAlert("%,d errors seen while loading segments", failedSegments.size())
             .addData("failedSegments", failedSegments)
             .emit();
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.makeAlert(e, "LoadingInterrupted").emit();
      }

      backgroundSegmentAnnouncer.finishAnnouncing();
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segments -- likely problem with announcing.")
         .addData("numSegments", segments.size())
         .emit();
    }
    finally {
      log.info("Finished cache load in [%,d]ms", stopwatch.millisElapsed());
      if (loadingExecutor != null) {
        loadingExecutor.shutdownNow();
      }
      if (loadSegmentsIntoPageCacheOnBootstrapExec != null) {
        // At this stage, all tasks have been submitted, send a shutdown command to the bootstrap
        // thread pool so threads will exit after finishing the tasks
        loadSegmentsIntoPageCacheOnBootstrapExec.shutdown();
      }
    }
  }

  /**
   * Cleans up a failed LOAD request by completely removing the partially
   * downloaded segment files and unannouncing the segment for safe measure.
   */
  @VisibleForTesting
  void cleanupFailedLoad(DataSegment segment)
  {
    unannounceSegment(segment);
    synchronized (segmentDropLock) {
      segmentsToDrop.add(segment);
    }
    dropSegment(segment);
  }

  @Override
  public void removeSegment(DataSegment segment, @Nullable DataSegmentChangeCallback callback)
  {
    // Mark the segment for drop
    synchronized (segmentDropLock) {
      segmentsToDrop.add(segment);
    }

    unannounceSegment(segment);
    resolveWaitingFutures();

    // Schedule drop of segment
    log.info(
        "Completely removing segment[%s] in [%,d] millis.",
        segment.getId(), config.getDropSegmentDelayMillis()
    );
    exec.schedule(
        () -> dropSegment(segment),
        config.getDropSegmentDelayMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Unannounces the segment and updates the result for the corresponding DROP request.
   * A DROP request is considered successful if the unannouncement has succeeded,
   * even if the segment files have not been deleted yet.
   */
  private void unannounceSegment(final DataSegment segment)
  {
    DataSegmentChangeResponse.Status result = null;
    try {
      announcer.unannounceSegment(segment);
      result = DataSegmentChangeResponse.Status.SUCCESS;
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
      result = DataSegmentChangeResponse.Status.failed(e.getMessage());
    }
    finally {
      updateRequestStatus(new SegmentChangeRequestDrop(segment), result);
    }
  }

  /**
   * Drops the given segment synchronously.
   */
  private void dropSegment(DataSegment segment)
  {
    try {
      synchronized (segmentDropLock) {
        if (segmentsToDrop.remove(segment)) {
          segmentManager.dropSegment(segment);

          File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getId().toString());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to drop segment. Possible resource leak.")
         .addData("segment", segment)
         .emit();
    }
  }

  public Collection<DataSegment> getPendingDeleteSnapshot()
  {
    return ImmutableList.copyOf(segmentsToDrop);
  }

  /**
   * Processes a batch of segment load/drop requests.
   *
   * @return Future of List of results of each change request in the batch.
   * This future completes as soon as <i>any</i> pending request is completed by
   * this {@code SegmentLoadDropHandler}.
   */
  public ListenableFuture<List<DataSegmentChangeResponse>> processBatch(
      List<DataSegmentChangeRequest> changeRequests
  )
  {
    boolean isAnyRequestDone = false;

    Map<DataSegmentChangeRequest, AtomicReference<DataSegmentChangeResponse>> statuses
        = Maps.newHashMapWithExpectedSize(changeRequests.size());

    for (DataSegmentChangeRequest cr : changeRequests) {
      AtomicReference<DataSegmentChangeResponse> status = processRequest(cr);
      if (status.get().isComplete()) {
        isAnyRequestDone = true;
      }
      statuses.put(cr, status);
    }

    final CustomSettableFuture future = new CustomSettableFuture(statuses);
    if (isAnyRequestDone) {
      future.resolve();
    } else {
      synchronized (waitingFutures) {
        waitingFutures.add(future);
      }
    }

    return future;
  }

  /**
   * Starts the processing of the given segment change request.
   */
  private AtomicReference<DataSegmentChangeResponse> processRequest(DataSegmentChangeRequest changeRequest)
  {
    synchronized (requestStatusesLock) {
      final DataSegment segment = changeRequest.getSegment();
      final AtomicReference<DataSegmentChangeResponse> cachedResponse = requestStatuses.getIfPresent(segment);

      if (cachedResponse == null) {
        // Start a fresh LOAD or DROP as there is no previous known request
        markRequestAsPending(changeRequest);
        changeRequest.go(this, null);
        return requestStatuses.getIfPresent(segment);
      } else if (cachedResponse.get().getRequest().equals(changeRequest)) {
        // Serve the cached response and clear it if the request has completed,
        // so that we don't keep serving a stale response indefinitely
        if (cachedResponse.get().isComplete()) {
          requestStatuses.invalidate(segment);
        }
        return cachedResponse;
      } else {
        // Clear the cached response as this is a different request
        requestStatuses.invalidate(segment);

        markRequestAsPending(changeRequest);
        changeRequest.go(this, null);
        return requestStatuses.getIfPresent(segment);
      }
    }
  }

  @GuardedBy("requestStatusesLock")
  private void markRequestAsPending(DataSegmentChangeRequest changeRequest)
  {
    DataSegmentChangeResponse pendingResponse
        = new DataSegmentChangeResponse(changeRequest, DataSegmentChangeResponse.Status.PENDING);
    requestStatuses.put(changeRequest.getSegment(), new AtomicReference<>(pendingResponse));
  }

  /**
   * Returns true only if there is a Load request in {@link #requestStatuses} for
   * this segment, and the segment is not present in {@link #segmentsToDrop}.
   */
  private boolean shouldLoadSegment(DataSegment segment)
  {
    if (segmentsToDrop.contains(segment)) {
      return false;
    }

    synchronized (requestStatusesLock) {
      AtomicReference<DataSegmentChangeResponse> response = requestStatuses.getIfPresent(segment);
      return response != null && response.get().isLoadRequest();
    }
  }

  /**
   * Updates the status for the given request only if this request is currently
   * in progress and present in {@link #requestStatuses}.
   */
  private void updateRequestStatus(DataSegmentChangeRequest changeRequest, DataSegmentChangeResponse.Status status)
  {
    if (status == null) {
      status = DataSegmentChangeResponse.Status.failed("Unknown reason. Check server logs.");
    }
    synchronized (requestStatusesLock) {
      AtomicReference<DataSegmentChangeResponse> statusRef
          = requestStatuses.getIfPresent(changeRequest.getSegment());
      if (statusRef != null && statusRef.get().getRequest().equals(changeRequest)) {
        statusRef.set(new DataSegmentChangeResponse(changeRequest, status));
      }
    }
  }

  /**
   * Resolves waiting futures after a LOAD or DROP request has completed.
   */
  private void resolveWaitingFutures()
  {
    LinkedHashSet<CustomSettableFuture> waitingFuturesCopy;
    synchronized (waitingFutures) {
      waitingFuturesCopy = new LinkedHashSet<>(waitingFutures);
      waitingFutures.clear();
    }
    for (CustomSettableFuture future : waitingFuturesCopy) {
      future.resolve();
    }
  }

  /**
   * Returns whether or not we should announce ourselves as a data server using {@link DataSegmentServerAnnouncer}.
   *
   * Returns true if _either_:
   *
   * (1) Our {@link #serverTypeConfig} indicates we are a segment server. This is necessary for Brokers to be able
   * to detect that we exist.
   * (2) We have non-empty storage locations in {@link #config}. This is necessary for Coordinators to be able to
   * assign segments to us.
   */
  private boolean shouldAnnounce()
  {
    return serverTypeConfig.getServerType().isSegmentServer() || !config.getLocations().isEmpty();
  }

  private static class BackgroundSegmentAnnouncer implements AutoCloseable
  {
    private static final EmittingLogger log = new EmittingLogger(BackgroundSegmentAnnouncer.class);

    private final int intervalMillis;
    private final DataSegmentAnnouncer announcer;
    private final ScheduledExecutorService exec;
    private final LinkedBlockingQueue<DataSegment> queue;
    private final SettableFuture<Boolean> doneAnnouncing;

    private final Object lock = new Object();

    private volatile boolean finished = false;
    @Nullable
    private volatile ScheduledFuture startedAnnouncing = null;
    @Nullable
    private volatile ScheduledFuture nextAnnoucement = null;

    public BackgroundSegmentAnnouncer(
        DataSegmentAnnouncer announcer,
        ScheduledExecutorService exec,
        int intervalMillis
    )
    {
      this.announcer = announcer;
      this.exec = exec;
      this.intervalMillis = intervalMillis;
      this.queue = new LinkedBlockingQueue<>();
      this.doneAnnouncing = SettableFuture.create();
    }

    public void announceSegment(final DataSegment segment) throws InterruptedException
    {
      if (finished) {
        throw new ISE("Announce segment called after finishAnnouncing");
      }
      queue.put(segment);
    }

    public void startAnnouncing()
    {
      if (intervalMillis <= 0) {
        return;
      }

      log.info("Starting background segment announcing task");

      // schedule background announcing task
      nextAnnoucement = startedAnnouncing = exec.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              synchronized (lock) {
                try {
                  if (!(finished && queue.isEmpty())) {
                    final List<DataSegment> segments = new ArrayList<>();
                    queue.drainTo(segments);
                    try {
                      announcer.announceSegments(segments);
                      nextAnnoucement = exec.schedule(this, intervalMillis, TimeUnit.MILLISECONDS);
                    }
                    catch (IOException e) {
                      doneAnnouncing.setException(
                          new SegmentLoadingException(e, "Failed to announce segments[%s]", segments)
                      );
                    }
                  } else {
                    doneAnnouncing.set(true);
                  }
                }
                catch (Exception e) {
                  doneAnnouncing.setException(e);
                }
              }
            }
          },
          intervalMillis,
          TimeUnit.MILLISECONDS
      );
    }

    public void finishAnnouncing() throws SegmentLoadingException
    {
      synchronized (lock) {
        finished = true;
        // announce any remaining segments
        try {
          final List<DataSegment> segments = new ArrayList<>();
          queue.drainTo(segments);
          announcer.announceSegments(segments);
        }
        catch (Exception e) {
          throw new SegmentLoadingException(e, "Failed to announce segments[%s]", queue);
        }

        // get any exception that may have been thrown in background announcing
        try {
          // check in case intervalMillis is <= 0
          if (startedAnnouncing != null) {
            startedAnnouncing.cancel(false);
          }
          // - if the task is waiting on the lock, then the queue will be empty by the time it runs
          // - if the task just released it, then the lock ensures any exception is set in doneAnnouncing
          if (doneAnnouncing.isDone()) {
            doneAnnouncing.get();
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SegmentLoadingException(e, "Loading Interrupted");
        }
        catch (ExecutionException e) {
          throw new SegmentLoadingException(e.getCause(), "Background Announcing Task Failed");
        }
      }
      log.info("Completed background segment announcing");
    }

    @Override
    public void close()
    {
      // stop background scheduling
      synchronized (lock) {
        finished = true;
        if (nextAnnoucement != null) {
          nextAnnoucement.cancel(false);
        }
      }
    }
  }

  /**
   * Represents the future result of a single batch of segment load drop requests.
   * <p>
   * Upon cancellation, this future removes itself from {@link #waitingFutures}.
   */
  private class CustomSettableFuture extends AbstractFuture<List<DataSegmentChangeResponse>>
  {
    private final Map<DataSegmentChangeRequest, AtomicReference<DataSegmentChangeResponse>> resultRefs;

    private CustomSettableFuture(
        Map<DataSegmentChangeRequest, AtomicReference<DataSegmentChangeResponse>> resultRefs
    )
    {
      this.resultRefs = resultRefs;
    }

    public void resolve()
    {
      // Synchronize here to ensure thread-safety of (a) resolving this future
      // and (b) updating the requestStatuses cache
      synchronized (requestStatusesLock) {
        if (isDone()) {
          return;
        }

        final List<DataSegmentChangeResponse> results = new ArrayList<>(resultRefs.size());
        resultRefs.forEach((request, reference) -> {
          DataSegmentChangeResponse result = reference.get();
          results.add(result);

          // Remove complete statuses from the cache
          if (result != null && result.isComplete()) {
            requestStatuses.invalidate(request.getSegment());
          }
        });

        set(results);
      }
    }

    @Override
    public boolean cancel(boolean interruptIfRunning)
    {
      synchronized (waitingFutures) {
        waitingFutures.remove(this);
      }
      return true;
    }
  }

}

