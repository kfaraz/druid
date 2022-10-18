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

import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Provides iterators over servers for a given tier that can load a specified
 * segment.
 * <p>
 * Once a selector is initialized with a {@link DruidCluster}, the iterator
 * returned by {@link #getServersInTierToLoadSegment(String, DataSegment)}
 * iterates over the servers in a tier in a round robin fashion. The next
 * invocation of this method picks up where the last iterator had left off.
 * <p>
 * This class is not thread-safe and must be used from a single thread.
 */
public class RoundRobinServerSelector
{
  private final Map<String, CircularServerIterator> tierToServers = new HashMap<>();

  public RoundRobinServerSelector(DruidCluster cluster)
  {
    cluster.getHistoricals().forEach(
        (tier, servers) -> tierToServers.put(tier, new CircularServerIterator(servers))
    );
  }

  /**
   * Returns an iterator over the servers in this tier which are eligible to
   * load the given segment.
   */
  public Iterator<ServerHolder> getServersInTierToLoadSegment(String tier, DataSegment segment)
  {
    final CircularServerIterator iterator = tierToServers.get(tier);
    if (iterator == null) {
      return Collections.emptyIterator();
    }

    return new EligibleServerIterator(segment, iterator);
  }

  /**
   * Iterator over servers in a tier that are eligible to load a given segment.
   */
  private static class EligibleServerIterator implements Iterator<ServerHolder>
  {
    final CircularServerIterator delegate;
    final DataSegment segment;

    ServerHolder nextEligible;

    EligibleServerIterator(DataSegment segment, CircularServerIterator delegate)
    {
      this.delegate = delegate;
      this.segment = segment;

      delegate.reset();
      this.nextEligible = search();
    }

    @Override
    public boolean hasNext()
    {
      return nextEligible != null;
    }

    @Override
    public ServerHolder next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      ServerHolder previous = nextEligible;
      nextEligible = search();
      return previous;
    }

    ServerHolder search()
    {
      while (delegate.hasNext()) {
        ServerHolder nextServer = delegate.next();
        if (nextServer.canLoadSegment(segment)) {
          return nextServer;
        }
      }

      return null;
    }
  }

  /**
   * Circular iterator over all servers in a tier. A single instance of this is
   * maintained for each tier.
   */
  private static class CircularServerIterator implements Iterator<ServerHolder>
  {
    final List<ServerHolder> servers = new ArrayList<>();
    int currentPosition;
    int searchLength = 0;

    CircularServerIterator(Set<ServerHolder> servers)
    {
      this.servers.addAll(servers);
      this.currentPosition = servers.size() - 1;
      //Collections.shuffle(this.servers);
    }

    void reset()
    {
      searchLength = 0;
      if (--currentPosition < 0) {
        currentPosition = servers.size() - 1;
      }
    }

    @Override
    public boolean hasNext()
    {
      return servers.size() > searchLength;
    }

    @Override
    public ServerHolder next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      ++searchLength;
      return servers.get(getNextPosition());
    }

    private int getNextPosition()
    {
      if (++currentPosition >= servers.size()) {
        currentPosition = 0;
      }
      return currentPosition;
    }
  }

}
