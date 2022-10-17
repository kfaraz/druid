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

public class RoundRobinServerSelector
{
  private final Map<String, CircularServerIterator> tierToServers = new HashMap<>();

  public RoundRobinServerSelector(DruidCluster cluster)
  {
    cluster.getHistoricals().forEach(
        (tier, servers) -> tierToServers.put(tier, new CircularServerIterator(servers))
    );
  }

  public Iterator<ServerHolder> getServersInTierToLoadSegment(String tier, DataSegment segment)
  {
    final CircularServerIterator iterator = tierToServers.get(tier);
    if (iterator == null) {
      return Collections.emptyIterator();
    }

    iterator.reset();
    return iterator;
  }

  private static class CircularServerIterator implements Iterator<ServerHolder>
  {
    final List<ServerHolder> servers = new ArrayList<>();
    int currentPosition = -1;
    int searchLength = 0;

    CircularServerIterator(Set<ServerHolder> servers)
    {
      this.servers.addAll(servers);
      //Collections.shuffle(this.servers);
    }

    void reset()
    {
      searchLength = 0;
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
