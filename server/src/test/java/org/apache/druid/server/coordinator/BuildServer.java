package org.apache.druid.server.coordinator;

import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class BuildServer
{
  private static final AtomicLong SERVER_ID = new AtomicLong(0);

  private final ServerType type;
  private final Set<DataSegment> segments = new HashSet<>();

  private String name;
  private String hostAndPort;
  private String tier = DruidServer.DEFAULT_TIER;

  /**
   * Default size of 10GB.
   */
  private long size = 10L << 30;

  private boolean decommissioning = false;

  private BuildServer(ServerType type)
  {
    this.type = type;
  }

  public static BuildServer ofType(ServerType type)
  {
    return new BuildServer(type);
  }

  public BuildServer name(String name)
  {
    this.name = name;
    return this;
  }

  public BuildServer tier(String tier)
  {
    this.tier = tier;
    return this;
  }

  public BuildServer hostAndPort(String hostAndPort)
  {
    this.hostAndPort = hostAndPort;
    return this;
  }

  public BuildServer decommissioning()
  {
    this.decommissioning = true;
    return this;
  }

  public BuildServer withSegment(DataSegment segment)
  {
    this.segments.add(segment);
    return this;
  }

  public BuildServer withSegments(DataSegment... segments)
  {
    return withSegments(Arrays.asList(segments));
  }

  public BuildServer withSegments(Collection<DataSegment> segments)
  {
    this.segments.addAll(segments);
    return this;
  }

  public BuildServer sizeInGb(long sizeInGb)
  {
    this.size = sizeInGb << 30;
    return this;
  }

  public DruidServer buildServer()
  {
    name = name == null ? type.name() + "_" + SERVER_ID.incrementAndGet() : name;
    hostAndPort = hostAndPort == null ? name : hostAndPort;

    final DruidServer server = new DruidServer(name, hostAndPort, null, size, type, tier, 1);
    for (DataSegment segment : segments) {
      server.addDataSegment(segment);
    }

    return server;
  }

  public ImmutableDruidServer asImmutable()
  {
    return buildServer().toImmutableDruidServer();
  }

  public ServerHolder asHolder()
  {
    return new ServerHolder(
        asImmutable(),
        new TestLoadQueuePeon(),
        decommissioning
    );
  }
}
