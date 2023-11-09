package org.apache.druid.server.coordinator.rules;

import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Period;

/**
 * Builder for a {@link BroadcastDistributionRule}.
 */
public class Broadcast
{
  private static final BroadcastDistributionRule FOREVER = new ForeverBroadcastDistributionRule();

  public static BroadcastDistributionRule forever()
  {
    return FOREVER;
  }

  public static BroadcastDistributionRule forInterval(String interval)
  {
    return new IntervalBroadcastDistributionRule(Intervals.of(interval));
  }

  public static BroadcastDistributionRule forPeriod(long duration)
  {
    return new PeriodBroadcastDistributionRule(new Period(duration), null);
  }
}
