package org.apache.druid.java.util.common;

import org.joda.time.Instant;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 * Wrapper over an {@link Interval} to allow building other intervals from it
 * used in tests.
 */
public class IntervalBuilder
{
  private final Interval baseInterval;

  private IntervalBuilder(Interval baseInterval)
  {
    this.baseInterval = baseInterval;
  }

  public static IntervalBuilder withBase(Interval baseInterval)
  {
    return new IntervalBuilder(baseInterval);
  }

  public Interval startingAt(String start)
  {
    return baseInterval.withStart(new Instant(start));
  }

  public Interval endingAt(String end)
  {
    return baseInterval.withEnd(new Instant(end));
  }

  public Interval endingAfter(String period)
  {
    return baseInterval.withPeriodAfterStart(new Period(period));
  }

  public Interval startingBefore(String period)
  {
    return baseInterval.withPeriodBeforeEnd(new Period(period));
  }

}
