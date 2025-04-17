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

package org.apache.druid.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

/**
 * Unit tests for {@link IndexerSQLMetadataStorageCoordinator} methods which
 * mark segments as used.
 * <p>
 * These tests have been kept out of {@link IndexerSQLMetadataStorageCoordinatorTest}
 * as that class is already too bloated.
 */
public class IndexerSQLMetadataStorageCoordinatorMarkUsedTest extends IndexerSqlMetadataStorageCoordinatorTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();
  
  private IndexerMetadataStorageCoordinator storageCoordinator;

  private final DataSegment wikiSegment1 =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).startingAt("2012-03-15").eachOfSizeInMb(500).get(0);
  private final DataSegment wikiSegment2 =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).startingAt("2012-01-05").eachOfSizeInMb(500).get(0);
  
  @Before
  public void setup()
  {
    storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        null,
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        null,
        CentralizedDatasourceSchemaConfig.create()
    );
  }
  
  @Test
  public void testmarkNonOvershadowedSegmentsAsUsed()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);
    final Set<SegmentId> segmentIds = Set.of(
        koalaSegment1.getId(),
        koalaSegment2.getId(),
        koalaSegment3.getId()
    );

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(2, storageCoordinator.markNonOvershadowedSegmentsAsUsed(TestDataSource.KOALA, segmentIds));
    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment1, koalaSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedInEternityIntervalWithVersions()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(
        2,
        storageCoordinator.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.KOALA,
            Intervals.ETERNITY,
            ImmutableList.of("2017-10-15T20:19:12.565Z", "2017-10-16T20:19:12.565Z")
        )
    );

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment1, koalaSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedWithEmptyVersions()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(
        0,
        storageCoordinator.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.KOALA,
            Intervals.of("2017/2018"),
            ImmutableList.of()
        )
    );

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedInEternityIntervalWithEmptyVersions()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(
        0,
        storageCoordinator.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.KOALA,
            Intervals.ETERNITY,
            ImmutableList.of()
        )
    );

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedInFiniteIntervalWithVersions()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(
        2,
        storageCoordinator.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.KOALA,
            Intervals.of("2017-10-15/2017-10-18"),
            ImmutableList.of("2017-10-15T20:19:12.565Z", "2017-10-16T20:19:12.565Z")
        )
    );
    

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment1, koalaSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedWithNonExistentVersions()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3);

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
    Assert.assertEquals(
        0,
        storageCoordinator.markNonOvershadowedSegmentsAsUsed(
            TestDataSource.KOALA,
            Intervals.ETERNITY,
            ImmutableList.of("foo", "bar")
        )
    );
    

    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedInvalidDataSource()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createNewSegment1(TestDataSource.KOALA);
    final DataSegment koalaSegment2 = createNewSegment1(TestDataSource.KOALA);

    publishUnusedSegments(koalaSegment1, koalaSegment2);
    final ImmutableSet<SegmentId> segmentIds =
        Set.of(koalaSegment1.getId(), koalaSegment2.getId());
    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> storageCoordinator.markNonOvershadowedSegmentsAsUsed("wrongDataSource", segmentIds)
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageContains("Could not find segment IDs")
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedWithInvalidSegmentIds()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createNewSegment1(TestDataSource.KOALA);
    final DataSegment koalaSegment2 = createNewSegment1(TestDataSource.KOALA);

    final ImmutableSet<SegmentId> segmentIds =
        Set.of(koalaSegment1.getId(), koalaSegment2.getId());
    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );

    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> storageCoordinator.markNonOvershadowedSegmentsAsUsed(TestDataSource.KOALA, segmentIds)
        ),
        DruidExceptionMatcher
            .invalidInput()
            .expectMessageContains("Could not find segment IDs")
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsed()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createNewSegment1(TestDataSource.KOALA);
    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-19T00:00:00.000/2017-10-20T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment4 = createNewSegment2(TestDataSource.KOALA);

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3, koalaSegment4);
    final Interval theInterval = Intervals.of("2017-10-15T00:00:00.000/2017-10-18T00:00:00.000");

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );

    // 2 out of 3 segments match the interval
    Assert.assertEquals(2, storageCoordinator.markNonOvershadowedSegmentsAsUsed(TestDataSource.KOALA, theInterval, null));

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment1, koalaSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testmarkNonOvershadowedSegmentsAsUsedWithOverlappingInterval()
  {
    publishSegments(wikiSegment1, wikiSegment2);
    
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-16T20:19:12.565Z"
    );

    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-19T00:00:00.000/2017-10-22T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );

    // Overshadowed by koalaSegment2
    final DataSegment koalaSegment4 = createNewSegment2(TestDataSource.KOALA);

    publishUnusedSegments(koalaSegment1, koalaSegment2, koalaSegment3, koalaSegment4);
    final Interval theInterval = Intervals.of("2017-10-16T00:00:00.000/2017-10-20T00:00:00.000");

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2),
        retrieveAllUsedSegments()
    );

    // 1 out of 3 segments match the interval, other 2 overlap, only the segment fully contained will be marked unused
    Assert.assertEquals(1, storageCoordinator.markNonOvershadowedSegmentsAsUsed(TestDataSource.KOALA, theInterval, null));

    
    Assert.assertEquals(
        Set.of(wikiSegment1, wikiSegment2, koalaSegment2),
        retrieveAllUsedSegments()
    );
  }

  @Test
  public void testGetUnusedSegmentIntervals()
  {
    publishSegment(wikiSegment1);
    publishSegment(wikiSegment2);

    // Allow null values of used_status_last_updated to test upgrade from older Druid versions
    allowUsedFlagLastUpdatedToBeNullable();

    int numChangedSegments = markSegmentsAsUnused(wikiSegment1.getId(), wikiSegment2.getId());
    Assert.assertEquals(2, numChangedSegments);

    // Publish an unused segment with used_status_last_updated 2 hours ago
    final DataSegment koalaSegment1 = createSegment(
        TestDataSource.KOALA,
        "2017-10-15T00:00:00.000/2017-10-16T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
    publishUnusedSegments(koalaSegment1);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(
        koalaSegment1.getId().toString(),
        DateTimes.nowUtc().minus(Duration.standardHours(2))
    );

    // Publish an unused segment with used_status_last_updated 2 days ago
    final DataSegment koalaSegment2 = createSegment(
        TestDataSource.KOALA,
        "2017-10-16T00:00:00.000/2017-10-17T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
    publishUnusedSegments(koalaSegment2);
    derbyConnectorRule.segments().updateUsedStatusLastUpdated(
        koalaSegment2.getId().toString(),
        DateTimes.nowUtc().minus(Duration.standardDays(2))
    );

    // Publish an unused segment and set used_status_last_updated to null
    final DataSegment koalaSegment3 = createSegment(
        TestDataSource.KOALA,
        "2017-10-17T00:00:00.000/2017-10-18T00:00:00.000",
        "2017-10-15T20:19:12.565Z"
    );
    publishUnusedSegments(koalaSegment3);
    updateUsedStatusLastUpdatedToNull(koalaSegment3);

    Assert.assertEquals(
        ImmutableList.of(wikiSegment2.getInterval()),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            null,
            DateTimes.of("3000"),
            1,
            DateTimes.COMPARE_DATE_AS_STRING_MAX
        )
    );

    // Test the DateTime maxEndTime argument of getUnusedSegmentIntervals
    Assert.assertEquals(
        ImmutableList.of(wikiSegment2.getInterval()),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            null,
            DateTimes.of(2012, 1, 7, 0, 0),
            1,
            DateTimes.COMPARE_DATE_AS_STRING_MAX
        )
    );
    Assert.assertEquals(
        ImmutableList.of(wikiSegment1.getInterval()),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            DateTimes.of(2012, 1, 7, 0, 0),
            DateTimes.of(2012, 4, 7, 0, 0),
            1,
            DateTimes.COMPARE_DATE_AS_STRING_MAX
        )
    );
    Assert.assertEquals(
        ImmutableList.of(),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            DateTimes.of(2012, 1, 7, 0, 0),
            DateTimes.of(2012, 1, 7, 0, 0),
            1,
            DateTimes.COMPARE_DATE_AS_STRING_MAX
        )
    );

    Assert.assertEquals(
        ImmutableList.of(wikiSegment2.getInterval(), wikiSegment1.getInterval()),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            null,
            DateTimes.of("3000"),
            5,
            DateTimes.COMPARE_DATE_AS_STRING_MAX
        )
    );

    // Test a buffer period that should exclude some segments

    // The wikipedia datasource has segments generated with last used time equal to roughly the time of test run. None of these segments should be selected with a bufer period of 1 day
    Assert.assertEquals(
        ImmutableList.of(),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.WIKI,
            DateTimes.COMPARE_DATE_AS_STRING_MIN,
            DateTimes.of("3000"),
            5,
            DateTimes.nowUtc().minus(Duration.parse("PT86400S"))
        )
    );

    // koalaSegment3 has a null used_status_last_updated which should mean getUnusedSegmentIntervals never returns it
    // koalaSegment2 has a used_status_last_updated older than 1 day which means it should be returned
    // The last of the 3 segments in koala has a used_status_last_updated date less than one day and should not be returned
    Assert.assertEquals(
        ImmutableList.of(koalaSegment2.getInterval()),
        storageCoordinator.getUnusedSegmentIntervals(
            TestDataSource.KOALA,
            DateTimes.COMPARE_DATE_AS_STRING_MIN,
            DateTimes.of("3000"),
            5,
            DateTimes.nowUtc().minus(Duration.parse("PT86400S"))
        )
    );
  }
}
