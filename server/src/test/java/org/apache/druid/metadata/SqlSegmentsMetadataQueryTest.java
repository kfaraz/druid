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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SqlSegmentsMetadataQueryTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  @Before
  public void setUp()
  {
    derbyConnectorRule.getConnector().createSegmentTable();
  }

  @Test
  public void test_markSegmentsAsUsed()
  {
    update(
        sql -> sql.markSegmentsAsUsed(Set.of(), DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsAsUnused()
  {
    update(
        sql -> sql.markSegmentsAsUnused(Set.of(), DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsAsUsed_isIdempotent()
  {
    update(
        sql -> sql.markSegmentsAsUsed(Set.of(), DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsAsUsed_forEmptySegmentIds_isNoop()
  {
    update(
        sql -> sql.markSegmentsAsUsed(Set.of(), DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsUnused_forEternityInterval()
  {
    update(
        sql -> sql.markSegmentsUnused("", null, null, DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsUnused_forSingleVersion()
  {
    update(
        sql -> sql.markSegmentsUnused("", null, null, DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsUnused_forMultipleVersions()
  {
    update(
        sql -> sql.markSegmentsUnused("", null, List.of("", ""), DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsUnused_forAllVersions()
  {
    update(
        sql -> sql.markSegmentsUnused("", null, null, DateTimes.nowUtc())
    );
  }

  @Test
  public void test_markSegmentsUnused_forEmptyVersions_isNoop()
  {
    update(
        sql -> sql.markSegmentsUnused("", null, null, DateTimes.nowUtc())
    );
  }

  @Test
  public void test_retrieveSegmentForId()
  {
    Assert.assertNull(
        read(
            sql -> sql.retrieveSegmentForId(SegmentId.dummy(TestDataSource.WIKI))
        )
    );
  }

  private <T> T read(Function<SqlSegmentsMetadataQuery, T> function)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    return connector.inReadOnlyTransaction(
        (handle, status) -> function.apply(
            SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER)
        )
    );
  }

  private <T> Set<T> readAsSet(Function<SqlSegmentsMetadataQuery, CloseableIterator<T>> iterableReader)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    return connector.inReadOnlyTransaction((handle, status) -> {
      final SqlSegmentsMetadataQuery query =
          SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER);

      try (CloseableIterator<T> iterator = iterableReader.apply(query)) {
        return ImmutableSet.copyOf(iterator);
      }
    });
  }

  private <T> T update(Function<SqlSegmentsMetadataQuery, T> function)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    return connector.retryWithHandle(
        handle -> function.apply(
            SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER)
        )
    );
  }

  private Set<DataSegment> retrieveAllUsedSegments(String dataSource)
  {
    return readAsSet(query -> query.retrieveUsedSegments(dataSource, List.of()));
  }

  private void insertSegments(DataSegment... segments)
  {
    IndexerSqlMetadataStorageCoordinatorTestBase.insertUsedSegments(
        Set.of(segments),
        Map.of(),
        derbyConnectorRule,
        TestHelper.JSON_MAPPER
    );
  }
}