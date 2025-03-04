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

package org.apache.druid.query.aggregation.bloom.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.BloomFilterExtensionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.bloom.BloomFilterAggregatorFactory;
import org.apache.druid.query.aggregation.bloom.sql.BloomFilterSqlAggregatorTest.BloomFilterSqlAggComponentSupplier;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.TestDataBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Test;

import java.util.List;

@SqlTestFrameworkConfig.ComponentSupplier(BloomFilterSqlAggComponentSupplier.class)
public class BloomFilterSqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final int TEST_NUM_ENTRIES = 1000;

  private static final String DATA_SOURCE = "numfoo";

  public static class BloomFilterSqlAggComponentSupplier extends StandardComponentSupplier
  {
    public BloomFilterSqlAggComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(super.getCoreModule(), new BloomFilterExtensionModule());
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      final QueryableIndex index =
          IndexBuilder.create()
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          new IncrementalIndexSchema.Builder()
                              .withMetrics(
                                  new CountAggregatorFactory("cnt"),
                                  new DoubleSumAggregatorFactory("m1", "m1")
                              )
                              .withDimensionsSpec(TestDataBuilder.INDEX_SCHEMA_NUMERIC_DIMS.getDimensionsSpec())
                              .withRollup(false)
                              .build()
                      )
                      .rows(TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS)
                      .buildMMappedIndex();

      return walker.add(
          DataSegment.builder()
                     .dataSource(DATA_SOURCE)
                     .interval(index.getDataInterval())
                     .version("1")
                     .shardSpec(new LinearShardSpec(0))
                     .size(0)
                     .build(),
          index
      );
    }
  }

  @Test
  public void testBloomFilterAgg() throws Exception
  {
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = (String) row.getRaw("dim1");
      if (raw == null) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addString(raw);
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(dim1, 1000),\n"
        + "BLOOM_FILTER(dim1, CAST(1000 AS INTEGER))\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("dim1", "a0:dim1"),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                queryFramework().queryJsonMapper().writeValueAsString(expectedbl1),
                queryFramework().queryJsonMapper().writeValueAsString(expectedbl1)
            }
        )
    );
  }

  @Test
  public void testBloomFilterTwoAggs() throws Exception
  {
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    BloomKFilter expected2 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = (String) row.getRaw("dim1");
      if (raw == null) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addString(raw);
      }
      List<String> lst = row.getDimension("dim2");
      if (lst.size() == 0) {
        expected2.addBytes(null, 0, 0);
      }
      for (String s : lst) {
        if (s == null) {
          expected2.addBytes(null, 0, 0);
        } else {
          expected2.addString(s);
        }
      }
    }

    ObjectMapper jsonMapper = queryFramework().queryJsonMapper();
    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(dim1, 1000),\n"
        + "BLOOM_FILTER(dim2, 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE3)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new BloomFilterAggregatorFactory(
                          "a0",
                          new DefaultDimensionSpec("dim1", "a0:dim1"),
                          TEST_NUM_ENTRIES
                      ),
                      new BloomFilterAggregatorFactory(
                          "a1",
                          new DefaultDimensionSpec("dim2", "a1:dim2"),
                          TEST_NUM_ENTRIES
                      )
                  )
              )
              .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
              .build()
        ),
        ImmutableList.of(
            new Object[] {
                jsonMapper.writeValueAsString(expectedbl1),
                jsonMapper.writeValueAsString(expected2)
            }
        )
    );
  }

  @Test
  public void testBloomFilterAggExtractionFn() throws Exception
  {
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      String raw = (String) row.getRaw("dim1");
      // empty string extractionFn produces null
      if (raw == null || "".equals(raw)) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addString(raw.substring(0, 1));
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(SUBSTRING(dim1, 1, 1), 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new ExtractionDimensionSpec(
                                  "dim1",
                                  "a0:dim1",
                                  new SubstringDimExtractionFn(0, 1)
                              ),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{queryFramework().queryJsonMapper().writeValueAsString(expectedbl1)}
        )
    );
  }

  @Test
  public void testBloomFilterAggLong() throws Exception
  {
    cannotVectorize();

    BloomKFilter expected3 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("l1");
      if (raw == null) {
        expected3.addBytes(null, 0, 0);
      } else {
        expected3.addLong(((Number) raw).longValue());
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(l1, 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("l1", "a0:l1", ColumnType.LONG),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{queryFramework().queryJsonMapper().writeValueAsString(expected3)}
        )
    );
  }

  @Test
  public void testBloomFilterAggLongVirtualColumn() throws Exception
  {
    cannotVectorize();
    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("l1");
      if (raw == null) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addLong(2 * ((Number) raw).longValue());
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(l1 * 2, 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"l1\" * 2)",
                          ColumnType.LONG,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("v0", "a0:v0"),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{queryFramework().queryJsonMapper().writeValueAsString(expectedbl1)}
        )
    );
  }

  @Test
  public void testBloomFilterAggFloatVirtualColumn() throws Exception
  {
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("f1");
      if (raw == null) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addFloat(2 * ((Number) raw).floatValue());
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(f1 * 2, 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"f1\" * 2)",
                          ColumnType.FLOAT,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("v0", "a0:v0"),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{queryFramework().queryJsonMapper().writeValueAsString(expectedbl1)}
        )
    );
  }

  @Test
  public void testBloomFilterAggDoubleVirtualColumn() throws Exception
  {
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    for (InputRow row : TestDataBuilder.ROWS1_WITH_NUMERIC_DIMS) {
      Object raw = row.getRaw("dbl1");
      if (raw == null) {
        expectedbl1.addBytes(null, 0, 0);
      } else {
        expectedbl1.addDouble(2 * ((Number) raw).doubleValue());
      }
    }

    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(dbl1 * 2, 1000)\n"
        + "FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "(\"dbl1\" * 2)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("v0", "a0:v0"),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{queryFramework().queryJsonMapper().writeValueAsString(expectedbl1)}
        )
    );
  }

  @Test
  public void testEmptyTimeseriesResults() throws Exception
  {
    // makes empty bloom filters
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    BloomKFilter expected2 = new BloomKFilter(TEST_NUM_ENTRIES);

    ObjectMapper jsonMapper = queryFramework().queryJsonMapper();
    testQuery(
        "SELECT\n"
        + "BLOOM_FILTER(dim1, 1000),\n"
        + "BLOOM_FILTER(l1, 1000)\n"
        + "FROM numfoo where dim2 = 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .filters(equality("dim2", 0L, ColumnType.LONG))
                  .aggregators(
                      ImmutableList.of(
                          new BloomFilterAggregatorFactory(
                              "a0",
                              new DefaultDimensionSpec("dim1", "a0:dim1"),
                              TEST_NUM_ENTRIES
                          ),
                          new BloomFilterAggregatorFactory(
                              "a1",
                              new DefaultDimensionSpec("l1", "a1:l1", ColumnType.LONG),
                              TEST_NUM_ENTRIES
                          )
                      )
                  )
                  .context(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[] {
                jsonMapper.writeValueAsString(expectedbl1),
                jsonMapper.writeValueAsString(expected2)
            }
        )
    );
  }

  @Test
  public void testGroupByAggregatorDefaultValues() throws Exception
  {
    // makes empty bloom filters
    cannotVectorize();

    BloomKFilter expectedbl1 = new BloomKFilter(TEST_NUM_ENTRIES);
    BloomKFilter expected2 = new BloomKFilter(TEST_NUM_ENTRIES);

    ObjectMapper jsonMapper = queryFramework().queryJsonMapper();
    testQuery(
        "SELECT\n"
        + "dim2,\n"
        + "BLOOM_FILTER(dim1, 1000) FILTER(WHERE dim1 = 'nonexistent'),\n"
        + "BLOOM_FILTER(l1, 1000) FILTER(WHERE dim1 = 'nonexistent')\n"
        + "FROM numfoo WHERE dim2 = 'a' GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim2", "a", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ColumnType.STRING))
                        .setDimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            aggregators(
                                new FilteredAggregatorFactory(
                                    new BloomFilterAggregatorFactory(
                                        "a0",
                                        new DefaultDimensionSpec("dim1", "a0:dim1"),
                                        TEST_NUM_ENTRIES
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                ),
                                new FilteredAggregatorFactory(
                                    new BloomFilterAggregatorFactory(
                                        "a1",
                                        new DefaultDimensionSpec("l1", "a1:l1", ColumnType.LONG),
                                        TEST_NUM_ENTRIES
                                    ),
                                    equality("dim1", "nonexistent", ColumnType.STRING)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[] {
                "a",
                jsonMapper.writeValueAsString(expectedbl1),
                jsonMapper.writeValueAsString(expected2)
            }
        )
    );
  }
}
