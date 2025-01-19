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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.SegmentReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a source... of data... for a query. Analogous to the "FROM" clause in SQL.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = TableDataSource.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TableDataSource.class, name = "table"),
    @JsonSubTypes.Type(value = QueryDataSource.class, name = "query"),
    @JsonSubTypes.Type(value = UnionDataSource.class, name = "union"),
    @JsonSubTypes.Type(value = JoinDataSource.class, name = "join"),
    @JsonSubTypes.Type(value = LookupDataSource.class, name = "lookup"),
    @JsonSubTypes.Type(value = InlineDataSource.class, name = "inline"),
    @JsonSubTypes.Type(value = GlobalTableDataSource.class, name = "globalTable"),
    @JsonSubTypes.Type(value = UnnestDataSource.class, name = "unnest"),
    @JsonSubTypes.Type(value = FilteredDataSource.class, name = "filter"),
    @JsonSubTypes.Type(value = RestrictedDataSource.class, name = "restrict")
})
public interface DataSource
{
  /**
   * Returns the names of all table datasources involved in this query. Does not include names for non-tables, like
   * lookups or inline datasources.
   */
  Set<String> getTableNames();

  /**
   * Returns datasources that this datasource depends on. Will be empty for leaf datasources like 'table'.
   */
  List<DataSource> getChildren();

  /**
   * Return a new DataSource, identical to this one, with different children. The number of children must be equal
   * to the number of children that this datasource already has.
   */
  DataSource withChildren(List<DataSource> children);

  /**
   * Returns true if queries on this dataSource are cacheable at both the result level and per-segment level.
   * Currently, dataSources that do not actually reference segments (like 'inline'), are not cacheable since cache keys
   * are always based on segment identifiers.
   */
  boolean isCacheable(boolean isBroker);

  /**
   * Returns true if all servers have a full copy of this datasource. True for things like inline, lookup, etc, or
   * for queries of those.
   * <p>
   * Currently this is coupled with joinability - if this returns true then the query engine expects there exists a
   * {@link org.apache.druid.segment.join.JoinableFactory} which might build a
   * {@link org.apache.druid.segment.join.Joinable} for this datasource directly. If a subquery 'inline' join is
   * required to join this datasource on the right hand side, then this value must be false for now.
   * <p>
   * In the future, instead of directly using this method, the query planner and engine should consider
   * {@link org.apache.druid.segment.join.JoinableFactory#isDirectlyJoinable(DataSource)} when determining if the
   * right hand side is directly joinable, which would allow decoupling this property from joins.
   */
  boolean isGlobal();

  /**
   * Returns true if this datasource can be the base datasource of query processing.
   * <p>
   * Base datasources drive query processing. If the base datasource is {@link TableDataSource}, for example, queries
   * are processed in parallel on data servers. If the base datasource is {@link InlineDataSource}, queries are
   * processed on the Broker. See {@link DataSourceAnalysis#getBaseDataSource()} for further discussion.
   * <p>
   * Datasources that are *not* concrete must be pre-processed in some way before they can be processed by the main
   * query stack. For example, {@link QueryDataSource} must be executed first and substituted with its results.
   *
   * @see DataSourceAnalysis#isConcreteBased() which uses this
   * @see DataSourceAnalysis#isConcreteAndTableBased() which uses this
   */
  boolean isConcrete();

  /**
   * Returns a segment function on to how to segment should be modified.
   *
   * @param query      the input query
   * @param cpuTimeAcc the cpu time accumulator
   * @return the segment function
   */
  Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query, AtomicLong cpuTimeAcc);

  /**
   * Returns an updated datasource based on the specified new source.
   *
   * @param newSource the new datasource to be used to update an existing query
   * @return the updated datasource to be used
   */
  DataSource withUpdatedDataSource(DataSource newSource);

  /**
   * Returns the query with an updated datasource based on the policy restrictions on tables.
   * <p>
   * If this datasource contains no table, no changes should occur.
   *
   * @param policyMap a mapping of table names to policy restrictions. A missing key is different from an empty value:
   *                  <ul>
   *                    <li> a missing key means the table has never been permission checked.
   *                    <li> an empty value indicates the table doesn't have any policy restrictions, it has been permission checked.
   * @return the updated datasource, with restrictions applied in the datasource tree
   * @throws IllegalStateException when mapping a RestrictedDataSource, unless the table has a NoRestrictionPolicy in
   *                               the policyMap (used by druid-internal). Missing policy or adding a
   *                               non-NoRestrictionPolicy to RestrictedDataSource would throw.
   */
  default DataSource withPolicies(Map<String, Optional<Policy>> policyMap)
  {
    List<DataSource> children = this.getChildren()
                                    .stream()
                                    .map(child -> child.withPolicies(policyMap))
                                    .collect(Collectors.toList());
    return this.withChildren(children);
  }

  /**
   * Compute a cache key prefix for a data source. This includes the data sources that participate in the RHS of a
   * join as well as any query specific constructs associated with join data source such as base table filter. This key prefix
   * can be used in segment level cache or result level cache. The function can return following
   * - Non-empty byte array - If there is join datasource involved and caching is possible. The result includes
   * join condition expression, join type and cache key returned by joinable factory for each {@link PreJoinableClause}
   * - NULL - There is a join but caching is not possible. It may happen if one of the participating datasource
   * in the JOIN is not cacheable.
   *
   * @return the cache key to be used as part of query cache key
   */
  byte[] getCacheKey();

  /**
   * Get the analysis for a data source
   *
   * @return The {@link DataSourceAnalysis} object for the callee data source
   */
  DataSourceAnalysis getAnalysis();
}
