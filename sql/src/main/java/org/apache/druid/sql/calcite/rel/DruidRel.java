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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.Set;

public abstract class DruidRel<T extends DruidRel<?>> extends AbstractRelNode
{
  private final PlannerContext plannerContext;

  protected DruidRel(RelOptCluster cluster, RelTraitSet traitSet, PlannerContext plannerContext)
  {
    super(cluster, traitSet);
    this.plannerContext = plannerContext;
  }

  /**
   * Returns the PartialDruidQuery associated with this DruidRel, and which can be built on top of. Returns null
   * if this rel cannot be built on top of.
   */
  @Nullable
  public abstract PartialDruidQuery getPartialDruidQuery();

  public QueryResponse<Object[]> runQuery()
  {
    // runQuery doesn't need to finalize aggregations, because the fact that runQuery is happening suggests this
    // is the outermost query, and it will actually get run as a native query. Druid's native query layer will
    // finalize aggregations for the outermost query even if we don't explicitly ask it to.

    return getPlannerContext().getQueryMaker().runQuery(toDruidQuery(false));
  }

  public abstract T withPartialQuery(PartialDruidQuery newQueryBuilder);

  public boolean isValidDruidQuery()
  {
    try {
      toDruidQueryForExplaining();
      return true;
    }
    catch (CannotBuildQueryException e) {
      return false;
    }
  }

  /**
   * Convert this DruidRel to a DruidQuery. This must be an inexpensive operation, since it is done often throughout
   * query planning.
   *
   * This method must not return null.
   *
   * @param finalizeAggregations true if this query should include explicit finalization for all of its
   *                             aggregators, where required. Useful for subqueries where Druid's native query layer
   *                             does not do this automatically.
   *
   * @throws CannotBuildQueryException
   */
  public abstract DruidQuery toDruidQuery(boolean finalizeAggregations);

  /**
   * Convert this DruidRel to a DruidQuery for purposes of explaining. This must be an inexpensive operation.
   *
   * This method must not return null.
   *
   * @throws CannotBuildQueryException
   */
  public abstract DruidQuery toDruidQueryForExplaining();

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  /**
   * Overridden to ensure that subclasses provide a proper implementation. The default implementation from
   * {@link AbstractRelNode} does nothing and is not appropriate.
   */
  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object clone() throws CloneNotSupportedException
  {
    // RelNode implements Cloneable, but our class of rels is not cloned, so does not need to implement clone().
    throw new CloneNotSupportedException();
  }

  /**
   * Returns a copy of this rel with the {@link DruidConvention} trait.
   */
  public abstract T asDruidConvention();

  /**
   * Get the set of names of table datasources read by this DruidRel
   */
  public abstract Set<String> getDataSourceNames();

  public final RelNode unwrapLogicalPlan()
  {
    return accept(new LogicalPlanUnwrapperShuttle());
  }

  private static class LogicalPlanUnwrapperShuttle extends RelShuttleImpl
  {
    @Override
    public RelNode visit(RelNode other)
    {
      return super.visit(visitNode(other));
    }

    private RelNode visitNode(RelNode other)
    {
      if (other instanceof RelSubset) {
        final RelSubset subset = (RelSubset) other;
        return visitNode(Iterables.getFirst(subset.getRels(), null));
      }
      if (other instanceof DruidRel<?>) {
        DruidRel<?> druidRel = (DruidRel<?>) other;
        if (druidRel.getPartialDruidQuery() != null && druidRel.getPartialDruidQuery().leafRel() != null) {
          return druidRel.getPartialDruidQuery().leafRel();
        }
      }
      return other;
    }
  }
}
