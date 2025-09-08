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

package org.apache.druid.testing.embedded.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.utils.AbstractQueryWithResults;
import org.apache.druid.testing.utils.QueryResultVerifier;
import org.apache.druid.testing.utils.QueryWithResults;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class QueryHelper
{
  private static final Logger LOG = new Logger(QueryHelper.class);

  private final EmbeddedDruidCluster cluster;
  private final ObjectMapper mapper;

  public QueryHelper(EmbeddedDruidCluster cluster, ObjectMapper mapper)
  {
    this.cluster = cluster;
    this.mapper = mapper;
  }

  public void testNativeQueriesFromResource(String resourceName, String dataSource)
  {
    try {
      String content = AbstractIndexerTest.getResourceAsString(resourceName);
      content = StringUtils.replace(content, "%%DATASOURCE%%", dataSource);

      final List<QueryWithResults> queries = mapper.readValue(content, new TypeReference<>() {});
      for (QueryWithResults queryWithResult : queries) {
        final String resultAsJson = cluster.callApi().onAnyBroker(
            b -> b.submitNativeQuery(queryWithResult.getQuery())
        );
        List<Map<String, Object>> result = JacksonUtils.readValue(
            TestHelper.JSON_MAPPER,
            resultAsJson.getBytes(StandardCharsets.UTF_8),
            new TypeReference<>() {}
        );
        compareResults(queryWithResult, result);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testSqlQueriesFromResource()
  {

  }

  public void testQueriesFromString(String query)
  {

  }

  public void testQueriesFromString(String url, String query)
  {
    testQueriesFromString(query);
  }

  public void testQueriesFromFile(String file)
  {

  }

  public void testQueriesFromFile(String url, String file)
  {
    testQueriesFromFile(file);
  }

  private <Q extends AbstractQueryWithResults<?>> void compareResults(
      Q queryWithResult,
      List<Map<String, Object>> result
  ) throws Exception
  {
    QueryResultVerifier.ResultVerificationObject resultsComparison = QueryResultVerifier.compareResults(
        result,
        queryWithResult.getExpectedResults(),
        queryWithResult.getFieldsToTest()
    );
    if (!resultsComparison.isSuccess()) {
      LOG.error(
          "Failed while executing query %s \n expectedResults: %s \n actualResults : %s",
          queryWithResult.getQuery(),
          mapper.writeValueAsString(queryWithResult.getExpectedResults()),
          mapper.writeValueAsString(result)
      );
      throw new ISE(
          "Results mismatch while executing the query %s.\n"
          + "Mismatch error: %s\n",
          queryWithResult.getQuery(),
          resultsComparison.getErrorMessage()
      );
    }
  }
}
