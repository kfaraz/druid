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

package org.apache.druid.testing.embedded.msq;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.embedded.indexer.AbstractGcsInputSourceParallelIndexTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment -
 * GOOGLE_PREFIX - path inside the bucket where the test data files will be uploaded
 * GOOGLE_BUCKET - Google cloud bucket name
 * GOOGLE_APPLICATION_CREDENTIALS - path to the json file containing google cloud credentials
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/google.html">Google Cloud Storage setup in druid</a>
 */

public class ITGcsSQLBasedIngestionTest extends AbstractGcsInputSourceParallelIndexTest
{
  private static final String CLOUD_INGEST_SQL = "/multi-stage-query/wikipedia_cloud_index_msq.sql";
  private static final String INDEX_QUERIES_FILE = "/multi-stage-query/wikipedia_index_queries.json";

  @Disabled("Can be enabled after adding GCS testcontainer")
  @ParameterizedTest(name = "Test_{index} ({0})")
  @MethodSource("resources")
  public void testSQLBasedBatchIngestion(Pair<String, List<?>> GcsInputSource)
  {
    doMSQTest(GcsInputSource, CLOUD_INGEST_SQL, INDEX_QUERIES_FILE, "google");
  }
}
