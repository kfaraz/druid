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

package /*CHECKSTYLE.OFF: PackageName*/com.google.inject.servlet/*CHECKSTYLE.ON: PackageName*/;

import com.google.inject.Inject;

/**
 * This filter is used with jetty server instead of {@link GuiceFilter} to allow
 * multiple Druid services to run in the same process. GuiceFilter maintains a
 * static filter pipeline which cause conflicts when initializing multiple jetty
 * servers and gives the warning {@code "Multiple Servlet injectors detected"}.
 * It eventually results in only one of the jetty servers being properly initialized
 * with all its resource classes.
 * <p>
 * The fix is to bind {@link DruidGuiceFilter} and force the injector to use the
 * constructor to set the pipeline instead of the static method {@link GuiceFilter#setPipeline}.
 * This ensures that each instance of GuiceFilter corresponding to each injector
 * maintains its own {@link FilterPipeline}.
 * <p>
 * This class must be in the package {@code com.google.inject.servlet} to be able
 * to access:
 * <ul>
 * <li>package-private class {@link FilterPipeline}</li>
 * <li>package-private constructor {@link GuiceFilter#GuiceFilter(FilterPipeline)}</li>
 * </ul>
 */
public class DruidGuiceFilter extends GuiceFilter
{
  /**
   * Do not use. Must inject a {@link FilterPipeline} via the constructor.
   */
  @SuppressWarnings("unused")
  private DruidGuiceFilter()
  {
    throw new IllegalStateException();
  }

  @Inject
  public DruidGuiceFilter(FilterPipeline filterPipeline)
  {
    super(filterPipeline);
  }
}
