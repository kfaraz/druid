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

package org.apache.druid.indexing.overlord.supervisor;

/**
 * This class is fairly close to being a template or a list of templates.
 * I think that definition might help us keep things simple.
 *
 * TODO: stuff needed here:
 *   - job definition
 *   - schedule??, yeah, for batch schedule is indeed part of the spec itself
 *   - validation logic - yeah most likely
 */
public interface BatchIndexingSupervisorSpec extends SupervisorSpec
{
  String getTargetDatasource();
}
