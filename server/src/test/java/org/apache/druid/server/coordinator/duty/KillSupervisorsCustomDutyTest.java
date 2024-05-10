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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.Stats;
import org.apache.druid.server.stats.DruidRunStats;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillSupervisorsCustomDutyTest
{
  @Mock
  private MetadataSupervisorManager mockMetadataSupervisorManager;

  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  private KillSupervisorsCustomDuty killSupervisors;

  @Test
  public void testConstructorSuccess()
  {
    killSupervisors = new KillSupervisorsCustomDuty(
        new Duration("PT1S"),
        mockMetadataSupervisorManager
    );
    Assert.assertNotNull(killSupervisors);
  }

  @Test
  public void testRun()
  {
    final DruidRunStats runStats = new DruidRunStats();
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(runStats);
    killSupervisors = new KillSupervisorsCustomDuty(
        new Duration("PT1S"),
        mockMetadataSupervisorManager
    );
    killSupervisors.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockMetadataSupervisorManager).removeTerminatedSupervisorsOlderThan(ArgumentMatchers.anyLong());
    Assert.assertTrue(runStats.hasStat(Stats.Kill.SUPERVISOR_SPECS));
  }
}
