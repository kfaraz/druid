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

package org.apache.druid.testing;

/**
 * Standard Druid commands.
 */
public enum DruidCommand
{
  /**
   * Command to run a Druid Coordinator which coordinates segment assignments,
   * balancing and centralized schema.
   */
  COORDINATOR("coordinator"),

  /**
   * Command to run a Druid Overlord which manages ingestion tasks and publishes
   * segment metadata to Metadata Store.
   */
  OVERLORD("overlord"),

  /**
   * Command to run a Druid Indexer which is a lightweight ingestion worker
   * that launches ingestion tasks as separate threads in the same JVM.
   */
  INDEXER("indexer"),

  /**
   * Command to run a Druid MiddleManager which is an ingestion worker
   * that launches ingestion tasks as child processes.
   */
  MIDDLE_MANAGER("middleManager"),

  /**
   * Command to run Druid Historical service which hosts segment data and can
   * be queried by a Broker.
   */
  HISTORICAL("historical"),

  /**
   * Command to run a Druid Broker which can handle all SQL and JSON queries
   * over HTTP and JDBC.
   */
  BROKER("broker"),

  /**
   * Command to run a Druid Router which routes queries to different Brokers
   * and serves the Druid Web-Console UI.
   */
  ROUTER("router");

  private final String name;

  DruidCommand(String name)
  {
    this.name = name;
  }

  public String getName()
  {
    return name;
  }
}
