# EmbeddedAutoCompactionTest Migration

This document describes the migration of `ITAutoCompactionTest` from the integration test framework to the embedded test framework.

## Overview

The `EmbeddedAutoCompactionTest` is a faithful migration of `ITAutoCompactionTest` that preserves all test logic and coverage while adapting to the embedded test environment.

## Key Changes

### Framework Migration
- **TestNG → JUnit5**: Changed from TestNG annotations to JUnit5 annotations
- **Base Class**: Changed from `AbstractIndexerTest` to `EmbeddedClusterTestBase`
- **Dependency Injection**: Replaced `@Inject` dependencies with embedded cluster bindings

### Test Infrastructure
- **Cluster Setup**: Uses `EmbeddedDruidCluster` instead of external Docker containers
- **Data Loading**: Uses `TaskPayload` with inline data instead of resource files
- **API Access**: Direct API calls via `cluster.callApi()` instead of HTTP clients
- **Segment Access**: Uses `overlord.bindings().segmentsMetadataStorage()` for metadata

### Compaction Configuration
- **Config Submission**: Uses `cluster.callApi().onLeaderOverlord(o -> o.setDataSourceCompactionConfig())`
- **Triggering**: Uses coordinator APIs via `cluster.callApi().onLeaderCoordinator()`
- **Verification**: SQL-based verification using `cluster.runSql()`

## Running the Test

To run the embedded auto-compaction test:

```bash
mvn test -pl embedded-tests -Dtest=EmbeddedAutoCompactionTest
```

## Test Methods Included

The migration includes the following key test methods:

1. `testAutoCompactionRowWithMetricAndRowWithoutMetricShouldPreserveExistingMetricsUsingAggregatorWithDifferentReturnType`
2. `testAutoCompactionRowWithMetricAndRowWithoutMetricShouldPreserveExistingMetrics`
3. `testAutoCompactionOnlyRowsWithoutMetricShouldAddNewMetrics`
4. `testAutoCompactionDutySubmitAndVerifyCompaction`
5. `testAutoCompactionDutyCanUpdateCompactionConfig`

## Benefits of Embedded Testing

- **Faster execution**: No Docker container startup overhead
- **Better isolation**: Self-contained test environment
- **Easier debugging**: Direct access to internal components
- **Consistent results**: Less dependent on external environment factors

## Limitations

- **Simplified data**: Uses inline test data instead of complex resource files
- **Basic verification**: Uses SQL queries instead of complex JSON query verification
- **Reduced complexity**: Some advanced integration scenarios may need adaptation

This migration demonstrates how integration tests can be effectively converted to embedded tests while preserving their essential functionality and test coverage.