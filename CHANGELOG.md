# Changelog

## 0.0.9 (2026-02-20)

### New Features
- Support for Copy Jobs via the Job Scheduler API
- Support for Materialized Lake View refresh (`RefreshMaterializedLakeViews` job type)

### Bug Fixes
- Fixed error payload format when parsing failure reasons from the API response
- Fixed semantic model refresh connection scope
- Fixed API URL for Spark jobs
- Fixed request ID field in log messages
- Unified API URL pattern for all artifact types when using the Job API

### Improvements
- Improved log messages for better observability

## 0.0.8 (2025-12-05)

- Lazy load hook for Run Item operators - Prevent DAG timeouts
- Fixes to Spark Job run
- Status Plugin
- Improvements to deeplink, prevent broken page
- Log Improvements

## 0.0.7 (2025-09-24)

- Support for Notebook and Pipeline parameters
- Improved logging on failures

## 0.1.0 (2025-08-29)

- Fixes for semantic model refresh

## 0.0.5 (2025-08-26)

- Support for semantic model refresh and user data function via new operators
- Support for operator links

## 0.0.4 (2025-07-16)

- Renamed python module names for consistency and better usability
- Operator names also changed to reflect the above
- Improved connection - no need to specify the scope
- Support for different Fabric environments
