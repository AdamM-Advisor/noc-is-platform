# Migration Backlog

## Phase 0: Safety Cleanup

- Stop tracking generated databases and backups.
- Move credentials out of `.replit` and into deployment secrets.
- Align `pyproject.toml`, `backend/requirements.txt`, and frontend package manifests.
- Add a small anonymized Parquet fixture for repeatable validation.
- Add golden KPI outputs from the current app.

## Phase 1: Data Lake Foundation

- Add canonical ticket schema definition.
- Add Parquet lake service for partition discovery and DuckDB scans.
- Add upload metadata catalog.
- Add durable job table for import, refresh, prediction, report, and backup jobs.
- Add checksum and duplicate-file detection.

## Phase 2: Scalable Ingestion

- Validate Parquet schema using metadata.
- Normalize ticket headers without loading full data into memory.
- Write bronze Parquet partitions.
- Generate silver derived columns with DuckDB SQL.
- Refresh only affected summary partitions.

## Phase 3: Summary and KPI Engine

- Rebuild monthly and weekly summaries from silver Parquet.
- Add entity-level summary tables for Area, Regional, NOP, TO, and Site.
- Add summary run versioning.
- Add regression tests against legacy KPI fixtures.

## Phase 4: Predictive Failure Engine

- Build feature windows for each site and entity level.
- Implement rolling statistics, anomaly detection, count forecasting, and hazard-style time-to-next-failure estimates.
- Store model version, parameters, prediction output, and actual outcome.
- Add backtesting reports for precision, recall, false positives, and missed failures.

## Phase 5: NDC and Recommendation Engine

- Recompute NDC entries from silver ticket facts.
- Separate auto-generated NDC enrichment from manually curated content.
- Version recommendations and thresholds.
- Add audit trail for curation changes.

## Phase 6: Frontend Reimplementation

- Convert to TypeScript.
- Use feature-based folders.
- Centralize API client and query caching.
- Split upload, profiler, NDC, external data, and master data pages into small components.
- Add progressive states for big-data jobs: queued, validating, partitioning, deriving, summarizing, completed, failed.

## Phase 7: Deployment Hardening

- Add reproducible build command.
- Add health/readiness endpoints.
- Add structured logging.
- Add backup/restore strategy for operational DB and Parquet lake metadata.
- Add role-based admin actions for destructive operations.
