# NOC-IS Analytics Big Data Reimplementation Blueprint

## Context

The next version must process tens of millions of NOC tickets per year, primarily delivered as Parquet. The current application is useful as a domain prototype, but its ingestion pattern reads whole files into pandas and inserts rows into DuckDB tables. That pattern will not scale safely for yearly ticket volumes, repeated reprocessing, or predictive failure workflows.

This blueprint treats the existing implementation as a source of business rules, UI behavior, and KPI formulas, not as code to port line by line.

## Target Principles

- Keep raw ticket data append-only and queryable directly from partitioned Parquet.
- Use DuckDB for local analytical SQL, Parquet predicate pushdown, and summary materialization.
- Avoid pandas for full-file ingestion. Use streaming/lazy engines such as DuckDB SQL, Polars lazy scans, or PyArrow dataset metadata.
- Separate transactional master data from analytical ticket facts.
- Store expensive aggregates as versioned materialized summaries.
- Make predictive models reproducible by snapshotting input windows, parameters, model version, and generated scores.
- Treat every import, refresh, prediction run, and report generation as a durable job with auditable status.

## Recommended Architecture

### Storage Layers

1. Raw Landing
   - Immutable uploaded files.
   - Stored with checksum, source type, period, upload metadata, and validation status.
   - No mutation after acceptance.

2. Bronze Parquet Lake
   - Normalized column names and canonical types.
   - Partitioned by `year`, `month`, and optionally `source`.
   - Keeps all ticket-level records needed for reprocessing.

3. Silver Ticket Fact
   - Derived columns such as response time, repair time, restore time, SLA duration, SLA-met flag, hierarchy IDs, week/month keys.
   - Stored as Parquet, not row-inserted into DuckDB for every ticket.

4. Gold Summaries
   - Monthly, weekly, entity-level, NDC, risk, and failure prediction aggregates.
   - Stored in DuckDB tables or Parquet summary datasets depending on query frequency.

5. Operational Store
   - Master data, users, sessions, jobs, saved views, report history, threshold config, model registry.
   - Recommended: PostgreSQL for production multi-user usage.
   - Acceptable internal first step: DuckDB for config plus Parquet lake for ticket facts.

## Ticket Lake Partitioning

Recommended directory shape:

```text
.parquet_lake/
  tickets/
    bronze/
      year=2026/
        month=01/
          source=swfm_realtime/
            part-000001.parquet
    silver/
      year=2026/
        month=01/
          source=swfm_realtime/
            part-000001.parquet
  summaries/
    monthly/
    weekly/
    ndc/
    predictive/
```

Minimum high-value partition columns:

- `year`
- `month`
- `source`

Do not over-partition by `area`, `regional`, or `site_id` unless profiling proves it helps. Too many tiny files will make planning and metadata scanning slower.

## Query Strategy

Use DuckDB `read_parquet` with:

- `hive_partitioning = true`
- `union_by_name = true`
- strict column projections
- date/period predicates pushed into SQL

The primary backend route should query gold summaries first. Ticket-level Parquet scans should be used for:

- drill-down
- backfill
- recalculation
- model feature generation
- ad hoc diagnostics

## Predictive Failure Design

Use a statistical baseline before complex ML. The platform should support per-site and hierarchical prediction with explainable features.

Recommended model ladder:

1. Baseline descriptive statistics
   - rolling mean, rolling median, variance, coefficient of variation
   - z-score and robust MAD anomaly detection
   - event gap statistics

2. Time series forecasting
   - weighted moving average
   - exponential smoothing
   - seasonal decomposition if there is enough history
   - Poisson or negative binomial count models for ticket volume

3. Failure risk scoring
   - recency, frequency, severity mix, repeat history, MTTR trend, escalation rate, device age, power/weather correlation
   - calibrated score buckets: low, medium, high, critical

4. Survival or hazard-style analysis
   - time-to-next-failure estimates
   - site class, region, device age, weather, PLN outage, historical patterns as covariates

5. Model monitoring
   - prediction timestamp, feature window, model version, expected date/risk, actual outcome, precision/recall by bucket

## Migration From Current App

### Preserve

- KPI definitions and business semantics.
- Master hierarchy: Area, Regional, NOP, TO, Site.
- NDC concepts and curation flow.
- Report and saved-view user workflows.
- Threshold-driven interpretation.

### Replace

- Whole-file pandas ingestion for ticket data.
- In-memory session and background job stores.
- Monolithic schema initialization/migration.
- Large router files containing SQL, transformations, and response shaping together.
- Unversioned predictive formulas.

## Implementation Slices

1. Foundation
   - Create ticket lake service.
   - Define canonical ticket schema and derived columns.
   - Add durable import job model.
   - Add metadata catalog for file uploads and partitions.

2. Scalable ingestion
   - Validate Parquet schema from metadata without reading full files.
   - Normalize and write bronze partitions.
   - Build silver partitions with DuckDB SQL.
   - Store row counts, checksum, period coverage, and quality checks.

3. Summary engine
   - Generate monthly and weekly summaries from silver Parquet.
   - Store summary run metadata.
   - Add incremental refresh by affected partitions.

4. Analytics API rewrite
   - Dashboard reads gold summaries.
   - Profiler reads summaries plus targeted Parquet scans.
   - NDC refresh reads silver ticket fact and writes curated gold tables.

5. Prediction engine
   - Implement feature store tables.
   - Implement statistical baseline models.
   - Add backtest endpoints and model monitoring.

6. Frontend rewrite
   - Move to TypeScript.
   - Centralize API client, errors, loading states, and caching.
   - Split large pages into feature modules.

7. Security and deployment
   - Replace auth/session model.
   - Add versioned migrations.
   - Remove tracked database and secret-bearing config.
   - Build reproducible deployment pipeline.

## Non-Negotiable Acceptance Criteria

- Importing a yearly Parquet dataset must not require loading the full dataset into pandas memory.
- Dashboard/profiler routes must have bounded query plans using partition and period predicates.
- Every long-running operation must survive app restart through durable job state.
- KPI outputs from the new engine must match golden fixtures from the old app.
- Predictive scores must be explainable and reproducible from stored feature snapshots.
- Raw uploads and processed Parquet partitions must be auditable by checksum and import id.
