# Implementation Checklist

## 0. Evaluation and Direction

- [x] Evaluate the existing Replit-era application.
- [x] Identify that line-by-line conversion is not suitable for tens of millions of yearly Parquet tickets.
- [x] Choose Cloudflare Pages for frontend and Google Cloud for Python compute/data.
- [x] Document big-data reimplementation direction.

## 1. Big Data Architecture

- [x] Define raw/bronze/silver/gold Parquet lake approach.
- [x] Define partition strategy: `year`, `month`, `source`.
- [x] Add DuckDB Parquet query helper.
- [x] Support local and remote lake roots such as `gs://...`.
- [x] Implement actual bronze writer.
- [x] Smoke test bronze writer with local Parquet and temporary catalog.
- [ ] Verify bronze writer against Google Cloud Storage from Cloud Run Jobs.
- [x] Implement actual silver derived-column writer.
- [x] Smoke test silver writer with local Parquet and temporary catalog.
- [ ] Verify silver writer against Google Cloud Storage from Cloud Run Jobs.
- [x] Implement incremental monthly summary refresh by affected partition.
- [x] Smoke test monthly summary writer with local Parquet and temporary catalog.
- [ ] Verify monthly summary refresh against Google Cloud Storage from Cloud Run Jobs.
- [x] Add benchmark with realistic Parquet volume.

## 2. Predictive Failure Foundation

- [x] Add statistical baseline service.
- [x] Implement weighted moving average.
- [x] Implement exponential smoothing forecast.
- [x] Implement robust MAD anomaly detection.
- [x] Implement event gap analysis.
- [x] Implement explainable failure risk scoring.
- [x] Add unit tests for baseline statistics.
- [x] Add feature extraction from silver Parquet.
- [x] Add CLI command for predictive risk scoring from silver Parquet.
- [x] Persist actual prediction outputs to model run catalog.
- [x] Smoke test predictive scoring with local silver Parquet and temporary catalog.
- [ ] Verify predictive scoring against Google Cloud Storage from Cloud Run Jobs.
- [x] Add backtesting workflow.
- [x] Persist backtest summary to model run catalog.
- [x] Smoke test backtesting workflow with local silver Parquet and temporary catalog.
- [ ] Verify backtesting workflow against Google Cloud Storage from Cloud Run Jobs.
- [x] Add model monitoring metrics.
- [x] Add SARIMAX batch forecast job from summary cache.
- [x] Persist SARIMAX forecast outputs to model run catalog.
- [ ] Validate SARIMAX runtime after `statsmodels` is installed locally/cloud.

## 3. Deployment Foundation

- [x] Add frontend API origin config for Cloudflare Pages.
- [x] Add Cloudflare `_redirects` and `_headers`.
- [x] Add Dockerfile for Cloud Run.
- [x] Add `.gcloudignore`.
- [x] Add Cloud Build config.
- [x] Add Cloud Run Jobs entrypoint.
- [x] Document Cloudflare + Google deployment path.
- [ ] Run actual Docker/Cloud Build image build.
- [ ] Deploy first Cloud Run API.
- [ ] Deploy first Cloudflare Pages frontend.
- [ ] Configure Secret Manager.

## 4. Operational Catalog and Jobs

- [x] Add operational job catalog.
- [x] Add file catalog.
- [x] Add lake partition catalog.
- [x] Add model run catalog schema.
- [x] Add `/api/ops` router for jobs, files, and partitions.
- [x] Add `/api/ops/model-runs` catalog read endpoints.
- [x] Add `/api/ops/summary` operational monitoring endpoint.
- [x] Add `ops-snapshot` CLI command.
- [x] Initialize operational catalog on backend boot.
- [ ] Move catalog from DuckDB to Cloud SQL PostgreSQL.
- [ ] Add durable job worker polling/claiming.
- [ ] Add job retry policy.
- [ ] Add job cancellation semantics.

## 5. Ingestion

- [x] Register external file URI as an ingestion job.
- [x] Compute local file checksum.
- [x] Capture file size and metadata.
- [x] Plan target bronze/silver partition URI.
- [x] Add CLI command for ingestion registration.
- [x] Add unit tests for ingestion path/checksum/partition helpers.
- [x] Define canonical ticket schema.
- [x] Validate Parquet schema without full dataset load.
- [x] Normalize headers to canonical ticket schema for bronze planning.
- [x] Generate DuckDB SQL plan for bronze partition writing.
- [x] Execute bronze partition writer against actual local Parquet.
- [x] Add CLI command to execute bronze partition writing.
- [x] Write silver partitions with derived columns.
- [x] Add CLI command to execute silver partition writing.
- [x] Register generated bronze partitions in catalog.
- [x] Register generated silver partitions in catalog.
- [x] Refresh monthly summary partitions.
- [x] Add RAW CSV/Excel/Parquet to Parquet pipeline.
- [x] Archive original RAW files before conversion.
- [x] Add CLI commands for `import-raw-file` and `import-raw-folder`.
- [x] Refresh DuckDB summary cache from Silver Parquet for existing UI reads.
- [ ] Add UI control to trigger folder-level RAW import jobs.

## 6. Backend API Migration

- [x] Keep legacy app running while new foundation is added.
- [x] Add configurable CORS for Cloudflare frontend.
- [x] Add configurable secure cookie options.
- [x] Replace in-memory auth/session store.
- [x] Replace in-memory upload processing jobs.
- [x] Replace in-memory resync jobs.
- [ ] Split monolithic routers into domain modules.
- [x] Route upload pipeline through gold summaries and DuckDB summary cache.
- [ ] Route dashboard/profiler reads directly through gold Parquet summaries.
- [ ] Route drill-down reads through targeted Parquet scans.

## 7. Frontend Migration

- [x] Keep current React/Vite frontend buildable.
- [x] Allow frontend to target remote Cloud Run API.
- [ ] Convert to TypeScript.
- [ ] Centralize direct Axios imports.
- [x] Add ops/job status UI.
- [x] Add basic big-data job progress UI.
- [ ] Refactor large pages into feature modules.

## 8. Data Safety and Repository Hygiene

- [x] Update `.gitignore` for generated data/lake/output folders.
- [x] Remove tracked DuckDB database and backups from Git index.
- [x] Remove secrets from `.replit`.
- [x] Add `.env.example` for backend.
- [x] Add golden anonymized fixtures.
- [x] Add CI tests.

## 9. External Setup Timing

- [x] Document when GitHub, Cloudflare, and Google Cloud are needed.
- [x] Prepare GitHub repository.
- [ ] Prepare Google Cloud project.
- [ ] Prepare Cloudflare Pages project.

## Current Setup Requirement

GitHub repository is prepared and `main` has been pushed. Continue with Google Cloud preparation for GCS, Secret Manager, Cloud Build, and Cloud Run validation.

The next setup checkpoint will come when we are ready to verify `execute-bronze` against a real `gs://` bucket and run the first Cloud Build image.
