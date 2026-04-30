# Deploying NOC-IS on Cloudflare + Google Cloud

## Target Shape

This deployment uses Cloudflare for the web edge and Google Cloud for Python compute and data processing.

```text
Cloudflare Pages
  React/Vite frontend

Google Cloud Run
  FastAPI backend API

Google Cloud Run Jobs
  Python workers for Parquet ingestion, summary refresh, and predictive scoring

Google Cloud Storage
  raw uploads, bronze/silver/gold Parquet lake, report exports

Cloud SQL PostgreSQL
  recommended operational store for users, jobs, master data, metadata, saved views
```

The current codebase still uses DuckDB as the operational database in many routes. The Cloud Run container scaffold is ready, but production persistence should be handled before real production traffic by moving operational state to Cloud SQL or mounting a managed storage path deliberately.

## Cloudflare Pages Frontend

Use these Pages settings:

- Root directory: `frontend`
- Build command: `npm ci && npm run build`
- Build output directory: `dist`
- Environment variable: `VITE_API_ORIGIN=https://<cloud-run-api-url>`

The frontend includes:

- `frontend/public/_redirects` for SPA fallback.
- `frontend/public/_headers` for basic browser security headers.
- `frontend/src/api/axiosConfig.js` to route API calls to Cloud Run while keeping local `/api` development behavior.

## Build Backend Image

Create an Artifact Registry repository once:

```bash
gcloud artifacts repositories create nocis \
  --repository-format=docker \
  --location=asia-southeast2
```

Build and push the image:

```bash
gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions _REGION=asia-southeast2,_REPOSITORY=nocis,_IMAGE=nocis-app
```

The image will be:

```text
asia-southeast2-docker.pkg.dev/<PROJECT_ID>/nocis/nocis-app:latest
```

## Cloud Run Backend API

Deploy the image from Artifact Registry:

```bash
gcloud run deploy nocis-api \
  --image asia-southeast2-docker.pkg.dev/<PROJECT_ID>/nocis/nocis-app:latest \
  --region asia-southeast2 \
  --allow-unauthenticated \
  --set-env-vars APP_ENV=production,COOKIE_SECURE=1,COOKIE_SAMESITE=none \
  --set-env-vars CORS_ALLOW_ORIGINS=https://<your-cloudflare-pages-domain> \
  --set-env-vars SESSION_SECRET=<strong-random-secret> \
  --set-env-vars ADMIN_PASSWORD_HASH=<sha256-password-hash> \
  --set-env-vars ADMIN_EMAIL=<admin-email>
```

Recommended production resource start:

- CPU: 2
- Memory: 4 GiB
- Min instances: 0 or 1
- Max instances: start with 3
- Timeout: 300 seconds

The `Dockerfile` runs:

```bash
uvicorn backend.main:app --host 0.0.0.0 --port ${PORT}
```

## Cloud Run Jobs

Use the same container image and override the command for batch jobs.

Smoke test command:

```bash
python -m backend.jobs predictive-smoke
```

Lake layout command:

```bash
python -m backend.jobs ensure-lake
```

Register an ingestion file and create the durable job/file catalog entries:

```bash
python -m backend.jobs register-ingestion \
  gs://<bucket>/nocis-lake/uploads/tickets_2026-04.parquet \
  --file-type ticket \
  --source swfm_realtime \
  --year 2026 \
  --month 4
```

Validate Parquet metadata without loading the full dataset:

```bash
python -m backend.jobs validate-parquet \
  gs://<bucket>/nocis-lake/uploads/tickets_2026-04.parquet
```

Generate a bronze writer SQL plan:

```bash
python -m backend.jobs plan-bronze-sql \
  gs://<bucket>/nocis-lake/uploads/tickets_2026-04.parquet \
  gs://<bucket>/nocis-lake/tickets/bronze/year=2026/month=04/source=swfm_realtime \
  --columns "Site ID,Severity,Occured Time" \
  --source swfm_realtime \
  --year 2026 \
  --month 4
```

Execute the bronze writer and register the generated partition:

```bash
python -m backend.jobs execute-bronze \
  gs://<bucket>/nocis-lake/uploads/tickets_2026-04.parquet \
  gs://<bucket>/nocis-lake/tickets/bronze/year=2026/month=04/source=swfm_realtime \
  --columns "Site ID,Severity,Occured Time" \
  --source swfm_realtime \
  --year 2026 \
  --month 4
```

The command writes a deterministic `part-<hash>.parquet` file inside the target partition and records the partition in the operational catalog. For `gs://` execution, the Cloud Run Job must have Google Cloud Storage permission and DuckDB remote file support available. During early deploy tests, set `NOCIS_DUCKDB_INSTALL_EXTENSIONS=1` if the container needs to install/load DuckDB `httpfs`.

Execute the silver writer from a bronze partition and register the generated silver partition:

```bash
python -m backend.jobs execute-silver \
  gs://<bucket>/nocis-lake/tickets/bronze/year=2026/month=04/source=swfm_realtime \
  gs://<bucket>/nocis-lake/tickets/silver/year=2026/month=04/source=swfm_realtime \
  --source swfm_realtime \
  --year 2026 \
  --month 4
```

The silver writer derives operational columns such as `calc_year_month`, `calc_restore_time_min`, SLA flag, source, and hierarchy placeholders used by KPI and predictive workloads.

Refresh the monthly summary partition from silver:

```bash
python -m backend.jobs execute-monthly-summary \
  gs://<bucket>/nocis-lake/tickets/silver/year=2026/month=04/source=swfm_realtime \
  --source swfm_realtime \
  --year 2026 \
  --month 4 \
  --target-partition-uri gs://<bucket>/nocis-lake/summaries/monthly/year=2026/month=04/source=swfm_realtime
```

The summary output is the first gold layer for dashboard reads and predictive feature extraction. It should be refreshed only for partitions touched by the ingestion job.

Score statistical failure risk from silver Parquet:

```bash
python -m backend.jobs execute-predictive-risk \
  gs://<bucket>/nocis-lake/tickets/silver \
  --entity-level site \
  --window-start 2026-01 \
  --window-end 2026-04 \
  --source swfm_realtime \
  --as-of-date 2026-04-30 \
  --limit 500
```

This command extracts per-entity monthly ticket counts, severity mix, repeat indicators, MTTR trend, escalation rate, anomaly count, forecast, and explainable risk score. By default it writes prediction outputs to `model_run_catalog`; use `--no-persist-model-runs` for read-only dry runs.

Backtest a historical risk window against a future outcome window:

```bash
python -m backend.jobs execute-predictive-backtest \
  gs://<bucket>/nocis-lake/tickets/silver \
  --entity-level site \
  --train-start 2026-01 \
  --train-end 2026-03 \
  --outcome-start 2026-04 \
  --outcome-end 2026-04 \
  --source swfm_realtime \
  --risk-threshold 55 \
  --min-actual-tickets 2
```

The backtest reports true positives, false positives, false negatives, precision, recall, specificity, F1, and accuracy. It persists one backtest summary to `model_run_catalog` unless `--no-persist-model-run` is provided.

Print the operational snapshot used by the Operations UI:

```bash
python -m backend.jobs ops-snapshot \
  --job-limit 100 \
  --partition-limit 250 \
  --model-run-limit 100
```

The API equivalent is:

```text
GET /api/ops/summary
```

Example Cloud Run Job:

```bash
gcloud run jobs create nocis-predictive-smoke \
  --image asia-southeast2-docker.pkg.dev/<PROJECT_ID>/nocis/nocis-app:latest \
  --region asia-southeast2 \
  --set-env-vars APP_ENV=production,NOCIS_LAKE_ROOT=gs://<bucket>/nocis-lake,NOCIS_DUCKDB_INSTALL_EXTENSIONS=1 \
  --command python \
  --args -m,backend.jobs,predictive-smoke
```

For real ingestion and predictive workloads, add dedicated commands that process one partition/window per task. Cloud Run Jobs can then run parallel tasks across affected month/source partitions.

## Storage Layout

Recommended bucket layout:

```text
gs://<bucket>/nocis-lake/
  uploads/
  tickets/
    bronze/
      year=2026/month=01/source=swfm_realtime/*.parquet
    silver/
      year=2026/month=01/source=swfm_realtime/*.parquet
  summaries/
    monthly/
    weekly/
    ndc/
    predictive/
  reports/
```

Set:

```text
NOCIS_LAKE_ROOT=gs://<bucket>/nocis-lake
```

For the legacy DuckDB app paths, use these only as a transition measure:

```text
NOCIS_DATA_DIR=/mnt/nocis-data
NOCIS_UPLOAD_DIR=/mnt/nocis-uploads
NOCIS_CHUNK_DIR=/tmp/nocis-chunks
NOCIS_EXPORT_DIR=/tmp/nocis-exports
```

Cloud Run local filesystem is ephemeral. Do not treat `/app/.data`, `/tmp`, or the container home directory as durable production storage.

## Repository Safety

Generated DuckDB data and backups are ignored and should stay outside Git:

```text
.data/
.data/backups/
.parquet_lake/
temp_chunks/
exports/
```

The Replit config keeps only placeholder admin values. Put real values in deployment secrets:

```text
SESSION_SECRET
ADMIN_EMAIL
ADMIN_PASSWORD_HASH
SENDGRID_API_KEY
```

The backend now stores sessions, pending 2FA, and login rate-limit attempts in database tables instead of process memory. This is a transition step toward Cloud SQL-backed auth/session persistence.

## Required Migration Before Production

Before this handles production NOC data:

1. Move auth/session/job metadata to Cloud SQL PostgreSQL.
2. Move ticket facts to partitioned Parquet in Cloud Storage.
3. Replace pandas full-file ticket ingestion with DuckDB/Arrow/Polars partition jobs.
4. Store job status durably, not in process memory.
5. Add golden KPI regression tests from known historical samples.
6. Configure secrets in Secret Manager instead of literal CLI env values.

## Local Verification

```bash
cd frontend
npm run build
```

```bash
python -m compileall -q backend main.py
python -m unittest backend.tests.test_statistical_failure_service
python -m backend.jobs predictive-smoke
```
