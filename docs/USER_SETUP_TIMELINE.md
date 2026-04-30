# When to Prepare GitHub, Cloudflare, and Google Cloud

This project can keep moving locally for a while before external accounts are required. Use this timeline so setup happens exactly when it becomes useful.

## Not Needed Yet

You do not need to prepare GitHub, Cloudflare, or Google Cloud while we are still working on:

- backend module boundaries
- data lake service logic
- statistical prediction logic
- operational job/catalog metadata
- local build and unit tests
- migration documentation

## Prepare GitHub When

Prepare GitHub when we are ready to connect Cloudflare Pages and Cloud Build to a repository.

You will need:

- a GitHub account
- a repository for this project
- permission for Cloudflare Pages to read the repo
- permission for Google Cloud Build to read the repo, if we choose Git-triggered builds

Recommended timing: after the local deploy scaffold and first production-style Docker build are stable.

## Prepare Cloudflare When

Prepare Cloudflare when the frontend is ready to publish.

You will need:

- Cloudflare account
- Pages project connected to the GitHub repo
- custom domain, if you want one
- Pages environment variable:

```text
VITE_API_ORIGIN=https://<cloud-run-api-url>
```

Recommended timing: after the backend has a working Cloud Run URL.

## Prepare Google Cloud When

Prepare Google Cloud when we are ready to run the backend container outside the local machine.

You will need:

- Google Cloud project
- billing enabled
- region, recommended: `asia-southeast2` if Jakarta latency matters
- enabled APIs:
  - Cloud Run
  - Cloud Build
  - Artifact Registry
  - Secret Manager
  - Cloud Storage
  - Cloud SQL, later for production operational DB
- a Cloud Storage bucket for the NOC data lake
- an Artifact Registry Docker repository

Recommended timing: before the first real deploy test of `nocis-api`.

## The First Moment I Will Ask You For Setup

I will ask you to prepare external accounts after these local items are complete:

1. Cloud Run container scaffold exists.
2. Frontend can target a remote API via `VITE_API_ORIGIN`.
3. Worker/job entrypoint exists.
4. Operational job/file/partition catalog exists.
5. Local build and smoke tests pass.

At that point, the first setup request will be:

```text
Please prepare:
1. GitHub repo URL
2. Google Cloud project ID
3. Google Cloud region
4. Cloud Storage bucket name
5. Cloudflare Pages project/domain preference
```

## Current Status

Current status: local implementation is still in progress. You do not need to prepare the external platforms yet.

Local bronze, silver, monthly summary, statistical predictive scoring, predictive backtesting, and Operations monitoring now work with small smoke tests. The first point where Google Cloud setup becomes useful is the next remote verification step: running the same `execute-bronze`, `execute-silver`, `execute-monthly-summary`, `execute-predictive-risk`, and `execute-predictive-backtest` flow against a real `gs://` bucket from Cloud Run Jobs.
