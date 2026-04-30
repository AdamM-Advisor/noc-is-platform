# Local Benchmark

The local benchmark generates anonymized synthetic Parquet tickets, runs the
raw to bronze to silver to gold pipeline, and optionally runs predictive risk
scoring and backtesting.

Outputs are written under `.test_tmp/benchmarks` by default, so benchmark data
does not enter Git.

## Smoke Run

```powershell
python -m backend.jobs benchmark-local --rows 5000 --months 2 --site-count 200
```

## Realistic Local Runs

Start with 100k rows:

```powershell
python -m backend.jobs benchmark-local --rows 100000 --months 3 --site-count 1000
```

Then try 1M rows:

```powershell
python -m backend.jobs benchmark-local --rows 1000000 --months 6 --site-count 5000
```

For very large local runs, skip predictive first and benchmark ingestion only:

```powershell
python -m backend.jobs benchmark-local --rows 1000000 --months 6 --site-count 5000 --skip-predictive --skip-backtest
```

## Result File

Each run writes `benchmark_result.json` with:

- generated raw Parquet size
- bronze, silver, and monthly summary output size
- per-stage timings
- rows per second
- predictive prediction count and top risks
- backtest confusion matrix and metrics

Use this before Cloud Run sizing. The first target is to estimate whether the
monthly partition size and Cloud Run Jobs memory/CPU settings are enough before
testing against `gs://` object storage.
