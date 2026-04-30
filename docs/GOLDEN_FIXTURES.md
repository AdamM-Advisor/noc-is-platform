# Golden Anonymized Fixtures

The golden fixture set is generated from code instead of committed as static
data files. This keeps the repository light and prevents accidental inclusion
of operational NOC data.

## What It Covers

- Multiple sources: `swfm_realtime`, `swfm_event`, and `fault_center`.
- Multiple periods: January and February 2026.
- SLA pass/fail examples.
- Critical, major, minor, and low severities.
- Duplicate ticket number for repeat-risk validation.
- Orphan-like hierarchy example through an unknown area value.

## Where It Lives

- Fixture generator: `backend/tests/golden_fixtures.py`
- End-to-end fixture test: `backend/tests/test_golden_fixture_pipeline.py`

## Expected Baseline

- Files: 4 generated Parquet files.
- Rows: 14 synthetic tickets.
- `swfm_realtime` January 2026:
  - total tickets: 4
  - SLA met: 2
  - critical: 1
  - major: 2
  - minor: 1
  - duplicate ticket numbers: 1

## How To Run

Run the backend test suite with project dependencies installed:

```powershell
python -m unittest backend.tests.test_golden_fixture_pipeline
```

The test writes temporary Parquet and DuckDB files under `.test_tmp`, which is
ignored by Git.
