import argparse
import json
import sys

from backend.services.parquet_lake_service import ParquetTicketLake, TicketLakeFilter
from backend.services.predictive_lake_service import (
    PredictiveBacktestConfig,
    PredictiveRunConfig,
    predictive_feature_sql,
    run_predictive_backtest,
    run_predictive_failure_scoring,
)
from backend.services.statistical_failure_service import (
    exponential_smoothing_forecast,
    score_failure_risk,
)
from backend.services.ingestion_service import create_ingestion_plan
from backend.services.ingestion_service import (
    bronze_writer_sql,
    execute_bronze_write,
    validate_parquet_ticket_schema,
)
from backend.services.operational_catalog_service import initialize_operational_catalog
from backend.services.operational_monitoring_service import build_operational_snapshot
from backend.services.silver_transform_service import execute_silver_write, silver_writer_sql
from backend.services.summary_lake_service import (
    execute_monthly_summary_refresh,
    monthly_summary_partition_uri,
    monthly_summary_writer_sql,
)
from backend.services.benchmark_service import LocalBenchmarkConfig, run_local_benchmark


def ensure_lake(_args):
    lake = ParquetTicketLake()
    return {"status": "ok", "layout": lake.ensure_layout()}


def init_ops(_args):
    return initialize_operational_catalog()


def ops_snapshot(args):
    return build_operational_snapshot(
        job_limit=args.job_limit,
        file_limit=args.file_limit,
        partition_limit=args.partition_limit,
        model_run_limit=args.model_run_limit,
    )


def register_ingestion(args):
    plan = create_ingestion_plan(
        storage_uri=args.storage_uri,
        file_type=args.file_type,
        source=args.source,
        year=args.year,
        month=args.month,
        filename=args.filename,
        checksum_sha256=args.checksum_sha256,
        size_bytes=args.size_bytes,
    )
    return {
        "status": "registered",
        "job": plan.job,
        "file": plan.file,
        "dataset": plan.dataset,
        "source": plan.source,
        "year": plan.year,
        "month": plan.month,
        "bronze_partition_uri": plan.bronze_partition_uri,
        "silver_partition_uri": plan.silver_partition_uri,
    }


def validate_parquet(args):
    return validate_parquet_ticket_schema(args.storage_uri)


def plan_bronze_sql(args):
    columns = [col.strip() for col in args.columns.split(",") if col.strip()]
    if not columns:
        raise ValueError("--columns must contain at least one column name")
    sql = bronze_writer_sql(
        source_uri=args.source_uri,
        target_partition_uri=args.target_partition_uri,
        raw_columns=columns,
        source=args.source,
        year=args.year,
        month=args.month,
    )
    return {"sql": sql}


def execute_bronze(args):
    columns = [col.strip() for col in args.columns.split(",") if col.strip()]
    if not columns:
        raise ValueError("--columns must contain at least one column name")
    return execute_bronze_write(
        source_uri=args.source_uri,
        target_partition_uri=args.target_partition_uri,
        raw_columns=columns,
        source=args.source,
        year=args.year,
        month=args.month,
        dataset=args.dataset,
        job_id=args.job_id,
    )


def plan_silver_sql(args):
    columns = [col.strip() for col in args.columns.split(",") if col.strip()]
    if not columns:
        raise ValueError("--columns must contain at least one column name")
    sql = silver_writer_sql(
        bronze_uri=args.bronze_uri,
        target_partition_uri=args.target_partition_uri,
        available_columns=columns,
        source=args.source,
        year=args.year,
        month=args.month,
    )
    return {"sql": sql}


def execute_silver(args):
    return execute_silver_write(
        bronze_uri=args.bronze_uri,
        target_partition_uri=args.target_partition_uri,
        source=args.source,
        year=args.year,
        month=args.month,
        dataset=args.dataset,
        job_id=args.job_id,
    )


def plan_monthly_summary_sql(args):
    target_partition_uri = args.target_partition_uri or monthly_summary_partition_uri(
        source=args.source,
        year=args.year,
        month=args.month,
    )
    sql = monthly_summary_writer_sql(
        silver_uri=args.silver_uri,
        target_partition_uri=target_partition_uri,
        source=args.source,
        year=args.year,
        month=args.month,
    )
    return {"target_partition_uri": target_partition_uri, "sql": sql}


def execute_monthly_summary(args):
    target_partition_uri = args.target_partition_uri or monthly_summary_partition_uri(
        source=args.source,
        year=args.year,
        month=args.month,
    )
    return execute_monthly_summary_refresh(
        silver_uri=args.silver_uri,
        target_partition_uri=target_partition_uri,
        source=args.source,
        year=args.year,
        month=args.month,
        job_id=args.job_id,
    )


def plan_predictive_features(args):
    sql = predictive_feature_sql(
        silver_uri=args.silver_uri,
        entity_level=args.entity_level,
        window_start=args.window_start,
        window_end=args.window_end,
        source=args.source,
    )
    return {"sql": sql}


def execute_predictive_risk(args):
    return run_predictive_failure_scoring(
        PredictiveRunConfig(
            silver_uri=args.silver_uri,
            entity_level=args.entity_level,
            window_start=args.window_start,
            window_end=args.window_end,
            source=args.source,
            as_of_date=args.as_of_date,
            horizon=args.horizon,
            limit=args.limit,
            persist_model_runs=not args.no_persist_model_runs,
            job_id=args.job_id,
        )
    )


def execute_predictive_backtest(args):
    return run_predictive_backtest(
        PredictiveBacktestConfig(
            silver_uri=args.silver_uri,
            entity_level=args.entity_level,
            train_start=args.train_start,
            train_end=args.train_end,
            outcome_start=args.outcome_start,
            outcome_end=args.outcome_end,
            source=args.source,
            as_of_date=args.as_of_date,
            horizon=args.horizon,
            risk_threshold=args.risk_threshold,
            min_actual_tickets=args.min_actual_tickets,
            persist_model_run=not args.no_persist_model_run,
            job_id=args.job_id,
        )
    )


def print_kpi_sql(args):
    filters = TicketLakeFilter(
        year_month_from=args.year_month_from,
        year_month_to=args.year_month_to,
        source=args.source,
        entity_level=args.entity_level,
        entity_id=args.entity_id,
    )
    sql, params = ParquetTicketLake().kpi_query(filters)
    return {"sql": sql, "params": params}


def predictive_smoke(_args):
    monthly_counts = [5, 8, 12, 15, 18, 22]
    mttr_values = [300, 330, 420, 480]
    forecast = exponential_smoothing_forecast(monthly_counts, horizon=3)
    risk = score_failure_risk(
        monthly_ticket_counts=monthly_counts,
        days_since_last_ticket=4,
        critical_major_pct=35,
        repeat_pct=18,
        mttr_values=mttr_values,
        escalation_pct=12,
        anomaly_count=2,
    )
    return {
        "status": "ok",
        "forecast": forecast.__dict__,
        "risk": {
            "total_score": risk.total_score,
            "risk_level": risk.risk_level,
            "top_factors": risk.top_factors,
            "features": risk.features.__dict__,
        },
    }


def benchmark_local(args):
    return run_local_benchmark(
        LocalBenchmarkConfig(
            output_dir=args.output_dir,
            total_rows=args.rows,
            months=args.months,
            source=args.source,
            start_year=args.start_year,
            start_month=args.start_month,
            site_count=args.site_count,
            run_predictive=not args.skip_predictive,
            run_backtest=not args.skip_backtest,
            persist_model_runs=args.persist_model_runs,
        )
    )


def build_parser():
    parser = argparse.ArgumentParser(description="NOC-IS Cloud Run job commands")
    sub = parser.add_subparsers(dest="command", required=True)

    ensure = sub.add_parser("ensure-lake", help="Create local Parquet lake directories")
    ensure.set_defaults(func=ensure_lake)

    ops = sub.add_parser("init-ops", help="Create operational catalog tables")
    ops.set_defaults(func=init_ops)

    snapshot = sub.add_parser("ops-snapshot", help="Print operational monitoring summary")
    snapshot.add_argument("--job-limit", type=int, default=100)
    snapshot.add_argument("--file-limit", type=int, default=100)
    snapshot.add_argument("--partition-limit", type=int, default=250)
    snapshot.add_argument("--model-run-limit", type=int, default=100)
    snapshot.set_defaults(func=ops_snapshot)

    ingest = sub.add_parser("register-ingestion", help="Register a Parquet ingestion job and source file")
    ingest.add_argument("storage_uri")
    ingest.add_argument("--file-type", default="ticket")
    ingest.add_argument("--source", default="unknown")
    ingest.add_argument("--year", type=int, default=None)
    ingest.add_argument("--month", type=int, default=None)
    ingest.add_argument("--filename", default=None)
    ingest.add_argument("--checksum-sha256", default=None)
    ingest.add_argument("--size-bytes", type=int, default=None)
    ingest.set_defaults(func=register_ingestion)

    validate = sub.add_parser("validate-parquet", help="Inspect Parquet metadata and validate ticket schema")
    validate.add_argument("storage_uri")
    validate.set_defaults(func=validate_parquet)

    bronze = sub.add_parser("plan-bronze-sql", help="Generate DuckDB SQL to write a bronze Parquet partition")
    bronze.add_argument("source_uri")
    bronze.add_argument("target_partition_uri")
    bronze.add_argument("--columns", required=True, help="Comma-separated source column names")
    bronze.add_argument("--source", default="unknown")
    bronze.add_argument("--year", type=int, default=None)
    bronze.add_argument("--month", type=int, default=None)
    bronze.set_defaults(func=plan_bronze_sql)

    bronze_exec = sub.add_parser("execute-bronze", help="Write a bronze Parquet partition and register it")
    bronze_exec.add_argument("source_uri")
    bronze_exec.add_argument("target_partition_uri")
    bronze_exec.add_argument("--columns", required=True, help="Comma-separated source column names")
    bronze_exec.add_argument("--source", default="unknown")
    bronze_exec.add_argument("--year", type=int, default=None)
    bronze_exec.add_argument("--month", type=int, default=None)
    bronze_exec.add_argument("--dataset", default="tickets")
    bronze_exec.add_argument("--job-id", default=None)
    bronze_exec.set_defaults(func=execute_bronze)

    silver = sub.add_parser("plan-silver-sql", help="Generate DuckDB SQL to write a silver Parquet partition")
    silver.add_argument("bronze_uri")
    silver.add_argument("target_partition_uri")
    silver.add_argument("--columns", required=True, help="Comma-separated bronze column names")
    silver.add_argument("--source", default="unknown")
    silver.add_argument("--year", type=int, default=None)
    silver.add_argument("--month", type=int, default=None)
    silver.set_defaults(func=plan_silver_sql)

    silver_exec = sub.add_parser("execute-silver", help="Write a silver Parquet partition and register it")
    silver_exec.add_argument("bronze_uri")
    silver_exec.add_argument("target_partition_uri")
    silver_exec.add_argument("--source", default="unknown")
    silver_exec.add_argument("--year", type=int, default=None)
    silver_exec.add_argument("--month", type=int, default=None)
    silver_exec.add_argument("--dataset", default="tickets")
    silver_exec.add_argument("--job-id", default=None)
    silver_exec.set_defaults(func=execute_silver)

    summary = sub.add_parser("plan-monthly-summary-sql", help="Generate DuckDB SQL to refresh a monthly summary partition")
    summary.add_argument("silver_uri")
    summary.add_argument("--source", required=True)
    summary.add_argument("--year", type=int, required=True)
    summary.add_argument("--month", type=int, required=True)
    summary.add_argument("--target-partition-uri", default=None)
    summary.set_defaults(func=plan_monthly_summary_sql)

    summary_exec = sub.add_parser("execute-monthly-summary", help="Refresh a monthly summary Parquet partition")
    summary_exec.add_argument("silver_uri")
    summary_exec.add_argument("--source", required=True)
    summary_exec.add_argument("--year", type=int, required=True)
    summary_exec.add_argument("--month", type=int, required=True)
    summary_exec.add_argument("--target-partition-uri", default=None)
    summary_exec.add_argument("--job-id", default=None)
    summary_exec.set_defaults(func=execute_monthly_summary)

    predictive_sql = sub.add_parser("plan-predictive-features", help="Generate DuckDB SQL for predictive feature extraction")
    predictive_sql.add_argument("silver_uri")
    predictive_sql.add_argument("--entity-level", default="site", choices=["area", "regional", "nop", "to", "site"])
    predictive_sql.add_argument("--window-start", required=True, help="YYYY-MM")
    predictive_sql.add_argument("--window-end", required=True, help="YYYY-MM")
    predictive_sql.add_argument("--source", default=None)
    predictive_sql.set_defaults(func=plan_predictive_features)

    predictive_exec = sub.add_parser("execute-predictive-risk", help="Score statistical failure risk from silver Parquet")
    predictive_exec.add_argument("silver_uri")
    predictive_exec.add_argument("--entity-level", default="site", choices=["area", "regional", "nop", "to", "site"])
    predictive_exec.add_argument("--window-start", required=True, help="YYYY-MM")
    predictive_exec.add_argument("--window-end", required=True, help="YYYY-MM")
    predictive_exec.add_argument("--source", default=None)
    predictive_exec.add_argument("--as-of-date", default=None, help="YYYY-MM-DD; defaults to latest ticket date")
    predictive_exec.add_argument("--horizon", type=int, default=3)
    predictive_exec.add_argument("--limit", type=int, default=100)
    predictive_exec.add_argument("--job-id", default=None)
    predictive_exec.add_argument("--no-persist-model-runs", action="store_true")
    predictive_exec.set_defaults(func=execute_predictive_risk)

    backtest = sub.add_parser("execute-predictive-backtest", help="Backtest statistical failure risk against a future outcome window")
    backtest.add_argument("silver_uri")
    backtest.add_argument("--entity-level", default="site", choices=["area", "regional", "nop", "to", "site"])
    backtest.add_argument("--train-start", required=True, help="YYYY-MM")
    backtest.add_argument("--train-end", required=True, help="YYYY-MM")
    backtest.add_argument("--outcome-start", required=True, help="YYYY-MM")
    backtest.add_argument("--outcome-end", required=True, help="YYYY-MM")
    backtest.add_argument("--source", default=None)
    backtest.add_argument("--as-of-date", default=None, help="YYYY-MM-DD; defaults to train window end date")
    backtest.add_argument("--horizon", type=int, default=3)
    backtest.add_argument("--risk-threshold", type=float, default=55.0)
    backtest.add_argument("--min-actual-tickets", type=int, default=1)
    backtest.add_argument("--job-id", default=None)
    backtest.add_argument("--no-persist-model-run", action="store_true")
    backtest.set_defaults(func=execute_predictive_backtest)

    kpi_sql = sub.add_parser("print-kpi-sql", help="Print DuckDB SQL for Parquet KPI scans")
    kpi_sql.add_argument("--year-month-from", default=None)
    kpi_sql.add_argument("--year-month-to", default=None)
    kpi_sql.add_argument("--source", default=None)
    kpi_sql.add_argument("--entity-level", default=None)
    kpi_sql.add_argument("--entity-id", default=None)
    kpi_sql.set_defaults(func=print_kpi_sql)

    smoke = sub.add_parser("predictive-smoke", help="Run a lightweight predictive scoring smoke test")
    smoke.set_defaults(func=predictive_smoke)

    benchmark = sub.add_parser("benchmark-local", help="Generate synthetic Parquet and benchmark the local pipeline")
    benchmark.add_argument("--rows", type=int, default=100_000, help="Total synthetic ticket rows")
    benchmark.add_argument("--months", type=int, default=3, help="Number of monthly partitions")
    benchmark.add_argument("--source", default="swfm_realtime")
    benchmark.add_argument("--start-year", type=int, default=2026)
    benchmark.add_argument("--start-month", type=int, default=1)
    benchmark.add_argument("--site-count", type=int, default=1_000)
    benchmark.add_argument("--output-dir", default=None)
    benchmark.add_argument("--skip-predictive", action="store_true")
    benchmark.add_argument("--skip-backtest", action="store_true")
    benchmark.add_argument("--persist-model-runs", action="store_true")
    benchmark.set_defaults(func=benchmark_local)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    result = args.func(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
