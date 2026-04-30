from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


SYNTHETIC_TICKET_COLUMNS = [
    "ticket_number_inap",
    "ticket_number_swfm",
    "site_id",
    "site_name",
    "site_class",
    "severity",
    "type_ticket",
    "fault_level",
    "cluster_to",
    "nop",
    "regional",
    "area",
    "occured_time",
    "created_at",
    "take_over_date",
    "cleared_time",
    "sla_status",
    "is_escalate",
    "is_auto_resolved",
    "rc_category",
    "summary",
]


@dataclass(frozen=True)
class LocalBenchmarkConfig:
    output_dir: str | None = None
    total_rows: int = 100_000
    months: int = 3
    source: str = "swfm_realtime"
    start_year: int = 2026
    start_month: int = 1
    site_count: int = 1_000
    run_predictive: bool = True
    run_backtest: bool = True
    persist_model_runs: bool = False


def run_local_benchmark(config: LocalBenchmarkConfig) -> dict:
    if config.total_rows <= 0:
        raise ValueError("total_rows must be positive")
    if config.months <= 0:
        raise ValueError("months must be positive")
    if config.site_count <= 0:
        raise ValueError("site_count must be positive")

    output_dir = Path(config.output_dir) if config.output_dir else default_benchmark_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    _configure_isolated_runtime(output_dir)

    periods = month_sequence(config.start_year, config.start_month, config.months)
    monthly_counts = distribute_rows(config.total_rows, config.months)
    stage_totals: dict[str, float] = {
        "generate_raw": 0.0,
        "register_ingestion": 0.0,
        "bronze_write": 0.0,
        "silver_write": 0.0,
        "monthly_summary": 0.0,
        "predictive_risk": 0.0,
        "predictive_backtest": 0.0,
    }
    partitions = []

    benchmark_started = time.perf_counter()
    for index, ((year, month), row_count) in enumerate(zip(periods, monthly_counts)):
        raw_path = output_dir / "raw" / f"tickets_{config.source}_{year}-{month:02d}.parquet"

        with timed(stage_totals, "generate_raw"):
            raw_file = create_synthetic_ticket_parquet(
                raw_path,
                row_count=row_count,
                source=config.source,
                year=year,
                month=month,
                site_count=config.site_count,
                period_index=index,
            )

        with timed(stage_totals, "register_ingestion"):
            from backend.services.ingestion_service import create_ingestion_plan

            plan = create_ingestion_plan(
                storage_uri=str(raw_file),
                file_type="ticket",
                source=config.source,
                year=year,
                month=month,
            )

        with timed(stage_totals, "bronze_write"):
            from backend.services.ingestion_service import execute_bronze_write

            bronze = execute_bronze_write(
                source_uri=str(raw_file),
                target_partition_uri=plan.bronze_partition_uri,
                raw_columns=SYNTHETIC_TICKET_COLUMNS,
                source=config.source,
                year=year,
                month=month,
                dataset=plan.dataset,
                job_id=plan.job["job_id"],
            )

        with timed(stage_totals, "silver_write"):
            from backend.services.silver_transform_service import execute_silver_write

            silver = execute_silver_write(
                bronze_uri=bronze["partition_uri"],
                target_partition_uri=plan.silver_partition_uri,
                source=config.source,
                year=year,
                month=month,
                dataset=plan.dataset,
            )

        with timed(stage_totals, "monthly_summary"):
            from backend.services.summary_lake_service import (
                execute_monthly_summary_refresh,
                monthly_summary_partition_uri,
            )

            summary = execute_monthly_summary_refresh(
                silver_uri=silver["partition_uri"],
                target_partition_uri=monthly_summary_partition_uri(config.source, year, month),
                source=config.source,
                year=year,
                month=month,
            )

        partitions.append(
            {
                "period": f"{year}-{month:02d}",
                "raw_uri": str(raw_file),
                "rows": row_count,
                "raw_size_bytes": raw_file.stat().st_size,
                "bronze_uri": bronze["output_uri"],
                "bronze_size_bytes": bronze["size_bytes"],
                "silver_uri": silver["output_uri"],
                "silver_size_bytes": silver["size_bytes"],
                "summary_uri": summary["output_uri"],
                "summary_rows": summary["row_count"],
                "summary_size_bytes": summary["size_bytes"],
            }
        )

    silver_scan_uri = silver_partition_scan_uri(output_dir)
    predictive = None
    backtest = None
    if config.run_predictive:
        with timed(stage_totals, "predictive_risk"):
            from backend.services.predictive_lake_service import PredictiveRunConfig, run_predictive_failure_scoring

            predictive = run_predictive_failure_scoring(
                PredictiveRunConfig(
                    silver_uri=silver_scan_uri,
                    entity_level="site",
                    window_start=format_period(periods[0]),
                    window_end=format_period(periods[-1]),
                    source=config.source,
                    limit=min(100, config.site_count),
                    persist_model_runs=config.persist_model_runs,
                )
            )

    if config.run_backtest and config.months >= 2:
        with timed(stage_totals, "predictive_backtest"):
            from backend.services.predictive_lake_service import PredictiveBacktestConfig, run_predictive_backtest

            backtest = run_predictive_backtest(
                PredictiveBacktestConfig(
                    silver_uri=silver_scan_uri,
                    entity_level="site",
                    train_start=format_period(periods[0]),
                    train_end=format_period(periods[-2]),
                    outcome_start=format_period(periods[-1]),
                    outcome_end=format_period(periods[-1]),
                    source=config.source,
                    persist_model_run=config.persist_model_runs,
                )
            )

    total_seconds = round(time.perf_counter() - benchmark_started, 4)
    result = {
        "status": "completed",
        "output_dir": str(output_dir),
        "config": {
            "total_rows": config.total_rows,
            "months": config.months,
            "source": config.source,
            "start_year": config.start_year,
            "start_month": config.start_month,
            "site_count": config.site_count,
            "run_predictive": config.run_predictive,
            "run_backtest": config.run_backtest,
            "persist_model_runs": config.persist_model_runs,
        },
        "totals": {
            "rows": sum(item["rows"] for item in partitions),
            "raw_size_bytes": sum(item["raw_size_bytes"] for item in partitions),
            "bronze_size_bytes": sum(item["bronze_size_bytes"] for item in partitions),
            "silver_size_bytes": sum(item["silver_size_bytes"] for item in partitions),
            "summary_size_bytes": sum(item["summary_size_bytes"] for item in partitions),
            "summary_rows": sum(item["summary_rows"] for item in partitions),
            "total_seconds": total_seconds,
            "rows_per_second_end_to_end": rate(config.total_rows, total_seconds),
        },
        "timings_seconds": {key: round(value, 4) for key, value in stage_totals.items()},
        "throughput_rows_per_second": {
            key: rate(config.total_rows, seconds)
            for key, seconds in stage_totals.items()
            if seconds > 0
        },
        "partitions": partitions,
        "predictive": compact_predictive_result(predictive),
        "backtest": compact_backtest_result(backtest),
    }

    result_path = output_dir / "benchmark_result.json"
    result_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    result["result_path"] = str(result_path)
    return result


def create_synthetic_ticket_parquet(
    path: Path,
    row_count: int,
    source: str,
    year: int,
    month: int,
    site_count: int,
    period_index: int = 0,
) -> Path:
    import duckdb

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    source_sql = escape_sql(source)
    base = f"{int(year):04d}-{int(month):02d}-01 00:00:00"

    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute(
            f"""
            COPY (
                WITH generated AS (
                    SELECT
                        i::BIGINT AS idx,
                        TIMESTAMP '{base}'
                          + CAST(i % 28 AS INTEGER) * INTERVAL 1 DAY
                          + CAST(i % 24 AS INTEGER) * INTERVAL 1 HOUR AS occured_ts,
                        printf('SITE-BENCH-%05d', i % {int(site_count)}) AS site_key
                    FROM range({int(row_count)}) AS t(i)
                )
                SELECT
                    CASE
                        WHEN idx > 0 AND idx % 1000 = 0 THEN printf('BENCH-INAP-{period_index:02d}-%08d', idx - 1)
                        ELSE printf('BENCH-INAP-{period_index:02d}-%08d', idx)
                    END AS ticket_number_inap,
                    printf('BENCH-SWFM-{period_index:02d}-%08d', idx) AS ticket_number_swfm,
                    site_key AS site_id,
                    concat('Synthetic Site ', site_key) AS site_name,
                    CASE WHEN idx % 5 = 0 THEN 'Platinum' WHEN idx % 5 = 1 THEN 'Gold' ELSE 'Silver' END AS site_class,
                    CASE
                        WHEN idx % 20 = 0 THEN 'Critical'
                        WHEN idx % 5 = 0 THEN 'Major'
                        WHEN idx % 3 = 0 THEN 'Minor'
                        ELSE 'Low'
                    END AS severity,
                    CASE WHEN idx % 4 = 0 THEN 'Power' WHEN idx % 4 = 1 THEN 'Transmission' WHEN idx % 4 = 2 THEN 'Radio' ELSE 'Synthetic' END AS type_ticket,
                    CASE WHEN idx % 6 = 0 THEN 'Site Down' WHEN idx % 6 = 1 THEN 'Degraded' ELSE 'Intermittent' END AS fault_level,
                    printf('TO-BENCH-%03d', idx % 50) AS cluster_to,
                    printf('NOP-BENCH-%03d', idx % 20) AS nop,
                    printf('REG-BENCH-%03d', idx % 10) AS regional,
                    printf('AREA-BENCH-%02d', idx % 5) AS area,
                    CAST(occured_ts AS VARCHAR) AS occured_time,
                    CAST(occured_ts + INTERVAL 5 MINUTE AS VARCHAR) AS created_at,
                    CAST(occured_ts + INTERVAL 20 MINUTE AS VARCHAR) AS take_over_date,
                    CAST(occured_ts + CAST(45 + (idx % 360) AS INTEGER) * INTERVAL 1 MINUTE AS VARCHAR) AS cleared_time,
                    CASE WHEN idx % 7 = 0 THEN 'OUT SLA' ELSE 'IN SLA' END AS sla_status,
                    CASE WHEN idx % 11 = 0 THEN 'Y' ELSE 'N' END AS is_escalate,
                    CASE WHEN idx % 13 = 0 THEN 'AUTO RESOLVED' ELSE 'N' END AS is_auto_resolved,
                    CASE WHEN idx % 4 = 0 THEN 'Power' WHEN idx % 4 = 1 THEN 'Transport' WHEN idx % 4 = 2 THEN 'Radio' ELSE 'Other' END AS rc_category,
                    concat('Synthetic benchmark ticket ', idx, ' from {source_sql}') AS summary
                FROM generated
            ) TO '{duckdb_path(path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
    finally:
        conn.close()
    return path


def default_benchmark_dir(config: LocalBenchmarkConfig) -> Path:
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return Path.cwd() / ".test_tmp" / "benchmarks" / f"{stamp}_{config.total_rows}_rows"


def _configure_isolated_runtime(output_dir: Path) -> None:
    import backend.config as app_config
    import backend.database as database
    import backend.services.ingestion_service as ingestion
    import backend.services.parquet_lake_service as lake
    import backend.services.summary_lake_service as summary

    data_dir = output_dir / "catalog"
    lake_root = output_dir / "lake"
    data_dir.mkdir(parents=True, exist_ok=True)
    lake_root.mkdir(parents=True, exist_ok=True)

    app_config.DATA_DIR = str(data_dir)
    app_config.BACKUP_DIR = str(data_dir / "backups")
    app_config.DB_PATH = str(data_dir / "benchmark_catalog.duckdb")
    database.DB_PATH = app_config.DB_PATH
    lake.LAKE_ROOT = str(lake_root)
    lake.TICKET_DATASET = str(lake_root / "tickets")
    ingestion.LAKE_ROOT = str(lake_root)
    summary.LAKE_ROOT = str(lake_root)


class timed:
    def __init__(self, totals: dict[str, float], key: str):
        self.totals = totals
        self.key = key
        self.started = 0.0

    def __enter__(self):
        self.started = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.totals[self.key] = self.totals.get(self.key, 0.0) + (time.perf_counter() - self.started)


def distribute_rows(total_rows: int, months: int) -> list[int]:
    base = total_rows // months
    remainder = total_rows % months
    return [base + (1 if index < remainder else 0) for index in range(months)]


def month_sequence(start_year: int, start_month: int, months: int) -> list[tuple[int, int]]:
    if not 1 <= start_month <= 12:
        raise ValueError("start_month must be between 1 and 12")
    values = []
    year = start_year
    month = start_month
    for _ in range(months):
        values.append((year, month))
        month += 1
        if month > 12:
            year += 1
            month = 1
    return values


def format_period(period: tuple[int, int]) -> str:
    return f"{period[0]:04d}-{period[1]:02d}"


def silver_partition_scan_uri(output_dir: Path) -> str:
    return str(output_dir / "lake" / "tickets" / "silver" / "year=*" / "month=*" / "source=*")


def compact_predictive_result(result: dict | None) -> dict | None:
    if not result:
        return None
    predictions = result.get("predictions") or []
    return {
        "feature_rows": result.get("feature_rows"),
        "prediction_count": result.get("prediction_count"),
        "top_predictions": predictions[: min(5, len(predictions))],
    }


def compact_backtest_result(result: dict | None) -> dict | None:
    if not result:
        return None
    return {
        "feature_rows": result.get("feature_rows"),
        "evaluated_entities": result.get("evaluated_entities"),
        "confusion": result.get("confusion"),
        "metrics": result.get("metrics"),
    }


def rate(count: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return round(count / seconds, 2)


def escape_sql(value: Any) -> str:
    return str(value).replace("'", "''")


def duckdb_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")
