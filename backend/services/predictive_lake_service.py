from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from backend.services.ingestion_service import configure_duckdb_connection, escape_sql_string, normalize_storage_uri
from backend.services.silver_transform_service import parquet_read_uri
from backend.services.statistical_failure_service import (
    TimeSeriesPoint,
    exponential_smoothing_forecast,
    robust_mad_anomalies,
    score_failure_risk,
)


MODEL_NAME = "statistical_failure_baseline"
MODEL_VERSION = "2026.04.30"

ENTITY_COLUMNS = {
    "area": "calc_area_id",
    "regional": "calc_regional_id",
    "nop": "calc_nop_id",
    "to": "calc_to_id",
    "site": "site_id",
}


@dataclass(frozen=True)
class PredictiveRunConfig:
    silver_uri: str
    entity_level: str = "site"
    window_start: str = ""
    window_end: str = ""
    source: str | None = None
    as_of_date: str | None = None
    horizon: int = 3
    limit: int = 100
    persist_model_runs: bool = True
    job_id: str | None = None


@dataclass(frozen=True)
class PredictiveBacktestConfig:
    silver_uri: str
    entity_level: str = "site"
    train_start: str = ""
    train_end: str = ""
    outcome_start: str = ""
    outcome_end: str = ""
    source: str | None = None
    as_of_date: str | None = None
    horizon: int = 3
    risk_threshold: float = 55.0
    min_actual_tickets: int = 1
    persist_model_run: bool = True
    job_id: str | None = None


def predictive_feature_sql(
    silver_uri: str,
    entity_level: str,
    window_start: str,
    window_end: str,
    source: str | None = None,
) -> str:
    entity_col = entity_column(entity_level)
    read_uri = parquet_read_uri(normalize_storage_uri(silver_uri))
    period_start = validate_year_month(window_start, "window_start")
    period_end = validate_year_month(window_end, "window_end")
    source_filter = ""
    if source:
        source_filter = f"AND COALESCE(calc_source, '') = '{escape_sql_string(source)}'"

    return f"""
WITH base AS (
    SELECT
        CAST({entity_col} AS VARCHAR) AS entity_id,
        calc_year_month,
        try_cast(occured_time AS TIMESTAMP) AS occured_time,
        CAST(severity AS VARCHAR) AS severity,
        try_cast(calc_restore_time_min AS DOUBLE) AS calc_restore_time_min,
        CAST(is_escalate AS VARCHAR) AS is_escalate,
        CAST(ticket_number_inap AS VARCHAR) AS ticket_number_inap,
        CAST(ticket_number_swfm AS VARCHAR) AS ticket_number_swfm,
        COALESCE(calc_source, '') AS calc_source
    FROM read_parquet('{escape_sql_string(read_uri)}', hive_partitioning = true, union_by_name = true)
    WHERE {entity_col} IS NOT NULL
      AND calc_year_month >= '{period_start}'
      AND calc_year_month <= '{period_end}'
      {source_filter}
)
SELECT
    entity_id,
    calc_year_month,
    COUNT(*) AS monthly_ticket_count,
    AVG(calc_restore_time_min) AS avg_mttr_min,
    SUM(CASE WHEN UPPER(COALESCE(severity, '')) IN ('CRITICAL', 'MAJOR') THEN 1 ELSE 0 END) AS critical_major_count,
    SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS escalated_count,
    COUNT(*) - COUNT(DISTINCT COALESCE(
        NULLIF(TRIM(ticket_number_inap), ''),
        NULLIF(TRIM(ticket_number_swfm), ''),
        concat(entity_id, '|', COALESCE(CAST(occured_time AS VARCHAR), ''), '|', COALESCE(severity, ''))
    )) AS duplicate_ticket_count,
    MAX(CAST(occured_time AS DATE)) AS last_occured_date
FROM base
GROUP BY entity_id, calc_year_month
ORDER BY entity_id, calc_year_month
""".strip()


def run_predictive_failure_scoring(config: PredictiveRunConfig) -> dict:
    from backend.services.operational_catalog_service import register_model_run, update_job

    normalized_silver_uri = normalize_storage_uri(config.silver_uri)
    read_uri = parquet_read_uri(normalized_silver_uri)
    if config.job_id:
        update_job(
            config.job_id,
            status="running",
            progress_phase="predictive_features",
            progress_current=0,
            progress_total=1,
        )

    conn = None
    try:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        configure_duckdb_connection(conn, [read_uri])
        rows = extract_predictive_feature_rows(
            conn,
            silver_uri=normalized_silver_uri,
            entity_level=config.entity_level,
            window_start=config.window_start,
            window_end=config.window_end,
            source=config.source,
        )
        predictions = score_feature_rows(
            rows,
            entity_level=config.entity_level,
            window_start=config.window_start,
            window_end=config.window_end,
            as_of_date=config.as_of_date,
            horizon=config.horizon,
            limit=config.limit,
        )

        model_runs = []
        if config.persist_model_runs:
            for prediction in predictions:
                model_runs.append(
                    register_model_run(
                        model_name=MODEL_NAME,
                        model_version=MODEL_VERSION,
                        entity_level=config.entity_level,
                        entity_id=prediction["entity_id"],
                        window_start=config.window_start,
                        window_end=config.window_end,
                        parameters={
                            "silver_uri": normalized_silver_uri,
                            "source": config.source,
                            "as_of_date": prediction["as_of_date"],
                            "horizon": config.horizon,
                        },
                        metrics=prediction,
                        status="completed",
                        job_id=config.job_id,
                    )
                )

        result = {
            "status": "completed",
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "entity_level": config.entity_level,
            "window_start": config.window_start,
            "window_end": config.window_end,
            "source": config.source,
            "feature_rows": len(rows),
            "prediction_count": len(predictions),
            "predictions": predictions,
            "model_runs": model_runs,
        }
        if config.job_id:
            update_job(
                config.job_id,
                status="completed",
                result={
                    **result,
                    "predictions": predictions[: min(10, len(predictions))],
                    "model_runs": model_runs[: min(10, len(model_runs))],
                },
                progress_phase="completed",
                progress_current=1,
                progress_total=1,
            )
        return result
    except Exception as exc:
        if config.job_id:
            update_job(config.job_id, status="failed", error_message=str(exc), progress_phase="failed")
        raise
    finally:
        if conn is not None:
            conn.close()


def run_predictive_backtest(config: PredictiveBacktestConfig) -> dict:
    from backend.services.operational_catalog_service import register_model_run, update_job

    normalized_silver_uri = normalize_storage_uri(config.silver_uri)
    read_uri = parquet_read_uri(normalized_silver_uri)
    if config.job_id:
        update_job(
            config.job_id,
            status="running",
            progress_phase="predictive_backtest",
            progress_current=0,
            progress_total=1,
        )

    conn = None
    try:
        import duckdb

        conn = duckdb.connect(database=":memory:")
        configure_duckdb_connection(conn, [read_uri])
        rows = extract_predictive_feature_rows(
            conn,
            silver_uri=normalized_silver_uri,
            entity_level=config.entity_level,
            window_start=config.train_start,
            window_end=config.outcome_end,
            source=config.source,
        )
        result = backtest_feature_rows(
            rows,
            entity_level=config.entity_level,
            train_start=config.train_start,
            train_end=config.train_end,
            outcome_start=config.outcome_start,
            outcome_end=config.outcome_end,
            as_of_date=config.as_of_date,
            horizon=config.horizon,
            risk_threshold=config.risk_threshold,
            min_actual_tickets=config.min_actual_tickets,
        )
        result.update(
            {
                "status": "completed",
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "source": config.source,
                "feature_rows": len(rows),
            }
        )

        model_run = None
        if config.persist_model_run:
            model_run = register_model_run(
                model_name=f"{MODEL_NAME}_backtest",
                model_version=MODEL_VERSION,
                entity_level=config.entity_level,
                entity_id="*",
                window_start=config.train_start,
                window_end=config.outcome_end,
                parameters={
                    "silver_uri": normalized_silver_uri,
                    "source": config.source,
                    "train_start": config.train_start,
                    "train_end": config.train_end,
                    "outcome_start": config.outcome_start,
                    "outcome_end": config.outcome_end,
                    "as_of_date": result["as_of_date"],
                    "horizon": config.horizon,
                    "risk_threshold": config.risk_threshold,
                    "min_actual_tickets": config.min_actual_tickets,
                },
                metrics=result,
                status="completed",
                job_id=config.job_id,
            )
        result["model_run"] = model_run

        if config.job_id:
            update_job(
                config.job_id,
                status="completed",
                result={**result, "evaluations": result["evaluations"][:20]},
                progress_phase="completed",
                progress_current=1,
                progress_total=1,
            )
        return result
    except Exception as exc:
        if config.job_id:
            update_job(config.job_id, status="failed", error_message=str(exc), progress_phase="failed")
        raise
    finally:
        if conn is not None:
            conn.close()


def extract_predictive_feature_rows(
    conn,
    silver_uri: str,
    entity_level: str,
    window_start: str,
    window_end: str,
    source: str | None = None,
) -> list[dict]:
    sql = predictive_feature_sql(
        silver_uri=silver_uri,
        entity_level=entity_level,
        window_start=window_start,
        window_end=window_end,
        source=source,
    )
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def score_feature_rows(
    rows: list[dict],
    entity_level: str,
    window_start: str,
    window_end: str,
    as_of_date: str | None = None,
    horizon: int = 3,
    limit: int = 100,
) -> list[dict]:
    periods = month_range(validate_year_month(window_start, "window_start"), validate_year_month(window_end, "window_end"))
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        entity_id = str(row.get("entity_id") or "").strip()
        if not entity_id:
            continue
        grouped.setdefault(entity_id, []).append(row)

    default_as_of = parse_date(as_of_date) or latest_event_date(rows) or date.today()
    predictions = []

    for entity_id, entity_rows in grouped.items():
        by_period = {str(row.get("calc_year_month")): row for row in entity_rows}
        monthly_counts = [float(by_period.get(period, {}).get("monthly_ticket_count") or 0) for period in periods]
        mttr_values = [
            float(row.get("avg_mttr_min"))
            for row in sorted(entity_rows, key=lambda item: str(item.get("calc_year_month")))
            if row.get("avg_mttr_min") is not None
        ]
        total_tickets = sum(float(row.get("monthly_ticket_count") or 0) for row in entity_rows)
        critical_major = sum(float(row.get("critical_major_count") or 0) for row in entity_rows)
        escalated = sum(float(row.get("escalated_count") or 0) for row in entity_rows)
        duplicated = sum(float(row.get("duplicate_ticket_count") or 0) for row in entity_rows)
        last_date = latest_event_date(entity_rows)
        days_since_last_ticket = (default_as_of - last_date).days if last_date else None
        anomalies = robust_mad_anomalies(
            [TimeSeriesPoint(period=period, value=value) for period, value in zip(periods, monthly_counts)]
        )
        forecast = exponential_smoothing_forecast(monthly_counts, horizon=horizon)
        risk = score_failure_risk(
            monthly_ticket_counts=monthly_counts,
            days_since_last_ticket=days_since_last_ticket,
            critical_major_pct=percent(critical_major, total_tickets),
            repeat_pct=percent(duplicated, total_tickets),
            mttr_values=mttr_values,
            escalation_pct=percent(escalated, total_tickets),
            anomaly_count=len(anomalies),
        )
        predictions.append(
            {
                "entity_level": entity_level,
                "entity_id": entity_id,
                "window_start": periods[0],
                "window_end": periods[-1],
                "as_of_date": default_as_of.isoformat(),
                "total_tickets": int(total_tickets),
                "active_months": sum(1 for value in monthly_counts if value > 0),
                "last_occured_date": last_date.isoformat() if last_date else None,
                "days_since_last_ticket": days_since_last_ticket,
                "monthly_ticket_counts": [int(value) if value.is_integer() else value for value in monthly_counts],
                "avg_mttr_series": [round(value, 2) for value in mttr_values],
                "critical_major_pct": round(percent(critical_major, total_tickets), 2),
                "repeat_pct": round(percent(duplicated, total_tickets), 2),
                "escalation_pct": round(percent(escalated, total_tickets), 2),
                "anomaly_count": len(anomalies),
                "anomalies": [anomaly.__dict__ for anomaly in anomalies],
                "forecast": forecast.__dict__,
                "risk": {
                    "total_score": risk.total_score,
                    "risk_level": risk.risk_level,
                    "top_factors": risk.top_factors,
                    "features": risk.features.__dict__,
                },
            }
        )

    predictions.sort(key=lambda item: item["risk"]["total_score"], reverse=True)
    return predictions[: max(1, min(limit, 10000))]


def backtest_feature_rows(
    rows: list[dict],
    entity_level: str,
    train_start: str,
    train_end: str,
    outcome_start: str,
    outcome_end: str,
    as_of_date: str | None = None,
    horizon: int = 3,
    risk_threshold: float = 55.0,
    min_actual_tickets: int = 1,
) -> dict:
    train_periods = set(month_range(train_start, train_end))
    outcome_periods = set(month_range(outcome_start, outcome_end))
    if max(train_periods) >= min(outcome_periods):
        raise ValueError("outcome_start must be after train_end")

    train_rows = [row for row in rows if str(row.get("calc_year_month")) in train_periods]
    outcome_rows = [row for row in rows if str(row.get("calc_year_month")) in outcome_periods]
    backtest_as_of = parse_date(as_of_date) or period_end_date(train_end)
    predictions = score_feature_rows(
        train_rows,
        entity_level=entity_level,
        window_start=train_start,
        window_end=train_end,
        as_of_date=backtest_as_of.isoformat(),
        horizon=horizon,
        limit=10000,
    )
    prediction_by_entity = {prediction["entity_id"]: prediction for prediction in predictions}
    actuals = actual_ticket_counts(outcome_rows)
    all_entities = sorted(set(prediction_by_entity) | set(actuals))

    evaluations = []
    confusion = {"true_positive": 0, "false_positive": 0, "true_negative": 0, "false_negative": 0}
    for entity_id in all_entities:
        prediction = prediction_by_entity.get(entity_id)
        score = float(prediction["risk"]["total_score"]) if prediction else 0.0
        risk_level = prediction["risk"]["risk_level"] if prediction else "not_scored"
        predicted_positive = score >= risk_threshold
        actual_tickets = int(actuals.get(entity_id, {}).get("actual_tickets", 0))
        actual_critical_major = int(actuals.get(entity_id, {}).get("actual_critical_major", 0))
        actual_positive = actual_tickets >= max(1, min_actual_tickets)
        outcome = confusion_bucket(predicted_positive, actual_positive)
        confusion[outcome] += 1
        evaluations.append(
            {
                "entity_level": entity_level,
                "entity_id": entity_id,
                "risk_score": round(score, 2),
                "risk_level": risk_level,
                "predicted_positive": predicted_positive,
                "actual_positive": actual_positive,
                "actual_tickets": actual_tickets,
                "actual_critical_major": actual_critical_major,
                "outcome": outcome,
            }
        )

    evaluations.sort(key=lambda item: (item["outcome"] != "false_negative", -item["risk_score"], item["entity_id"]))
    metrics = classification_metrics(confusion)
    return {
        "entity_level": entity_level,
        "train_start": validate_year_month(train_start, "train_start"),
        "train_end": validate_year_month(train_end, "train_end"),
        "outcome_start": validate_year_month(outcome_start, "outcome_start"),
        "outcome_end": validate_year_month(outcome_end, "outcome_end"),
        "as_of_date": backtest_as_of.isoformat(),
        "horizon": horizon,
        "risk_threshold": risk_threshold,
        "min_actual_tickets": min_actual_tickets,
        "train_feature_rows": len(train_rows),
        "outcome_feature_rows": len(outcome_rows),
        "evaluated_entities": len(evaluations),
        "confusion": confusion,
        "metrics": metrics,
        "evaluations": evaluations,
    }


def actual_ticket_counts(rows: list[dict]) -> dict[str, dict]:
    actuals: dict[str, dict] = {}
    for row in rows:
        entity_id = str(row.get("entity_id") or "").strip()
        if not entity_id:
            continue
        actual = actuals.setdefault(entity_id, {"actual_tickets": 0, "actual_critical_major": 0})
        actual["actual_tickets"] += int(row.get("monthly_ticket_count") or 0)
        actual["actual_critical_major"] += int(row.get("critical_major_count") or 0)
    return actuals


def confusion_bucket(predicted_positive: bool, actual_positive: bool) -> str:
    if predicted_positive and actual_positive:
        return "true_positive"
    if predicted_positive and not actual_positive:
        return "false_positive"
    if not predicted_positive and actual_positive:
        return "false_negative"
    return "true_negative"


def classification_metrics(confusion: dict[str, int]) -> dict[str, float]:
    tp = confusion.get("true_positive", 0)
    fp = confusion.get("false_positive", 0)
    tn = confusion.get("true_negative", 0)
    fn = confusion.get("false_negative", 0)
    total = tp + fp + tn + fn
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    specificity = tn / (tn + fp) if tn + fp else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / total if total else 0.0
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "specificity": round(specificity, 4),
        "f1": round(f1, 4),
        "accuracy": round(accuracy, 4),
        "evaluated_entities": total,
    }


def period_end_date(year_month: str) -> date:
    period = validate_year_month(year_month, "year_month")
    year = int(period[:4])
    month = int(period[5:7])
    if month == 12:
        return date(year, month, 31)
    next_month = date(year, month + 1, 1)
    return date.fromordinal(next_month.toordinal() - 1)


def entity_column(entity_level: str) -> str:
    try:
        return ENTITY_COLUMNS[entity_level]
    except KeyError:
        allowed = ", ".join(sorted(ENTITY_COLUMNS))
        raise ValueError(f"Unsupported entity_level: {entity_level}. Allowed: {allowed}")


def validate_year_month(value: str, field_name: str) -> str:
    if not value or len(value) != 7 or value[4] != "-":
        raise ValueError(f"{field_name} must use YYYY-MM format")
    year = int(value[:4])
    month = int(value[5:7])
    if year < 2000 or not 1 <= month <= 12:
        raise ValueError(f"{field_name} must use a valid YYYY-MM value")
    return f"{year:04d}-{month:02d}"


def month_range(start: str, end: str) -> list[str]:
    start = validate_year_month(start, "window_start")
    end = validate_year_month(end, "window_end")
    year, month = int(start[:4]), int(start[5:7])
    end_year, end_month = int(end[:4]), int(end[5:7])
    if (year, month) > (end_year, end_month):
        raise ValueError("window_start must be <= window_end")

    periods = []
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return periods


def latest_event_date(rows: list[dict]) -> date | None:
    dates = [parse_date(row.get("last_occured_date")) for row in rows]
    valid = [value for value in dates if value is not None]
    return max(valid) if valid else None


def parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def percent(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator * 100.0 / denominator
