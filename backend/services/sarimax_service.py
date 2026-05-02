from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.database import get_connection
from backend.services.operational_catalog_service import register_model_run, update_job
from backend.services.predictive_lake_service import month_range, validate_year_month


MODEL_NAME = "sarimax_volume_forecast"
MODEL_VERSION = "2026.05.02"

ENTITY_COLUMNS = {
    "area": "area_id",
    "regional": "regional_id",
    "nop": "nop_id",
    "to": "to_id",
    "site": "site_id",
}


@dataclass(frozen=True)
class SarimaxRunConfig:
    entity_level: str = "site"
    entity_id: str | None = None
    window_start: str = ""
    window_end: str = ""
    horizon: int = 3
    limit: int = 100
    min_points: int = 6
    order: tuple[int, int, int] = (1, 1, 1)
    seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 12)
    persist_model_runs: bool = True
    job_id: str | None = None


def run_sarimax_volume_forecast(config: SarimaxRunConfig) -> dict:
    if config.job_id:
        update_job(
            config.job_id,
            status="running",
            progress_phase="sarimax_load_summary",
            progress_current=0,
            progress_total=2,
        )

    try:
        rows = load_summary_volume_rows(
            entity_level=config.entity_level,
            entity_id=config.entity_id,
            window_start=config.window_start,
            window_end=config.window_end,
            limit=config.limit,
        )
        forecasts = forecast_summary_rows(rows, config)
        model_runs = []
        if config.persist_model_runs:
            for forecast in forecasts:
                model_runs.append(
                    register_model_run(
                        model_name=MODEL_NAME,
                        model_version=MODEL_VERSION,
                        entity_level=config.entity_level,
                        entity_id=forecast["entity_id"],
                        window_start=config.window_start,
                        window_end=config.window_end,
                        parameters={
                            "horizon": config.horizon,
                            "order": config.order,
                            "seasonal_order": forecast["method"]["seasonal_order"],
                            "min_points": config.min_points,
                        },
                        metrics=forecast,
                        status="completed",
                        job_id=config.job_id,
                    )
                )

        result = {
            "status": "completed",
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "entity_level": config.entity_level,
            "entity_id": config.entity_id,
            "window_start": validate_year_month(config.window_start, "window_start"),
            "window_end": validate_year_month(config.window_end, "window_end"),
            "horizon": config.horizon,
            "feature_rows": len(rows),
            "forecast_count": len(forecasts),
            "forecasts": forecasts,
            "model_runs": model_runs,
        }
        if config.job_id:
            update_job(
                config.job_id,
                status="completed",
                result={**result, "forecasts": forecasts[:20], "model_runs": model_runs[:20]},
                progress_phase="completed",
                progress_current=2,
                progress_total=2,
            )
        return result
    except Exception as exc:
        if config.job_id:
            update_job(config.job_id, status="failed", error_message=str(exc), progress_phase="failed")
        raise


def load_summary_volume_rows(
    entity_level: str,
    window_start: str,
    window_end: str,
    entity_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    entity_col = entity_column(entity_level)
    start = validate_year_month(window_start, "window_start")
    end = validate_year_month(window_end, "window_end")
    params: list[Any] = [start, end]
    entity_filter = ""
    if entity_id:
        entity_filter = f"AND {entity_col} = ?"
        params.append(entity_id)

    params.append(max(1, min(limit, 10000)))
    with get_connection() as conn:
        cursor = conn.execute(
            f"""
            WITH ranked_entities AS (
                SELECT {entity_col} AS entity_id, SUM(total_tickets) AS total_tickets
                FROM summary_monthly
                WHERE year_month >= ?
                  AND year_month <= ?
                  AND severity IS NULL
                  AND type_ticket IS NULL
                  AND fault_level IS NULL
                  AND {entity_col} IS NOT NULL
                  {entity_filter}
                GROUP BY {entity_col}
                ORDER BY total_tickets DESC
                LIMIT ?
            )
            SELECT
                CAST(s.{entity_col} AS VARCHAR) AS entity_id,
                s.year_month,
                SUM(s.total_tickets) AS total_tickets
            FROM summary_monthly s
            JOIN ranked_entities r ON s.{entity_col} = r.entity_id
            WHERE s.year_month >= '{start}'
              AND s.year_month <= '{end}'
              AND s.severity IS NULL
              AND s.type_ticket IS NULL
              AND s.fault_level IS NULL
            GROUP BY s.{entity_col}, s.year_month
            ORDER BY s.{entity_col}, s.year_month
            """,
            params,
        )
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def forecast_summary_rows(rows: list[dict], config: SarimaxRunConfig) -> list[dict]:
    periods = month_range(config.window_start, config.window_end)
    grouped: dict[str, dict[str, float]] = {}
    totals: dict[str, float] = {}
    for row in rows:
        entity_id = str(row.get("entity_id") or "").strip()
        if not entity_id:
            continue
        period = str(row.get("year_month"))
        value = float(row.get("total_tickets") or 0)
        grouped.setdefault(entity_id, {})[period] = value
        totals[entity_id] = totals.get(entity_id, 0.0) + value

    forecasts = []
    for entity_id in sorted(grouped, key=lambda item: totals.get(item, 0), reverse=True):
        values = [float(grouped[entity_id].get(period, 0.0)) for period in periods]
        if sum(1 for value in values if value > 0) < config.min_points:
            continue
        forecast = fit_sarimax_series(
            values,
            periods,
            horizon=config.horizon,
            order=config.order,
            seasonal_order=config.seasonal_order,
        )
        forecasts.append(
            {
                "entity_level": config.entity_level,
                "entity_id": entity_id,
                "window_start": periods[0],
                "window_end": periods[-1],
                "historical": [{"period": period, "value": int(value)} for period, value in zip(periods, values)],
                **forecast,
            }
        )
    return forecasts[: max(1, min(config.limit, 10000))]


def fit_sarimax_series(
    values: list[float],
    periods: list[str],
    horizon: int,
    order: tuple[int, int, int] = (1, 1, 1),
    seasonal_order: tuple[int, int, int, int] = (1, 1, 1, 12),
) -> dict:
    try:
        from statsmodels.tsa.statespace.sarimax import SARIMAX
    except ImportError as exc:
        raise RuntimeError(
            "statsmodels belum terpasang. Jalankan setup lokal lagi agar SARIMAX aktif."
        ) from exc

    effective_seasonal_order = seasonal_order if len(values) >= seasonal_order[3] * 2 else (0, 0, 0, 0)
    model = SARIMAX(
        values,
        order=order,
        seasonal_order=effective_seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    fitted = model.fit(disp=False, maxiter=75)
    prediction = fitted.get_forecast(steps=horizon)
    predicted_mean = [max(0.0, float(value)) for value in prediction.predicted_mean]
    confidence = prediction.conf_int(alpha=0.05)
    forecast_periods = _future_periods(periods[-1], horizon)

    forecasts = []
    for idx, value in enumerate(predicted_mean):
        ci_lower = max(0.0, float(confidence[idx][0]))
        ci_upper = max(ci_lower, float(confidence[idx][1]))
        forecasts.append(
            {
                "period": forecast_periods[idx],
                "period_offset": idx + 1,
                "forecast": round(value, 2),
                "ci_lower": round(ci_lower, 2),
                "ci_upper": round(ci_upper, 2),
            }
        )

    last_actual = float(values[-1]) if values else 0.0
    first_forecast = forecasts[0]["forecast"] if forecasts else 0.0
    pct_change = (first_forecast - last_actual) / last_actual * 100 if last_actual > 0 else 0.0
    return {
        "forecasts": forecasts,
        "method": {
            "name": "SARIMAX",
            "order": order,
            "seasonal_order": effective_seasonal_order,
            "requested_seasonal_order": seasonal_order,
            "seasonal_enabled": effective_seasonal_order != (0, 0, 0, 0),
        },
        "fit": {
            "aic": round(float(fitted.aic), 4) if fitted.aic is not None else None,
            "bic": round(float(fitted.bic), 4) if fitted.bic is not None else None,
            "observations": len(values),
        },
        "pct_change": round(pct_change, 2),
        "trend_word": "Naik" if pct_change > 5 else ("Turun" if pct_change < -5 else "Stabil"),
        "cached": True,
    }


def latest_sarimax_forecast(
    entity_level: str,
    entity_id: str,
    window_start: str | None = None,
    window_end: str | None = None,
) -> dict | None:
    conditions = [
        "model_name = ?",
        "entity_level = ?",
        "entity_id = ?",
        "status = 'completed'",
    ]
    params: list[Any] = [MODEL_NAME, entity_level, entity_id]
    if window_start:
        conditions.append("window_start = ?")
        params.append(window_start)
    if window_end:
        conditions.append("window_end = ?")
        params.append(window_end)

    try:
        with get_connection() as conn:
            row = conn.execute(
                f"""
                SELECT metrics_json, created_at
                FROM model_run_catalog
                WHERE {' AND '.join(conditions)}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
    except Exception:
        return None
    if not row:
        return None

    import json

    try:
        payload = json.loads(row[0])
    except Exception:
        return None
    payload["model_run_created_at"] = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
    return payload


def entity_column(entity_level: str) -> str:
    try:
        return ENTITY_COLUMNS[entity_level]
    except KeyError:
        allowed = ", ".join(sorted(ENTITY_COLUMNS))
        raise ValueError(f"Unsupported entity_level: {entity_level}. Allowed: {allowed}")


def _future_periods(last_period: str, horizon: int) -> list[str]:
    year = int(last_period[:4])
    month = int(last_period[5:7])
    periods = []
    for _ in range(horizon):
        month += 1
        if month == 13:
            year += 1
            month = 1
        periods.append(f"{year:04d}-{month:02d}")
    return periods
