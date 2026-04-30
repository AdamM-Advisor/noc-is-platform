from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from statistics import median
from typing import Iterable, Sequence


@dataclass(frozen=True)
class TimeSeriesPoint:
    period: str
    value: float


@dataclass(frozen=True)
class ForecastResult:
    method: str
    horizon: int
    forecast: list[float]
    baseline: float
    trend_slope: float
    confidence: str


@dataclass(frozen=True)
class AnomalyPoint:
    period: str
    value: float
    score: float
    direction: str
    severity: str


@dataclass(frozen=True)
class FailureRiskFeatures:
    frequency_score: float
    recency_score: float
    severity_score: float
    repeat_score: float
    mttr_trend_score: float
    escalation_score: float
    anomaly_score: float


@dataclass(frozen=True)
class FailureRiskResult:
    total_score: float
    risk_level: str
    top_factors: list[str]
    features: FailureRiskFeatures


def weighted_moving_average(values: Sequence[float], window: int = 6) -> float:
    clean = _clean_numbers(values)
    if not clean:
        return 0.0
    tail = clean[-window:]
    weights = list(range(1, len(tail) + 1))
    return sum(v * w for v, w in zip(tail, weights)) / sum(weights)


def linear_slope(values: Sequence[float]) -> float:
    clean = _clean_numbers(values)
    n = len(clean)
    if n < 2:
        return 0.0
    x_mean = (n - 1) / 2
    y_mean = sum(clean) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(clean))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0.0
    return numerator / denominator


def exponential_smoothing_forecast(
    values: Sequence[float],
    horizon: int = 3,
    alpha: float = 0.35,
) -> ForecastResult:
    clean = _clean_numbers(values)
    if not clean:
        return ForecastResult("exponential_smoothing", horizon, [0.0] * horizon, 0.0, 0.0, "low")

    smoothed = clean[0]
    for value in clean[1:]:
        smoothed = alpha * value + (1 - alpha) * smoothed

    slope = linear_slope(clean[-min(len(clean), 6):])
    forecast = [max(0.0, smoothed + slope * step) for step in range(1, horizon + 1)]
    confidence = "high" if len(clean) >= 12 else ("medium" if len(clean) >= 6 else "low")
    return ForecastResult(
        method="exponential_smoothing",
        horizon=horizon,
        forecast=[round(v, 2) for v in forecast],
        baseline=round(smoothed, 2),
        trend_slope=round(slope, 4),
        confidence=confidence,
    )


def robust_mad_anomalies(points: Iterable[TimeSeriesPoint], threshold: float = 3.5) -> list[AnomalyPoint]:
    series = list(points)
    values = _clean_numbers([p.value for p in series])
    if len(values) < 4:
        return []

    med = median(values)
    deviations = [abs(v - med) for v in values]
    mad = median(deviations)
    if mad == 0:
        return []

    anomalies: list[AnomalyPoint] = []
    for point in series:
        score = 0.6745 * (point.value - med) / mad
        abs_score = abs(score)
        if abs_score >= threshold:
            anomalies.append(
                AnomalyPoint(
                    period=point.period,
                    value=point.value,
                    score=round(score, 3),
                    direction="up" if score > 0 else "down",
                    severity="critical" if abs_score >= threshold * 1.5 else "warning",
                )
            )
    return anomalies


def event_gap_days(event_dates: Sequence[str | date | datetime]) -> dict:
    parsed = sorted(_parse_date(d) for d in event_dates if _parse_date(d) is not None)
    if len(parsed) < 2:
        return {"avg_gap_days": None, "median_gap_days": None, "min_gap_days": None, "max_gap_days": None}

    gaps = [(parsed[i] - parsed[i - 1]).days for i in range(1, len(parsed))]
    return {
        "avg_gap_days": round(sum(gaps) / len(gaps), 2),
        "median_gap_days": median(gaps),
        "min_gap_days": min(gaps),
        "max_gap_days": max(gaps),
    }


def score_failure_risk(
    monthly_ticket_counts: Sequence[float],
    days_since_last_ticket: int | None,
    critical_major_pct: float,
    repeat_pct: float,
    mttr_values: Sequence[float],
    escalation_pct: float,
    anomaly_count: int = 0,
) -> FailureRiskResult:
    frequency_score = _clamp(weighted_moving_average(monthly_ticket_counts, 6) * 4, 0, 100)
    recency_score = _score_recency(days_since_last_ticket)
    severity_score = _clamp(critical_major_pct, 0, 100)
    repeat_score = _clamp(repeat_pct * 1.2, 0, 100)
    mttr_trend_score = _score_positive_slope(mttr_values)
    escalation_score = _clamp(escalation_pct * 2, 0, 100)
    anomaly_score = _clamp(anomaly_count * 20, 0, 100)

    features = FailureRiskFeatures(
        frequency_score=round(frequency_score, 2),
        recency_score=round(recency_score, 2),
        severity_score=round(severity_score, 2),
        repeat_score=round(repeat_score, 2),
        mttr_trend_score=round(mttr_trend_score, 2),
        escalation_score=round(escalation_score, 2),
        anomaly_score=round(anomaly_score, 2),
    )

    weights = {
        "frequency_score": 0.24,
        "recency_score": 0.16,
        "severity_score": 0.16,
        "repeat_score": 0.12,
        "mttr_trend_score": 0.14,
        "escalation_score": 0.10,
        "anomaly_score": 0.08,
    }
    total = sum(getattr(features, key) * weight for key, weight in weights.items())
    top_factors = _top_factors(features)
    return FailureRiskResult(
        total_score=round(total, 2),
        risk_level=_risk_level(total),
        top_factors=top_factors,
        features=features,
    )


def _clean_numbers(values: Sequence[float]) -> list[float]:
    clean = []
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            clean.append(number)
    return clean


def _score_recency(days_since_last_ticket: int | None) -> float:
    if days_since_last_ticket is None:
        return 0.0
    if days_since_last_ticket <= 3:
        return 100.0
    if days_since_last_ticket <= 7:
        return 80.0
    if days_since_last_ticket <= 14:
        return 60.0
    if days_since_last_ticket <= 30:
        return 35.0
    return 10.0


def _score_positive_slope(values: Sequence[float]) -> float:
    clean = _clean_numbers(values)
    if len(clean) < 3:
        return 0.0
    slope = linear_slope(clean[-min(6, len(clean)):])
    baseline = max(abs(clean[0]), 1.0)
    pct_slope = slope / baseline * 100
    return _clamp(pct_slope * 8, 0, 100)


def _top_factors(features: FailureRiskFeatures, limit: int = 3) -> list[str]:
    ranked = sorted(
        features.__dict__.items(),
        key=lambda item: item[1],
        reverse=True,
    )
    return [name for name, score in ranked[:limit] if score > 0]


def _risk_level(score: float) -> str:
    if score >= 75:
        return "critical"
    if score >= 55:
        return "high"
    if score >= 35:
        return "medium"
    return "low"


def _parse_date(value: str | date | datetime) -> date | None:
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


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
