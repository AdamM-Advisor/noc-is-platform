from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any


def build_operational_snapshot(
    job_limit: int = 100,
    file_limit: int = 100,
    partition_limit: int = 250,
    model_run_limit: int = 100,
) -> dict:
    from backend.services.operational_catalog_service import (
        list_files,
        list_jobs,
        list_model_runs,
        list_partitions,
    )

    jobs = list_jobs(limit=job_limit)
    files = list_files(limit=file_limit)
    partitions = list_partitions(limit=partition_limit)
    model_runs = list_model_runs(limit=model_run_limit)

    snapshot = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "jobs": summarize_jobs(jobs),
        "files": summarize_files(files),
        "lake": summarize_partitions(partitions),
        "models": summarize_model_runs(model_runs),
        "recent": {
            "jobs": jobs[:20],
            "files": files[:20],
            "partitions": partitions[:20],
            "model_runs": model_runs[:20],
        },
    }
    snapshot["health"] = summarize_health(snapshot)
    return snapshot


def summarize_jobs(jobs: list[dict]) -> dict:
    status_counts = count_by(jobs, "status")
    type_counts = count_by(jobs, "job_type")
    failed = [job for job in jobs if job.get("status") == "failed"]
    running = [job for job in jobs if job.get("status") == "running"]
    queued = [job for job in jobs if job.get("status") == "queued"]
    completed = [job for job in jobs if job.get("status") == "completed"]

    return {
        "total_recent": len(jobs),
        "status_counts": status_counts,
        "type_counts": type_counts,
        "running_count": len(running),
        "queued_count": len(queued),
        "failed_count": len(failed),
        "completed_count": len(completed),
        "success_rate": safe_ratio(len(completed), len(completed) + len(failed)),
        "latest_failed_job": failed[0] if failed else None,
        "latest_running_job": running[0] if running else None,
    }


def summarize_files(files: list[dict]) -> dict:
    row_count = sum_int(files, "row_count")
    size_bytes = sum_int(files, "size_bytes")
    return {
        "total_recent": len(files),
        "status_counts": count_by(files, "status"),
        "file_type_counts": count_by(files, "file_type"),
        "source_counts": count_by(files, "source"),
        "row_count": row_count,
        "size_bytes": size_bytes,
        "latest_file": files[0] if files else None,
    }


def summarize_partitions(partitions: list[dict]) -> dict:
    by_dataset_layer: dict[str, dict[str, int]] = defaultdict(lambda: {"partition_count": 0, "row_count": 0, "size_bytes": 0})
    months = set()
    sources = set()

    for partition in partitions:
        dataset = partition.get("dataset") or "unknown"
        layer = partition.get("layer") or "unknown"
        key = f"{dataset}:{layer}"
        by_dataset_layer[key]["partition_count"] += 1
        by_dataset_layer[key]["row_count"] += int(partition.get("row_count") or 0)
        by_dataset_layer[key]["size_bytes"] += int(partition.get("size_bytes") or 0)
        if partition.get("year") and partition.get("month"):
            months.add(f"{int(partition['year']):04d}-{int(partition['month']):02d}")
        if partition.get("source"):
            sources.add(partition["source"])

    return {
        "total_recent": len(partitions),
        "dataset_layer": dict(sorted(by_dataset_layer.items())),
        "layer_counts": count_by(partitions, "layer"),
        "source_counts": count_by(partitions, "source"),
        "row_count": sum_int(partitions, "row_count"),
        "size_bytes": sum_int(partitions, "size_bytes"),
        "covered_months": sorted(months),
        "covered_sources": sorted(sources),
        "latest_partition": partitions[0] if partitions else None,
    }


def summarize_model_runs(model_runs: list[dict]) -> dict:
    risk_runs = [run for run in model_runs if run.get("model_name") == "statistical_failure_baseline"]
    backtests = [run for run in model_runs if str(run.get("model_name", "")).endswith("_backtest")]
    risk_distribution = Counter()
    latest_backtest = backtests[0] if backtests else None

    for run in risk_runs:
        risk_level = deep_get(run, ["metrics", "risk", "risk_level"])
        if risk_level:
            risk_distribution[str(risk_level)] += 1

    latest_backtest_metrics = deep_get(latest_backtest or {}, ["metrics", "metrics"]) or {}
    return {
        "total_recent": len(model_runs),
        "model_counts": count_by(model_runs, "model_name"),
        "status_counts": count_by(model_runs, "status"),
        "risk_run_count": len(risk_runs),
        "risk_distribution": dict(sorted(risk_distribution.items())),
        "backtest_count": len(backtests),
        "latest_prediction_run": risk_runs[0] if risk_runs else None,
        "latest_backtest_run": latest_backtest,
        "latest_backtest_metrics": latest_backtest_metrics,
    }


def summarize_health(snapshot: dict) -> dict:
    issues = []
    jobs = snapshot.get("jobs", {})
    lake = snapshot.get("lake", {})
    models = snapshot.get("models", {})

    if jobs.get("failed_count", 0) > 0:
        issues.append(
            {
                "severity": "warning",
                "area": "jobs",
                "message": f"{jobs['failed_count']} recent job(s) failed",
            }
        )
    if lake.get("total_recent", 0) == 0:
        issues.append(
            {
                "severity": "warning",
                "area": "lake",
                "message": "No lake partitions registered yet",
            }
        )

    backtest_metrics = models.get("latest_backtest_metrics") or {}
    recall = backtest_metrics.get("recall")
    precision = backtest_metrics.get("precision")
    if isinstance(recall, (int, float)) and recall < 0.5:
        issues.append(
            {
                "severity": "warning",
                "area": "models",
                "message": f"Latest backtest recall is low ({recall})",
            }
        )
    if isinstance(precision, (int, float)) and precision < 0.5:
        issues.append(
            {
                "severity": "warning",
                "area": "models",
                "message": f"Latest backtest precision is low ({precision})",
            }
        )

    if any(issue["severity"] == "critical" for issue in issues):
        status = "critical"
    elif issues:
        status = "warning"
    else:
        status = "ok"

    return {"status": status, "issue_count": len(issues), "issues": issues}


def count_by(rows: list[dict], key: str) -> dict[str, int]:
    counts = Counter(str(row.get(key) or "unknown") for row in rows)
    return dict(sorted(counts.items()))


def sum_int(rows: list[dict], key: str) -> int:
    total = 0
    for row in rows:
        try:
            total += int(row.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return total


def safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def deep_get(value: dict, path: list[str]) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current
