import unittest

from backend.services.operational_monitoring_service import (
    summarize_health,
    summarize_jobs,
    summarize_model_runs,
    summarize_partitions,
)


class OperationalMonitoringServiceTest(unittest.TestCase):
    def test_summarize_jobs_counts_status_and_success_rate(self):
        jobs = [
            {"job_type": "execute-bronze", "status": "completed"},
            {"job_type": "execute-silver", "status": "completed"},
            {"job_type": "execute-silver", "status": "failed", "error_message": "bad parquet"},
        ]

        summary = summarize_jobs(jobs)

        self.assertEqual(summary["status_counts"]["completed"], 2)
        self.assertEqual(summary["type_counts"]["execute-silver"], 2)
        self.assertEqual(summary["failed_count"], 1)
        self.assertEqual(summary["success_rate"], 0.6667)
        self.assertEqual(summary["latest_failed_job"]["error_message"], "bad parquet")

    def test_summarize_partitions_rolls_up_dataset_layer(self):
        partitions = [
            {"dataset": "tickets", "layer": "bronze", "row_count": 10, "size_bytes": 100, "year": 2026, "month": 4, "source": "manual"},
            {"dataset": "tickets", "layer": "silver", "row_count": 8, "size_bytes": 120, "year": 2026, "month": 4, "source": "manual"},
            {"dataset": "summary_monthly", "layer": "gold", "row_count": 2, "size_bytes": 50, "year": 2026, "month": 4, "source": "manual"},
        ]

        summary = summarize_partitions(partitions)

        self.assertEqual(summary["dataset_layer"]["tickets:bronze"]["row_count"], 10)
        self.assertEqual(summary["dataset_layer"]["tickets:silver"]["partition_count"], 1)
        self.assertEqual(summary["row_count"], 20)
        self.assertEqual(summary["covered_months"], ["2026-04"])

    def test_summarize_model_runs_extracts_risk_distribution_and_backtest_metrics(self):
        model_runs = [
            {
                "model_name": "statistical_failure_baseline_backtest",
                "status": "completed",
                "metrics": {"metrics": {"precision": 0.75, "recall": 0.6}},
            },
            {
                "model_name": "statistical_failure_baseline",
                "status": "completed",
                "metrics": {"risk": {"risk_level": "high"}},
            },
            {
                "model_name": "statistical_failure_baseline",
                "status": "completed",
                "metrics": {"risk": {"risk_level": "critical"}},
            },
        ]

        summary = summarize_model_runs(model_runs)

        self.assertEqual(summary["risk_distribution"], {"critical": 1, "high": 1})
        self.assertEqual(summary["backtest_count"], 1)
        self.assertEqual(summary["latest_backtest_metrics"]["recall"], 0.6)

    def test_summarize_health_warns_for_failed_jobs_and_empty_lake(self):
        snapshot = {
            "jobs": {"failed_count": 1},
            "lake": {"total_recent": 0},
            "models": {"latest_backtest_metrics": {"precision": 0.4, "recall": 0.45}},
        }

        health = summarize_health(snapshot)

        self.assertEqual(health["status"], "warning")
        self.assertEqual(health["issue_count"], 4)


if __name__ == "__main__":
    unittest.main()
