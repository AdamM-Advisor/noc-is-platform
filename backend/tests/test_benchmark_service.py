import unittest
import uuid
from pathlib import Path

from backend.services.benchmark_service import (
    LocalBenchmarkConfig,
    distribute_rows,
    month_sequence,
    run_local_benchmark,
)


class BenchmarkServiceTest(unittest.TestCase):
    def setUp(self):
        self.workspace = Path.cwd() / ".test_tmp" / "benchmark_service" / uuid.uuid4().hex
        self.workspace.mkdir(parents=True)

    def test_distribute_rows_keeps_total(self):
        self.assertEqual(distribute_rows(10, 3), [4, 3, 3])

    def test_month_sequence_rolls_year(self):
        self.assertEqual(month_sequence(2026, 11, 4), [(2026, 11), (2026, 12), (2027, 1), (2027, 2)])

    def test_run_local_benchmark_smoke(self):
        result = run_local_benchmark(
            LocalBenchmarkConfig(
                output_dir=str(self.workspace),
                total_rows=600,
                months=2,
                source="swfm_realtime",
                site_count=50,
                run_predictive=True,
                run_backtest=True,
                persist_model_runs=False,
            )
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["totals"]["rows"], 600)
        self.assertEqual(len(result["partitions"]), 2)
        self.assertGreater(result["totals"]["summary_rows"], 0)
        self.assertGreater(result["timings_seconds"]["bronze_write"], 0)
        self.assertIsNotNone(result["predictive"])
        self.assertIsNotNone(result["backtest"])
        self.assertTrue(Path(result["result_path"]).is_file())


if __name__ == "__main__":
    unittest.main()
