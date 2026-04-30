import unittest

from backend.services.statistical_failure_service import (
    TimeSeriesPoint,
    event_gap_days,
    exponential_smoothing_forecast,
    robust_mad_anomalies,
    score_failure_risk,
    weighted_moving_average,
)


class StatisticalFailureServiceTest(unittest.TestCase):
    def test_weighted_moving_average_weights_recent_values_higher(self):
        self.assertAlmostEqual(weighted_moving_average([10, 20, 30], window=3), 23.3333333333)

    def test_exponential_smoothing_forecast_returns_requested_horizon(self):
        result = exponential_smoothing_forecast([10, 12, 13, 18, 21, 24], horizon=2)

        self.assertEqual(result.method, "exponential_smoothing")
        self.assertEqual(len(result.forecast), 2)
        self.assertEqual(result.confidence, "medium")
        self.assertGreater(result.forecast[-1], result.forecast[0])

    def test_robust_mad_anomalies_flags_extreme_volume(self):
        points = [
            TimeSeriesPoint("2026-01", 10),
            TimeSeriesPoint("2026-02", 11),
            TimeSeriesPoint("2026-03", 9),
            TimeSeriesPoint("2026-04", 10),
            TimeSeriesPoint("2026-05", 80),
        ]

        anomalies = robust_mad_anomalies(points, threshold=3.5)

        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0].period, "2026-05")
        self.assertEqual(anomalies[0].direction, "up")

    def test_event_gap_days_summarizes_failure_intervals(self):
        result = event_gap_days(["2026-01-01", "2026-01-04", "2026-01-10"])

        self.assertEqual(result["min_gap_days"], 3)
        self.assertEqual(result["max_gap_days"], 6)
        self.assertEqual(result["median_gap_days"], 4.5)

    def test_score_failure_risk_returns_explainable_result(self):
        result = score_failure_risk(
            monthly_ticket_counts=[5, 8, 12, 15, 18, 22],
            days_since_last_ticket=4,
            critical_major_pct=35,
            repeat_pct=18,
            mttr_values=[300, 330, 420, 480],
            escalation_pct=12,
            anomaly_count=2,
        )

        self.assertEqual(result.risk_level, "high")
        self.assertIn("mttr_trend_score", result.top_factors)
        self.assertGreater(result.total_score, 55)


if __name__ == "__main__":
    unittest.main()
