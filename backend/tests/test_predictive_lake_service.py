import unittest

from backend.services.predictive_lake_service import (
    backtest_feature_rows,
    month_range,
    period_end_date,
    predictive_feature_sql,
    score_feature_rows,
)


class PredictiveLakeServiceTest(unittest.TestCase):
    def test_predictive_feature_sql_uses_entity_and_window_filters(self):
        sql = predictive_feature_sql(
            silver_uri="gs://bucket/nocis-lake/tickets/silver",
            entity_level="site",
            window_start="2026-01",
            window_end="2026-04",
            source="manual",
        )

        self.assertIn("CAST(site_id AS VARCHAR) AS entity_id", sql)
        self.assertIn("calc_year_month >= '2026-01'", sql)
        self.assertIn("calc_year_month <= '2026-04'", sql)
        self.assertIn("COALESCE(calc_source, '') = 'manual'", sql)
        self.assertIn("duplicate_ticket_count", sql)

    def test_month_range_includes_boundaries(self):
        self.assertEqual(month_range("2025-11", "2026-02"), ["2025-11", "2025-12", "2026-01", "2026-02"])

    def test_score_feature_rows_returns_ranked_predictions(self):
        rows = [
            {
                "entity_id": "SITE_A",
                "calc_year_month": "2026-01",
                "monthly_ticket_count": 2,
                "avg_mttr_min": 120,
                "critical_major_count": 1,
                "escalated_count": 0,
                "duplicate_ticket_count": 0,
                "last_occured_date": "2026-01-20",
            },
            {
                "entity_id": "SITE_A",
                "calc_year_month": "2026-02",
                "monthly_ticket_count": 10,
                "avg_mttr_min": 300,
                "critical_major_count": 8,
                "escalated_count": 3,
                "duplicate_ticket_count": 2,
                "last_occured_date": "2026-02-25",
            },
            {
                "entity_id": "SITE_B",
                "calc_year_month": "2026-02",
                "monthly_ticket_count": 1,
                "avg_mttr_min": 50,
                "critical_major_count": 0,
                "escalated_count": 0,
                "duplicate_ticket_count": 0,
                "last_occured_date": "2026-02-01",
            },
        ]

        predictions = score_feature_rows(
            rows,
            entity_level="site",
            window_start="2026-01",
            window_end="2026-03",
            as_of_date="2026-03-01",
            horizon=2,
            limit=10,
        )

        self.assertEqual(predictions[0]["entity_id"], "SITE_A")
        self.assertEqual(predictions[0]["monthly_ticket_counts"], [2, 10, 0])
        self.assertEqual(len(predictions[0]["forecast"]["forecast"]), 2)
        self.assertGreater(predictions[0]["risk"]["total_score"], predictions[1]["risk"]["total_score"])

    def test_backtest_feature_rows_computes_confusion_metrics(self):
        rows = [
            _feature("SITE_A", "2026-01", 10, 8, "2026-01-20"),
            _feature("SITE_A", "2026-02", 12, 9, "2026-02-20"),
            _feature("SITE_A", "2026-03", 14, 10, "2026-03-20"),
            _feature("SITE_A", "2026-04", 2, 1, "2026-04-05"),
            _feature("SITE_B", "2026-01", 1, 0, "2026-01-05"),
            _feature("SITE_B", "2026-03", 1, 0, "2026-03-05"),
            _feature("SITE_B", "2026-04", 3, 1, "2026-04-06"),
            _feature("SITE_C", "2026-01", 12, 9, "2026-01-18"),
            _feature("SITE_C", "2026-02", 13, 10, "2026-02-18"),
            _feature("SITE_C", "2026-03", 14, 11, "2026-03-18"),
        ]

        result = backtest_feature_rows(
            rows,
            entity_level="site",
            train_start="2026-01",
            train_end="2026-03",
            outcome_start="2026-04",
            outcome_end="2026-04",
            risk_threshold=30,
            min_actual_tickets=2,
        )

        self.assertEqual(result["confusion"]["true_positive"], 1)
        self.assertEqual(result["confusion"]["false_positive"], 1)
        self.assertEqual(result["confusion"]["false_negative"], 1)
        self.assertEqual(result["metrics"]["precision"], 0.5)
        self.assertEqual(result["metrics"]["recall"], 0.5)

    def test_period_end_date_returns_last_day_of_month(self):
        self.assertEqual(period_end_date("2026-02").isoformat(), "2026-02-28")

def _feature(entity_id, period, count, critical_major, last_date):
    return {
        "entity_id": entity_id,
        "calc_year_month": period,
        "monthly_ticket_count": count,
        "avg_mttr_min": 120,
        "critical_major_count": critical_major,
        "escalated_count": 0,
        "duplicate_ticket_count": 0,
        "last_occured_date": last_date,
    }


if __name__ == "__main__":
    unittest.main()
