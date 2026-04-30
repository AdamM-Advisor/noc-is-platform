import unittest

from backend.services.summary_lake_service import monthly_summary_partition_uri, monthly_summary_writer_sql


class SummaryLakeServiceTest(unittest.TestCase):
    def test_monthly_summary_partition_uri_uses_hive_layout(self):
        uri = monthly_summary_partition_uri("manual", 2026, 4)

        self.assertIn("/summaries/monthly/year=2026/month=04/source=manual", uri.replace("\\", "/"))

    def test_monthly_summary_writer_sql_groups_expected_dimensions(self):
        sql = monthly_summary_writer_sql(
            silver_uri="gs://bucket/nocis-lake/tickets/silver/year=2026/month=04/source=manual",
            target_partition_uri="gs://bucket/nocis-lake/summaries/monthly/year=2026/month=04/source=manual",
            source="manual",
            year=2026,
            month=4,
        )

        self.assertIn("calc_year_month AS year_month", sql)
        self.assertIn("COUNT(*) AS total_tickets", sql)
        self.assertIn("AVG(calc_restore_time_min)", sql)
        self.assertIn("WHERE calc_year_month = '2026-04'", sql)
        self.assertIn("summaries/monthly/year=2026/month=04/source=manual/part-", sql)


if __name__ == "__main__":
    unittest.main()
