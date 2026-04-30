import unittest

from backend.services.silver_transform_service import (
    parquet_read_uri,
    silver_projection_sql,
    silver_writer_sql,
)


class SilverTransformServiceTest(unittest.TestCase):
    def test_parquet_read_uri_uses_partition_glob_for_directories(self):
        uri = parquet_read_uri("gs://bucket/lake/tickets/bronze/year=2026/month=04/source=manual")

        self.assertEqual(uri, "gs://bucket/lake/tickets/bronze/year=2026/month=04/source=manual/*.parquet")

    def test_silver_projection_adds_core_calculated_columns(self):
        projection = silver_projection_sql(
            ["site_id", "severity", "occured_time", "created_at", "cleared_time", "sla_status", "source"],
            source="manual",
            year=2026,
            month=4,
        )

        self.assertIn('try_cast("occured_time" AS TIMESTAMP) AS "occured_time"', projection)
        self.assertIn('CAST(NULL AS VARCHAR) AS "ticket_number_swfm"', projection)
        self.assertIn('"calc_restore_time_min"', projection)
        self.assertIn("strftime(try_cast(\"occured_time\" AS TIMESTAMP), '%Y-%m')", projection)
        self.assertIn('"calc_source"', projection)

    def test_silver_writer_sql_targets_deterministic_partition_file(self):
        sql = silver_writer_sql(
            bronze_uri="gs://bucket/lake/tickets/bronze/year=2026/month=04/source=manual",
            target_partition_uri="gs://bucket/lake/tickets/silver/year=2026/month=04/source=manual",
            available_columns=["site_id", "severity", "occured_time", "source", "year", "month"],
            source="manual",
            year=2026,
            month=4,
        )

        self.assertIn("read_parquet('gs://bucket/lake/tickets/bronze/year=2026/month=04/source=manual/*.parquet'", sql)
        self.assertIn("tickets/silver/year=2026/month=04/source=manual/part-", sql)
        self.assertIn("FORMAT PARQUET", sql)


if __name__ == "__main__":
    unittest.main()
