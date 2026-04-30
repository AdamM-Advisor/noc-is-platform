import hashlib
import unittest
from pathlib import Path

from backend.services.ingestion_service import (
    bronze_output_uri,
    bronze_writer_sql,
    dataset_for_file_type,
    format_year_month,
    infer_filename,
    infer_year_month,
    normalize_storage_uri,
    normalize_token,
    partition_uri,
    sha256_file,
)


class IngestionServiceTest(unittest.TestCase):
    def test_normalize_storage_uri_accepts_gcs_uri(self):
        uri = normalize_storage_uri("gs://nocis-lake/raw/2026-01/file.parquet")

        self.assertEqual(uri, "gs://nocis-lake/raw/2026-01/file.parquet")

    def test_normalize_storage_uri_rejects_https_uri(self):
        with self.assertRaises(ValueError):
            normalize_storage_uri("https://example.com/file.parquet")

    def test_infer_filename_and_period(self):
        uri = "gs://bucket/tickets/bronze/year=2026/month=04/source=inap/part.parquet"

        self.assertEqual(infer_filename(uri), "part.parquet")
        self.assertEqual(infer_year_month(uri), (2026, 4))

    def test_partition_uri_uses_hive_style_segments(self):
        uri = partition_uri("tickets", "bronze", "swfm_realtime", 2026, 4)

        self.assertIn("/tickets/bronze/year=2026/month=04/source=swfm_realtime", uri.replace("\\", "/"))

    def test_dataset_mapping_and_period_format(self):
        self.assertEqual(dataset_for_file_type("ticket"), "tickets")
        self.assertEqual(dataset_for_file_type("site_master"), "site_master")
        self.assertEqual(format_year_month(2026, 4), "2026-04")

    def test_normalize_token_rejects_unsupported_value(self):
        with self.assertRaises(ValueError):
            normalize_token("bad source", {"inap"}, "source")

    def test_sha256_file(self):
        scratch = Path.cwd() / "temp_chunks" / "tests"
        scratch.mkdir(parents=True, exist_ok=True)
        path = scratch / "sample_sha256.parquet"
        try:
            path.write_bytes(b"nocis")
            self.assertEqual(sha256_file(path), hashlib.sha256(b"nocis").hexdigest())
        finally:
            if path.exists():
                path.unlink()

    def test_bronze_writer_sql_projects_normalized_columns(self):
        sql = bronze_writer_sql(
            source_uri="gs://bucket/uploads/tickets_2026-04.parquet",
            target_partition_uri="gs://bucket/lake/tickets/bronze/year=2026/month=04/source=inap",
            raw_columns=["Site ID", "Severity"],
            source="inap",
            year=2026,
            month=4,
        )

        self.assertIn('"Site ID" AS "site_id"', sql)
        self.assertIn("2026 AS year", sql)
        self.assertIn("4 AS month", sql)
        self.assertIn("'inap' AS source", sql)
        self.assertIn("FORMAT PARQUET", sql)
        self.assertIn("source=inap/part-", sql)

    def test_bronze_output_uri_is_deterministic_partition_file(self):
        partition = "gs://bucket/lake/tickets/bronze/year=2026/month=04/source=inap"
        source = "gs://bucket/uploads/tickets_2026-04.parquet"

        first = bronze_output_uri(partition, source)
        second = bronze_output_uri(partition, source)

        self.assertEqual(first, second)
        self.assertTrue(first.startswith(f"{partition}/part-"))
        self.assertTrue(first.endswith(".parquet"))

    def test_bronze_output_uri_allows_explicit_file_target(self):
        target = "gs://bucket/lake/tickets/bronze/year=2026/month=04/source=inap/custom.parquet"

        self.assertEqual(bronze_output_uri(target, "gs://bucket/uploads/file.parquet"), target)


if __name__ == "__main__":
    unittest.main()
