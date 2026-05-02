import asyncio
import unittest
import uuid
from pathlib import Path


class DataCoverageRouterTest(unittest.TestCase):
    def setUp(self):
        workspace = Path.cwd() / ".test_tmp" / "data_coverage_router" / uuid.uuid4().hex
        workspace.mkdir(parents=True)

        import backend.config as config
        import backend.database as database

        self.old_db_path = config.DB_PATH
        config.DB_PATH = str(workspace / "coverage.duckdb")
        database.DB_PATH = config.DB_PATH

        from backend.services.operational_catalog_service import initialize_operational_catalog
        from backend.services.schema_service import initialize_schema

        initialize_schema()
        initialize_operational_catalog()

    def tearDown(self):
        import backend.config as config
        import backend.database as database

        config.DB_PATH = self.old_db_path
        database.DB_PATH = self.old_db_path

    def test_coverage_uses_silver_parquet_partitions_when_ticket_table_empty(self):
        from backend.database import get_write_connection
        from backend.routers.data import get_data_coverage
        from backend.services.operational_catalog_service import register_partition

        register_partition(
            dataset="tickets",
            layer="silver",
            storage_uri="lake/tickets/silver/year=2025/month=02/source=swfm_event",
            year=2025,
            month=2,
            source="swfm_event",
            file_count=1,
            row_count=249188,
            size_bytes=1024,
            job_id="job_test",
        )

        with get_write_connection() as conn:
            conn.execute(
                """
                INSERT INTO import_logs (
                    id, filename, file_type, file_size_mb, period,
                    rows_total, rows_imported, rows_skipped, rows_error,
                    orphan_count, processing_time_sec, status
                ) VALUES (1, 'feb.xlsx', 'swfm_event', 74.8, '2025-02',
                    249188, 249188, 0, 0, 0, 120.0, 'completed')
                """
            )

        response = asyncio.run(get_data_coverage())
        cell = response["coverage"]["2025-02"]["swfm_event"]

        self.assertTrue(cell["exists"])
        self.assertEqual(cell["count"], 249188)
        self.assertEqual(cell["storage_layer"], "parquet")
        self.assertEqual(response["summary"]["total_tickets"], 249188)
        self.assertEqual(response["summary"]["months_covered"], 1)
        self.assertEqual(cell["imports"][0]["filename"], "feb.xlsx")


if __name__ == "__main__":
    unittest.main()
