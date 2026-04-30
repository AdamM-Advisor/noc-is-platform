import unittest
import uuid
from pathlib import Path

from backend.tests.golden_fixtures import (
    GOLDEN_EXPECTED,
    GOLDEN_TICKET_COLUMNS,
    write_golden_fixture_set,
)


class GoldenFixturePipelineTest(unittest.TestCase):
    def setUp(self):
        self.workspace = Path.cwd() / ".test_tmp" / "golden_fixture_pipeline" / uuid.uuid4().hex
        self.workspace.mkdir(parents=True)
        self._configure_local_runtime(self.workspace)

    def test_fixture_set_materializes_expected_parquet_files(self):
        fixtures = write_golden_fixture_set(self.workspace)

        self.assertEqual(len(fixtures), GOLDEN_EXPECTED["files"])
        self.assertEqual(sum(item["row_count"] for item in fixtures), GOLDEN_EXPECTED["row_count"])
        self.assertTrue(all(item["path"].is_file() for item in fixtures))

    def test_swfm_realtime_january_matches_gold_summary_expectations(self):
        import duckdb

        from backend.services.ingestion_service import create_ingestion_plan, execute_bronze_write
        from backend.services.silver_transform_service import execute_silver_write
        from backend.services.summary_lake_service import (
            execute_monthly_summary_refresh,
            monthly_summary_partition_uri,
        )

        fixtures = write_golden_fixture_set(self.workspace)
        fixture = next(
            item
            for item in fixtures
            if item["source"] == "swfm_realtime" and item["year"] == 2026 and item["month"] == 1
        )

        plan = create_ingestion_plan(
            storage_uri=fixture["storage_uri"],
            file_type="ticket",
            source=fixture["source"],
            year=fixture["year"],
            month=fixture["month"],
        )
        bronze = execute_bronze_write(
            source_uri=fixture["storage_uri"],
            target_partition_uri=plan.bronze_partition_uri,
            raw_columns=GOLDEN_TICKET_COLUMNS,
            source=fixture["source"],
            year=fixture["year"],
            month=fixture["month"],
            dataset=plan.dataset,
            job_id=plan.job["job_id"],
        )
        silver = execute_silver_write(
            bronze_uri=bronze["partition_uri"],
            target_partition_uri=plan.silver_partition_uri,
            source=fixture["source"],
            year=fixture["year"],
            month=fixture["month"],
            dataset=plan.dataset,
        )
        gold = execute_monthly_summary_refresh(
            silver_uri=silver["partition_uri"],
            target_partition_uri=monthly_summary_partition_uri(fixture["source"], fixture["year"], fixture["month"]),
            source=fixture["source"],
            year=fixture["year"],
            month=fixture["month"],
        )

        expected = GOLDEN_EXPECTED["swfm_realtime_2026_01"]
        conn = duckdb.connect(database=":memory:")
        try:
            aggregate = conn.execute(
                f"""
                SELECT
                    SUM(total_tickets),
                    SUM(total_sla_met),
                    SUM(count_critical),
                    SUM(count_major),
                    SUM(count_minor)
                FROM read_parquet('{_duckdb_path(gold["output_uri"])}')
                """
            ).fetchone()
            duplicate_count = conn.execute(
                f"""
                SELECT COUNT(*) - COUNT(DISTINCT ticket_number_inap)
                FROM read_parquet('{_duckdb_path(silver["output_uri"])}')
                """
            ).fetchone()[0]
        finally:
            conn.close()

        self.assertEqual(gold["row_count"], 3)
        self.assertEqual(int(aggregate[0]), expected["total_tickets"])
        self.assertEqual(int(aggregate[1]), expected["total_sla_met"])
        self.assertEqual(int(aggregate[2]), expected["count_critical"])
        self.assertEqual(int(aggregate[3]), expected["count_major"])
        self.assertEqual(int(aggregate[4]), expected["count_minor"])
        self.assertEqual(int(duplicate_count), expected["duplicate_ticket_numbers"])

    def _configure_local_runtime(self, workspace: Path):
        import backend.config as config
        import backend.services.ingestion_service as ingestion
        import backend.services.parquet_lake_service as lake
        import backend.services.summary_lake_service as summary

        data_dir = workspace / "data"
        lake_root = workspace / "lake"
        data_dir.mkdir(parents=True, exist_ok=True)
        lake_root.mkdir(parents=True, exist_ok=True)

        config.DB_PATH = str(data_dir / "catalog.duckdb")
        config.DATA_DIR = str(data_dir)
        config.BACKUP_DIR = str(data_dir / "backups")
        lake.LAKE_ROOT = str(lake_root)
        lake.TICKET_DATASET = str(lake_root / "tickets")
        ingestion.LAKE_ROOT = str(lake_root)
        summary.LAKE_ROOT = str(lake_root)

        try:
            import backend.database as database

            database.DB_PATH = config.DB_PATH
        except ImportError:
            pass


def _duckdb_path(path: str) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


if __name__ == "__main__":
    unittest.main()
