import unittest
import uuid
from pathlib import Path


class RawPipelineServiceTest(unittest.TestCase):
    def setUp(self):
        self.workspace = Path.cwd() / ".test_tmp" / "raw_pipeline_service" / uuid.uuid4().hex
        self.workspace.mkdir(parents=True)
        self._configure_local_runtime(self.workspace)

    def test_process_raw_csv_materializes_parquet_and_summary_cache(self):
        from backend.services.raw_pipeline_service import process_raw_ticket_file

        raw_file = self.workspace / "tickets_2026-01.csv"
        raw_file.write_text(
            "\n".join(
                [
                    "site_id,severity,occured_time,cleared_time,sla_status,area,regional,nop,cluster_to,ticket_number_inap",
                    "SITE_A,Critical,2026-01-05 10:00:00,2026-01-05 11:00:00,IN SLA,AREA1,REG1,NOP1,TO1,T1",
                    "SITE_A,Major,2026-01-06 10:00:00,2026-01-06 13:00:00,OUT SLA,AREA1,REG1,NOP1,TO1,T2",
                ]
            ),
            encoding="utf-8",
        )

        result = process_raw_ticket_file(str(raw_file), source="manual")

        self.assertEqual(result["pipeline"], "raw_to_parquet")
        self.assertEqual(result["period"], "2026-01")
        self.assertEqual(result["total"], 2)
        self.assertTrue(Path(result["raw_uri"]).is_file())
        self.assertTrue(Path(result["source_parquet_uri"]).is_file())
        self.assertTrue(Path(result["bronze"]["output_uri"]).is_file())
        self.assertTrue(Path(result["silver"]["output_uri"]).is_file())
        self.assertTrue(Path(result["gold"]["output_uri"]).is_file())
        self.assertEqual(result["summary_cache"]["source_rows"], 2)

        import duckdb
        import backend.config as config

        conn = duckdb.connect(config.DB_PATH)
        try:
            row = conn.execute(
                """
                SELECT total_tickets, count_critical, count_major
                FROM summary_monthly
                WHERE year_month = '2026-01'
                  AND site_id = 'SITE_A'
                  AND severity IS NULL
                  AND type_ticket IS NULL
                  AND fault_level IS NULL
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(row, (2, 1, 1))

    def _configure_local_runtime(self, workspace: Path):
        import backend.config as config
        import backend.database as database
        import backend.services.ingestion_service as ingestion
        import backend.services.parquet_lake_service as lake
        import backend.services.summary_lake_service as summary

        data_dir = workspace / "data"
        raw_dir = workspace / "raw"
        lake_root = workspace / "lake"
        data_dir.mkdir(parents=True, exist_ok=True)
        raw_dir.mkdir(parents=True, exist_ok=True)
        lake_root.mkdir(parents=True, exist_ok=True)

        config.DB_PATH = str(data_dir / "catalog.duckdb")
        config.DATA_DIR = str(data_dir)
        config.RAW_DIR = str(raw_dir)
        config.BACKUP_DIR = str(data_dir / "backups")
        database.DB_PATH = config.DB_PATH
        lake.LAKE_ROOT = str(lake_root)
        lake.TICKET_DATASET = str(lake_root / "tickets")
        ingestion.LAKE_ROOT = str(lake_root)
        summary.LAKE_ROOT = str(lake_root)


if __name__ == "__main__":
    unittest.main()
