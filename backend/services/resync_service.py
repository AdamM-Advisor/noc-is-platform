import time
import logging
from backend.database import get_write_connection, get_connection

logger = logging.getLogger(__name__)

CALC_HIERARCHY_COLS = ["calc_area_id", "calc_regional_id", "calc_nop_id", "calc_to_id"]


def resync_hierarchy(progress_callback=None):
    start_time = time.time()

    def update(phase, detail="", row=0, total=0):
        if progress_callback:
            progress_callback(phase, detail, row, total)

    update("loading", "Memuat data master...")

    with get_connection() as conn:
        total_tickets = conn.execute("SELECT COUNT(*) FROM noc_tickets").fetchone()[0]

    if total_tickets == 0:
        return {
            "total_tickets": 0,
            "resolved": {"area": 0, "regional": 0, "nop": 0, "to": 0},
            "remaining_orphans": {"area": 0, "regional": 0, "nop": 0, "to": 0},
            "duration_sec": round(time.time() - start_time, 2),
        }

    update("resolving", f"Menyiapkan lookup tables...", 0, total_tickets)

    with get_write_connection() as conn:
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _al AS
            SELECT match_key, FIRST(area_id) AS area_id FROM (
                SELECT TRIM(area_alias) AS match_key, area_id FROM master_area WHERE area_alias IS NOT NULL AND TRIM(area_alias) != ''
                UNION ALL
                SELECT TRIM(area_id), area_id FROM master_area WHERE area_id IS NOT NULL AND TRIM(area_id) != ''
            ) GROUP BY match_key
        """)

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _rl AS
            SELECT match_key, FIRST(regional_id) AS regional_id FROM (
                SELECT TRIM(regional_alias_ticket) AS match_key, regional_id FROM master_regional WHERE regional_alias_ticket IS NOT NULL AND TRIM(regional_alias_ticket) != ''
                UNION ALL
                SELECT TRIM(regional_name), regional_id FROM master_regional WHERE regional_name IS NOT NULL AND TRIM(regional_name) != ''
                UNION ALL
                SELECT TRIM(regional_id), regional_id FROM master_regional WHERE regional_id IS NOT NULL AND TRIM(regional_id) != ''
            ) GROUP BY match_key
        """)

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _nl AS
            SELECT match_key, FIRST(nop_id) AS nop_id FROM (
                SELECT TRIM(nop_alias_ticket) AS match_key, nop_id FROM master_nop WHERE nop_alias_ticket IS NOT NULL AND TRIM(nop_alias_ticket) != ''
                UNION ALL
                SELECT TRIM(nop_name), nop_id FROM master_nop WHERE nop_name IS NOT NULL AND TRIM(nop_name) != ''
                UNION ALL
                SELECT TRIM(nop_id), nop_id FROM master_nop WHERE nop_id IS NOT NULL AND TRIM(nop_id) != ''
            ) GROUP BY match_key
        """)

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _tl AS
            SELECT match_key, FIRST(to_id) AS to_id FROM (
                SELECT TRIM(to_alias_ticket) AS match_key, to_id FROM master_to WHERE to_alias_ticket IS NOT NULL AND TRIM(to_alias_ticket) != ''
                UNION ALL
                SELECT TRIM(to_name), to_id FROM master_to WHERE to_name IS NOT NULL AND TRIM(to_name) != ''
                UNION ALL
                SELECT TRIM(to_id), to_id FROM master_to WHERE to_id IS NOT NULL AND TRIM(to_id) != ''
            ) GROUP BY match_key
        """)

        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _sh AS
            SELECT ms.site_id, ms.to_id, mt.nop_id, mn.regional_id, mr.area_id
            FROM master_site ms
            LEFT JOIN master_to mt ON ms.to_id = mt.to_id
            LEFT JOIN master_nop mn ON mt.nop_id = mn.nop_id
            LEFT JOIN master_regional mr ON mn.regional_id = mr.regional_id
            WHERE ms.site_id IS NOT NULL
        """)

        update("resolving", f"Re-resolusi {total_tickets:,} tiket (single-pass CTAS)...", 0, total_tickets)

        conn.execute("""
            CREATE OR REPLACE TABLE _noc_tickets_resolved AS
            SELECT
                t.* EXCLUDE (calc_area_id, calc_regional_id, calc_nop_id, calc_to_id),
                COALESCE(al.area_id, sh.area_id) AS calc_area_id,
                COALESCE(rl.regional_id, sh.regional_id) AS calc_regional_id,
                COALESCE(nl.nop_id, sh.nop_id) AS calc_nop_id,
                COALESCE(tl.to_id, sh.to_id) AS calc_to_id
            FROM noc_tickets t
            LEFT JOIN _al al ON t.area = al.match_key
            LEFT JOIN _rl rl ON t.regional = rl.match_key
            LEFT JOIN _nl nl ON t.nop = nl.match_key
            LEFT JOIN _tl tl ON t.cluster_to = tl.match_key
            LEFT JOIN _sh sh ON t.site_id = sh.site_id
        """)

        update("swapping", "Mengganti tabel tiket...", 0, total_tickets)
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DROP TABLE IF EXISTS noc_tickets")
            conn.execute("ALTER TABLE _noc_tickets_resolved RENAME TO noc_tickets")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        conn.execute("DROP TABLE IF EXISTS _al")
        conn.execute("DROP TABLE IF EXISTS _rl")
        conn.execute("DROP TABLE IF EXISTS _nl")
        conn.execute("DROP TABLE IF EXISTS _tl")
        conn.execute("DROP TABLE IF EXISTS _sh")

        update("counting", "Menghitung statistik...", 0, total_tickets)
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN calc_area_id IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN calc_regional_id IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN calc_nop_id IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN calc_to_id IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN area IS NOT NULL AND calc_area_id IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN regional IS NOT NULL AND calc_regional_id IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN nop IS NOT NULL AND calc_nop_id IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN cluster_to IS NOT NULL AND calc_to_id IS NULL THEN 1 ELSE 0 END)
            FROM noc_tickets
        """).fetchone()

        resolved = {
            "area": int(stats[1] or 0),
            "regional": int(stats[2] or 0),
            "nop": int(stats[3] or 0),
            "to": int(stats[4] or 0),
        }
        remaining_orphans = {
            "area": int(stats[5] or 0),
            "regional": int(stats[6] or 0),
            "nop": int(stats[7] or 0),
            "to": int(stats[8] or 0),
        }

    update("orphans", "Memperbarui orphan log...", 0, total_tickets)
    _refresh_orphan_log()

    update("summaries", "Memperbarui ringkasan...", 0, total_tickets)
    _refresh_all_summaries()

    update("updating_imports", "Memperbarui orphan count di import logs...", 0, total_tickets)
    _update_import_orphan_counts()

    duration = round(time.time() - start_time, 2)
    update("completed", "Selesai", total_tickets, total_tickets)

    return {
        "total_tickets": total_tickets,
        "resolved": resolved,
        "remaining_orphans": remaining_orphans,
        "duration_sec": duration,
    }


def _refresh_orphan_log():
    try:
        with get_write_connection() as conn:
            conn.execute("DELETE FROM orphan_log")

            max_id = 0
            for level, col, id_col in [
                ("area", "area", "calc_area_id"),
                ("regional", "regional", "calc_regional_id"),
                ("nop", "nop", "calc_nop_id"),
                ("to", "cluster_to", "calc_to_id"),
            ]:
                rows = conn.execute(f"""
                    SELECT calc_source, TRIM(CAST({col} AS VARCHAR)) as val, COUNT(*) as cnt
                    FROM noc_tickets
                    WHERE {col} IS NOT NULL AND {id_col} IS NULL
                      AND TRIM(CAST({col} AS VARCHAR)) != ''
                    GROUP BY calc_source, TRIM(CAST({col} AS VARCHAR))
                """).fetchall()

                for source, value, count in rows:
                    max_id += 1
                    conn.execute("""
                        INSERT INTO orphan_log (id, source, level, value, resolved, ticket_count)
                        VALUES (?, ?, ?, ?, FALSE, ?)
                    """, [max_id, source, level, value, int(count)])
    except Exception as e:
        logger.warning(f"Failed to refresh orphan log: {e}")


def _refresh_all_summaries():
    try:
        with get_connection() as conn:
            periods = conn.execute("""
                SELECT DISTINCT calc_year_month FROM noc_tickets
                WHERE calc_year_month IS NOT NULL
                ORDER BY calc_year_month
            """).fetchall()

        period_list = [r[0] for r in periods]
        if period_list:
            from backend.services.summary_service import refresh_summaries
            refresh_summaries(period_list)
    except Exception as e:
        logger.warning(f"Failed to refresh summaries: {e}")


def _update_import_orphan_counts():
    try:
        with get_write_connection() as conn:
            imports = conn.execute("""
                SELECT id, file_type, period FROM import_logs
                WHERE status = 'completed' AND file_type != 'site_master'
            """).fetchall()

            for imp_id, file_type, period in imports:
                if not period:
                    continue
                orphan_count = conn.execute("""
                    SELECT
                        SUM(CASE WHEN area IS NOT NULL AND calc_area_id IS NULL THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN regional IS NOT NULL AND calc_regional_id IS NULL THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN nop IS NOT NULL AND calc_nop_id IS NULL THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN cluster_to IS NOT NULL AND calc_to_id IS NULL THEN 1 ELSE 0 END)
                    FROM noc_tickets
                    WHERE calc_source = ? AND calc_year_month = ?
                """, [file_type, period]).fetchone()[0]

                conn.execute("""
                    UPDATE import_logs SET orphan_count = ? WHERE id = ?
                """, [int(orphan_count or 0), imp_id])
    except Exception as e:
        logger.warning(f"Failed to update import orphan counts: {e}")
