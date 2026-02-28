import os
import time
import logging
import pandas as pd
from backend.database import get_write_connection, get_connection
from backend.services.header_normalizer import normalize_headers, EXPECTED_TICKET_COLUMNS
from backend.services.backup_service import create_backup

logger = logging.getLogger(__name__)

NOC_TICKET_COLUMNS = [
    "ticket_number_inap", "ticket_number_swfm", "ticket_creation", "ticket_creator",
    "severity", "type_ticket", "fault_level", "impact", "ne_class", "incident_priority",
    "site_id", "site_name", "site_class", "cluster_to", "sub_cluster", "nop", "regional",
    "area", "hub", "occured_time", "created_at", "cleared_time", "submitted_time",
    "take_over_date", "check_in_at", "dispatch_date", "follow_up_at", "closed_at",
    "site_cleared_on", "rca_validate_at", "duration_ticket", "age_ticket", "rh_start",
    "rh_start_time", "rh_stop", "rh_stop_time", "ticket_inap_status", "ticket_swfm_status",
    "sla_status", "holding_status", "pic_take_over_ticket", "is_escalate", "escalate_to",
    "is_auto_resolved", "assignee_group", "dispatch_by", "is_force_dispatch",
    "is_excluded_in_kpi", "rc_owner", "rc_category", "rc_1", "rc_2", "inap_rc_1",
    "inap_rc_2", "resolution_action", "inap_resolution_action", "rc_owner_engineer",
    "rc_category_engineer", "rc_1_engineer", "rc_2_engineer", "rca_validated",
    "rca_validated_by", "summary", "description", "note", "nossa_no", "rank",
    "pic_email", "rat", "parking_status", "parking_start", "parking_end", "yearmonth",
]

CALC_COLUMNS = [
    "calc_response_time_min", "calc_repair_time_min", "calc_restore_time_min",
    "calc_detection_time_min", "calc_sla_duration_min", "calc_sla_target_min",
    "calc_is_sla_met", "calc_hour_of_day", "calc_day_of_week", "calc_week_of_month",
    "calc_month", "calc_year", "calc_year_month", "calc_year_week",
    "calc_area_id", "calc_regional_id", "calc_nop_id", "calc_to_id", "calc_source",
]

TIMESTAMP_COLUMNS = [
    "occured_time", "created_at", "cleared_time", "submitted_time",
    "take_over_date", "check_in_at", "dispatch_date", "follow_up_at",
    "closed_at", "site_cleared_on", "rca_validate_at",
]


def process_ticket_file(file_path: str, file_type: str, progress_callback=None) -> dict:
    start_time = time.time()

    def update_progress(phase, detail="", row=0, total=0):
        if progress_callback:
            progress_callback(phase, detail, row, total)

    try:
        create_backup()
    except Exception as e:
        logger.warning(f"Backup before import failed: {e}")

    update_progress("reading", "Membaca file...")

    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        df = pd.read_excel(file_path, engine='openpyxl')
    elif ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    total_rows = len(df)
    update_progress("normalizing", "Menormalisasi header...", 0, total_rows)

    header_format = "snake_case" if file_type == "swfm_realtime" else "title_case"
    norm_result = normalize_headers(df, header_format)
    df = norm_result["df"]

    required = ["site_id", "severity"]
    missing_required = [c for c in required if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    for col in NOC_TICKET_COLUMNS:
        if col not in df.columns:
            df[col] = None

    for col in CALC_COLUMNS:
        df[col] = None

    df["calc_source"] = file_type

    for col in TIMESTAMP_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    update_progress("calculating", "Menghitung kolom kalkulasi...", 0, total_rows)

    if "occured_time" in df.columns:
        ot = df["occured_time"]
        df["calc_hour_of_day"] = ot.dt.hour
        df["calc_day_of_week"] = ot.dt.dayofweek
        df["calc_week_of_month"] = ((ot.dt.day - 1) // 7 + 1).astype("Int64")
        df["calc_month"] = ot.dt.month
        df["calc_year"] = ot.dt.year
        df["calc_year_month"] = ot.dt.strftime("%Y-%m")
        df["calc_year_week"] = ot.dt.strftime("%Y-W%W")

    if "take_over_date" in df.columns and "created_at" in df.columns:
        diff = (df["take_over_date"] - df["created_at"]).dt.total_seconds() / 60
        df["calc_response_time_min"] = diff.clip(lower=0)

    if "cleared_time" in df.columns and "take_over_date" in df.columns:
        diff = (df["cleared_time"] - df["take_over_date"]).dt.total_seconds() / 60
        df["calc_repair_time_min"] = diff.clip(lower=0)

    if "cleared_time" in df.columns and "occured_time" in df.columns:
        diff = (df["cleared_time"] - df["occured_time"]).dt.total_seconds() / 60
        df["calc_restore_time_min"] = diff.clip(lower=0)

    if "created_at" in df.columns and "occured_time" in df.columns:
        diff = (df["created_at"] - df["occured_time"]).dt.total_seconds() / 60
        df["calc_detection_time_min"] = diff.clip(lower=0)

    df["calc_sla_duration_min"] = df["calc_restore_time_min"]

    if "sla_status" in df.columns:
        df["calc_is_sla_met"] = df["sla_status"].astype(str).str.strip().str.upper() == "IN SLA"
    else:
        df["calc_is_sla_met"] = None

    update_progress("resolving", "Meresolusi hierarki...", 0, total_rows)

    area_map = _load_area_map()
    regional_map = _load_regional_ticket_map()
    nop_map = _load_nop_ticket_map()
    to_map = _load_to_ticket_map()

    orphans = {"area": 0, "regional": 0, "nop": 0, "to": 0}
    orphan_details = []

    if "area" in df.columns:
        df["calc_area_id"] = df["area"].map(lambda x: area_map.get(str(x).strip(), None) if pd.notna(x) else None)
        unresolved = df[(df["area"].notna()) & (df["calc_area_id"].isna())]["area"].value_counts()
        for val, cnt in unresolved.items():
            orphans["area"] += cnt
            orphan_details.append(("area", str(val).strip(), cnt))

    if "regional" in df.columns:
        df["calc_regional_id"] = df["regional"].map(lambda x: regional_map.get(str(x).strip(), None) if pd.notna(x) else None)
        unresolved = df[(df["regional"].notna()) & (df["calc_regional_id"].isna())]["regional"].value_counts()
        for val, cnt in unresolved.items():
            orphans["regional"] += cnt
            orphan_details.append(("regional", str(val).strip(), cnt))

    if "nop" in df.columns:
        df["calc_nop_id"] = df["nop"].map(lambda x: nop_map.get(str(x).strip(), None) if pd.notna(x) else None)
        unresolved = df[(df["nop"].notna()) & (df["calc_nop_id"].isna())]["nop"].value_counts()
        for val, cnt in unresolved.items():
            orphans["nop"] += cnt
            orphan_details.append(("nop", str(val).strip(), cnt))

    if "cluster_to" in df.columns:
        df["calc_to_id"] = df["cluster_to"].map(lambda x: to_map.get(str(x).strip(), None) if pd.notna(x) else None)
        unresolved = df[(df["cluster_to"].notna()) & (df["calc_to_id"].isna())]["cluster_to"].value_counts()
        for val, cnt in unresolved.items():
            orphans["to"] += cnt
            orphan_details.append(("to", str(val).strip(), cnt))

    if orphan_details:
        _log_orphans(file_type, orphan_details)

    _auto_map_area_from_tickets(df)

    _load_sla_targets(df)

    update_progress("deduplicating", "Mendeteksi duplikat...", 0, total_rows)

    all_cols = NOC_TICKET_COLUMNS + CALC_COLUMNS
    for col in all_cols:
        if col not in df.columns:
            df[col] = None

    df_final = df[all_cols].copy()

    for ts_col in TIMESTAMP_COLUMNS:
        if ts_col in df_final.columns:
            df_final[ts_col] = df_final[ts_col].where(df_final[ts_col].notna(), None)

    update_progress("inserting", "Menyimpan ke database...", 0, total_rows)

    imported = 0
    skipped = 0

    with get_write_connection() as conn:
        try:
            conn.execute("BEGIN TRANSACTION")
            conn.execute("CREATE OR REPLACE TEMP TABLE temp_import AS SELECT * FROM df_final WHERE 1=0")
            conn.execute("INSERT INTO temp_import SELECT * FROM df_final")

            existing = conn.execute("""
                SELECT COUNT(*) FROM temp_import t
                WHERE EXISTS (
                    SELECT 1 FROM noc_tickets n
                    WHERE n.ticket_number_inap = t.ticket_number_inap
                    AND n.calc_source = t.calc_source
                    AND n.ticket_number_inap IS NOT NULL
                )
            """).fetchone()[0]

            skipped = existing

            conn.execute("""
                INSERT INTO noc_tickets
                SELECT t.* FROM temp_import t
                WHERE NOT EXISTS (
                    SELECT 1 FROM noc_tickets n
                    WHERE n.ticket_number_inap = t.ticket_number_inap
                    AND n.calc_source = t.calc_source
                    AND n.ticket_number_inap IS NOT NULL
                )
            """)

            imported = total_rows - skipped
            conn.execute("DROP TABLE IF EXISTS temp_import")
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
                conn.execute("DROP TABLE IF EXISTS temp_import")
            except Exception:
                pass
            raise

    update_progress("summarizing", "Memperbarui summary...", 0, 0)

    year_months = df["calc_year_month"].dropna().unique().tolist()
    if year_months:
        from backend.services.summary_service import refresh_summaries
        refresh_summaries(year_months)

    period = year_months[0] if year_months else None

    _log_import(file_path, file_type, period, total_rows, imported, skipped, orphans, time.time() - start_time)

    update_progress("completed", "Selesai", total_rows, total_rows)

    duration = round(time.time() - start_time, 2)
    return {
        "file_type": file_type,
        "period": period,
        "total": total_rows,
        "imported": imported,
        "skipped": skipped,
        "errors": 0,
        "orphans": orphans,
        "calculated_columns": 17,
        "duration_sec": duration,
    }


def _load_area_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT area_id, area_alias FROM master_area").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            result[r[0]] = r[0]
    return result


def _load_regional_ticket_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT regional_id, regional_alias_ticket, regional_name FROM master_regional").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            if r[2]:
                result[r[2]] = r[0]
            result[r[0]] = r[0]
    return result


def _load_nop_ticket_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT nop_id, nop_alias_ticket, nop_name FROM master_nop").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            if r[2]:
                result[r[2]] = r[0]
            result[r[0]] = r[0]
    return result


def _load_to_ticket_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT to_id, to_alias_ticket, to_name FROM master_to").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            if r[2]:
                result[r[2]] = r[0]
            result[r[0]] = r[0]
    return result


def _auto_map_area_from_tickets(df):
    if "regional" not in df.columns or "area" not in df.columns:
        return

    pairs = df[["regional", "area"]].dropna().drop_duplicates()
    if pairs.empty:
        return

    area_map = _load_area_map()

    with get_write_connection() as conn:
        for _, row in pairs.iterrows():
            reg_val = str(row["regional"]).strip()
            area_val = str(row["area"]).strip()

            area_id = area_map.get(area_val)
            if not area_id:
                continue

            conn.execute("""
                UPDATE master_regional
                SET area_id = ?, regional_alias_ticket = ?
                WHERE (regional_alias_ticket = ? OR regional_name = ?)
                AND (area_id IS NULL OR area_id = '')
            """, [area_id, reg_val, reg_val, reg_val])


def _load_sla_targets(df):
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT site_class, site_flag, mttr_target_min, priority FROM master_sla_target ORDER BY priority DESC"
            ).fetchall()

        if not rows:
            return

        def get_target(site_class, site_flag):
            for r in rows:
                sc_match = r[0] == '*' or r[0] == site_class
                sf_match = r[1] == '*' or r[1] == site_flag
                if sc_match and sf_match:
                    return r[2]
            return None

        if "site_class" in df.columns and "site_flag" not in df.columns:
            df["calc_sla_target_min"] = df["site_class"].apply(
                lambda x: get_target(str(x).strip(), '*') if pd.notna(x) else None
            )
        elif "site_class" in df.columns:
            df["calc_sla_target_min"] = df.apply(
                lambda r: get_target(
                    str(r.get("site_class", "")).strip(),
                    str(r.get("site_flag", "")).strip() if pd.notna(r.get("site_flag")) else "*"
                ), axis=1
            )
    except Exception as e:
        logger.warning(f"SLA target loading failed: {e}")


def _log_orphans(source, orphan_details):
    try:
        with get_write_connection() as conn:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM orphan_log").fetchone()[0]
            for level, value, count in orphan_details:
                max_id += 1
                existing = conn.execute(
                    "SELECT id FROM orphan_log WHERE source = ? AND level = ? AND value = ? AND resolved = FALSE",
                    [source, level, value]
                ).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE orphan_log SET ticket_count = ticket_count + ? WHERE id = ?",
                        [int(count), existing[0]]
                    )
                else:
                    conn.execute("""
                        INSERT INTO orphan_log (id, source, level, value, resolved, ticket_count)
                        VALUES (?, ?, ?, ?, FALSE, ?)
                    """, [max_id, source, level, value, int(count)])
    except Exception as e:
        logger.warning(f"Failed to log orphans: {e}")


def _log_import(file_path, file_type, period, total, imported, skipped, orphans, duration):
    orphan_count = sum(v for v in orphans.values() if isinstance(v, int) and v > 0)
    filename = os.path.basename(file_path)
    file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2) if os.path.exists(file_path) else 0

    with get_write_connection() as conn:
        max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM import_logs").fetchone()[0]
        conn.execute("""
            INSERT INTO import_logs (id, filename, file_type, file_size_mb, period,
                rows_total, rows_imported, rows_skipped, rows_error,
                orphan_count, processing_time_sec, status, backup_created)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 'completed', TRUE)
        """, [
            max_id + 1, filename, file_type, file_size_mb, period,
            total, imported, skipped, orphan_count, round(duration, 2),
        ])
