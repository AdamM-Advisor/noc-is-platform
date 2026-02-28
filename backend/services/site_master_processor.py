import os
import time
import logging
import re
import pandas as pd
from backend.database import get_write_connection, get_connection
from backend.services.header_normalizer import normalize_headers
from backend.services.enrichment_service import enrich_site

logger = logging.getLogger(__name__)


def _slugify(name: str) -> str:
    s = name.strip().upper()
    s = re.sub(r'[^A-Z0-9]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s


def process_site_master(file_path: str) -> dict:
    start_time = time.time()

    ext = os.path.splitext(file_path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        df = pd.read_excel(file_path, engine='openpyxl')
    elif ext == '.csv':
        df = pd.read_csv(file_path)
    elif ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    result = normalize_headers(df, "upper_case")
    df = result["df"]

    required = ["site_id", "site_name", "site_class", "site_flag"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    _auto_populate_hierarchy(df)

    regional_map = _load_regional_map()
    nop_map = _load_nop_map()
    to_map = _load_to_map()

    inserted = 0
    updated = 0
    orphans = {"regional": 0, "nop": 0, "to": 0}
    enriched_count = 0

    with get_write_connection() as conn:
        for _, row in df.iterrows():
            site_id = str(row.get("site_id", "")).strip()
            if not site_id:
                continue

            site_name = str(row.get("site_name", "")).strip()
            site_class = str(row.get("site_class", "")).strip()
            site_flag = str(row.get("site_flag", "")).strip()

            to_id = None
            id_region = str(row.get("id_region_network", "")).strip()
            nop_name_val = str(row.get("nop_name", "")).strip()
            to_name_val = str(row.get("sitearea_to", "")).strip()

            if to_name_val and to_name_val in to_map:
                to_id = to_map[to_name_val]
            elif to_name_val:
                orphans["to"] += 1

            enrichment = enrich_site(site_class, site_flag)
            enriched_count += 1

            exists = conn.execute(
                "SELECT COUNT(*) FROM master_site WHERE site_id = ?", [site_id]
            ).fetchone()[0]

            if exists:
                conn.execute("""
                    UPDATE master_site SET
                        site_name = ?, to_id = ?, site_class = ?, site_flag = ?,
                        site_category = ?, site_sub_class = ?, upgrade_potential = ?,
                        est_technology = ?, est_transmission = ?, est_power = ?,
                        est_sector = ?, complexity_level = ?, est_opex_level = ?,
                        strategy_focus = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE site_id = ?
                """, [
                    site_name, to_id, site_class, site_flag,
                    enrichment["site_category"], enrichment["site_sub_class"],
                    enrichment["upgrade_potential"], enrichment["est_technology"],
                    enrichment["est_transmission"], enrichment["est_power"],
                    enrichment["est_sector"], enrichment["complexity_level"],
                    enrichment["est_opex_level"], enrichment["strategy_focus"],
                    site_id,
                ])
                updated += 1
            else:
                conn.execute("""
                    INSERT INTO master_site (
                        site_id, site_name, to_id, site_class, site_flag,
                        site_category, site_sub_class, upgrade_potential,
                        est_technology, est_transmission, est_power,
                        est_sector, complexity_level, est_opex_level,
                        strategy_focus
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    site_id, site_name, to_id, site_class, site_flag,
                    enrichment["site_category"], enrichment["site_sub_class"],
                    enrichment["upgrade_potential"], enrichment["est_technology"],
                    enrichment["est_transmission"], enrichment["est_power"],
                    enrichment["est_sector"], enrichment["complexity_level"],
                    enrichment["est_opex_level"], enrichment["strategy_focus"],
                ])
                inserted += 1

    duration = round(time.time() - start_time, 2)
    return {
        "total": len(df),
        "inserted": inserted,
        "updated": updated,
        "orphans": orphans,
        "enriched": enriched_count,
        "duration_sec": duration,
    }


def _auto_populate_hierarchy(df):
    with get_write_connection() as conn:
        existing_regional = conn.execute(
            "SELECT regional_alias_site_master FROM master_regional WHERE regional_alias_site_master IS NOT NULL"
        ).fetchall()
        existing_regional_aliases = {r[0] for r in existing_regional}

        if "id_region_network" in df.columns:
            unique_regionals = df["id_region_network"].dropna().unique()
            for reg_raw in unique_regionals:
                reg_str = str(reg_raw).strip()
                if not reg_str or reg_str in existing_regional_aliases:
                    continue

                match = re.search(r'(\d+)', reg_str)
                num = match.group(1).zfill(2) if match else reg_str
                regional_id = f"R{num}"
                regional_name = f"Regional {int(num)}" if num.isdigit() else reg_str

                try:
                    conn.execute("""
                        INSERT INTO master_regional (regional_id, regional_name, area_id, regional_alias_site_master, status)
                        VALUES (?, ?, '', ?, 'ACTIVE')
                        ON CONFLICT DO NOTHING
                    """, [regional_id, regional_name, reg_str])
                    existing_regional_aliases.add(reg_str)
                except Exception:
                    pass

        reg_alias_to_id = {}
        rows = conn.execute("SELECT regional_id, regional_alias_site_master FROM master_regional").fetchall()
        for r in rows:
            if r[1]:
                reg_alias_to_id[r[1]] = r[0]

        existing_nop = conn.execute(
            "SELECT nop_alias_site_master FROM master_nop WHERE nop_alias_site_master IS NOT NULL"
        ).fetchall()
        existing_nop_aliases = {r[0] for r in existing_nop}

        if "nop_name" in df.columns and "id_region_network" in df.columns:
            nop_regional = df[["nop_name", "id_region_network"]].dropna().drop_duplicates()
            for _, row in nop_regional.iterrows():
                nop_name_val = str(row["nop_name"]).strip()
                reg_val = str(row["id_region_network"]).strip()
                if not nop_name_val or nop_name_val in existing_nop_aliases:
                    continue

                nop_id = _slugify(nop_name_val)
                regional_id = reg_alias_to_id.get(reg_val, "")

                try:
                    conn.execute("""
                        INSERT INTO master_nop (nop_id, nop_name, regional_id, nop_alias_site_master, status)
                        VALUES (?, ?, ?, ?, 'ACTIVE')
                        ON CONFLICT DO NOTHING
                    """, [nop_id, nop_name_val, regional_id, nop_name_val])
                    existing_nop_aliases.add(nop_name_val)
                except Exception:
                    pass

        nop_alias_to_id = {}
        rows = conn.execute("SELECT nop_id, nop_alias_site_master FROM master_nop").fetchall()
        for r in rows:
            if r[1]:
                nop_alias_to_id[r[1]] = r[0]

        existing_to = conn.execute(
            "SELECT to_alias_site_master FROM master_to WHERE to_alias_site_master IS NOT NULL"
        ).fetchall()
        existing_to_aliases = {r[0] for r in existing_to}

        if "sitearea_to" in df.columns and "nop_name" in df.columns:
            to_nop = df[["sitearea_to", "nop_name"]].dropna().drop_duplicates()
            for _, row in to_nop.iterrows():
                to_name_val = str(row["sitearea_to"]).strip()
                nop_name_val = str(row["nop_name"]).strip()
                if not to_name_val or to_name_val in existing_to_aliases:
                    continue

                to_id = _slugify(to_name_val)
                nop_id = nop_alias_to_id.get(nop_name_val, "")

                try:
                    conn.execute("""
                        INSERT INTO master_to (to_id, to_name, nop_id, to_alias_site_master, status)
                        VALUES (?, ?, ?, ?, 'ACTIVE')
                        ON CONFLICT DO NOTHING
                    """, [to_id, to_name_val, nop_id, to_name_val])
                    existing_to_aliases.add(to_name_val)
                except Exception:
                    pass


def _load_regional_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT regional_id, regional_alias_site_master FROM master_regional").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
    return result


def _load_nop_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT nop_id, nop_alias_site_master, nop_name FROM master_nop").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            if r[2]:
                result[r[2]] = r[0]
    return result


def _load_to_map():
    result = {}
    with get_connection() as conn:
        rows = conn.execute("SELECT to_id, to_alias_site_master, to_name FROM master_to").fetchall()
        for r in rows:
            if r[1]:
                result[r[1]] = r[0]
            if r[2]:
                result[r[2]] = r[0]
    return result
