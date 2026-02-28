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


def _vectorized_enrich(df: pd.DataFrame) -> pd.DataFrame:
    cls = df["site_class"]
    flag = df["site_flag"]

    df["site_category"] = "Non-Komersial"
    df.loc[flag == "Site Reguler", "site_category"] = "Komersial"

    df["site_sub_class"] = cls
    df.loc[flag == "No BTS", "site_sub_class"] = "No BTS"
    df.loc[flag == "3T", "site_sub_class"] = cls + "-3T"
    df.loc[flag == "USO/MP", "site_sub_class"] = cls + "-USO"
    df.loc[flag == "Femto", "site_sub_class"] = cls + "-Femto"

    df["est_technology"] = "N/A"
    df.loc[flag == "No BTS", "est_technology"] = "Tidak Ada BTS"
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "est_technology"] = "Multi (2G/3G/4G/5G)"
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "est_technology"] = "Multi (2G/3G/4G)"
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "est_technology"] = "Limited (4G / 2G+4G)"
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP"]), "est_technology"] = "Single (4G LTE)"
    df.loc[(cls == "Bronze") & (flag == "Femto"), "est_technology"] = "Single (4G Femto)"

    df["est_transmission"] = "N/A"
    df.loc[flag == "No BTS", "est_transmission"] = "Tidak Ada"
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "est_transmission"] = "Fiber Optic + Microwave"
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "est_transmission"] = "Fiber Optic atau Microwave"
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "est_transmission"] = "Microwave"
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP"]), "est_transmission"] = "VSAT atau Microwave"
    df.loc[(cls == "Bronze") & (flag == "Femto"), "est_transmission"] = "WiFi Backhaul / VSAT"

    df["est_power"] = "N/A"
    df.loc[flag == "No BTS", "est_power"] = "Tidak Ada"
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "est_power"] = "PLN + Genset + Baterai (redundan)"
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "est_power"] = "PLN + Baterai"
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "est_power"] = "PLN + Baterai (minimal)"
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP"]), "est_power"] = "Solar Panel + Baterai"
    df.loc[(cls == "Bronze") & (flag == "Femto"), "est_power"] = "PLN (rumah/gedung)"

    df["est_sector"] = "N/A"
    df.loc[flag == "No BTS", "est_sector"] = "Tidak Ada"
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "est_sector"] = "3+ Sektor (directional)"
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "est_sector"] = "3 Sektor"
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "est_sector"] = "1-2 Sektor"
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP", "Femto"]), "est_sector"] = "Omni (360\u00b0)"

    df["complexity_level"] = 0
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "complexity_level"] = 5
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "complexity_level"] = 4
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "complexity_level"] = 3
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP"]), "complexity_level"] = 2
    df.loc[(cls == "Bronze") & (flag == "Femto"), "complexity_level"] = 1

    opex_map = {5: "Very High", 4: "High", 3: "Medium", 2: "Low", 1: "Very Low", 0: "N/A"}
    df["est_opex_level"] = df["complexity_level"].map(opex_map).fillna("N/A")

    df["upgrade_potential"] = "N/A"
    df.loc[cls.isin(["Diamond", "Platinum"]), "upgrade_potential"] = "N/A (Tertinggi)"
    df.loc[cls == "Gold", "upgrade_potential"] = "Low"
    df.loc[cls == "Silver", "upgrade_potential"] = "Medium"
    df.loc[cls == "Bronze", "upgrade_potential"] = "High"

    df["strategy_focus"] = "N/A"
    df.loc[flag == "No BTS", "strategy_focus"] = "Non-Applicable"
    df.loc[cls.isin(["Diamond", "Platinum"]) & (flag != "No BTS"), "strategy_focus"] = "Capacity & Quality Management"
    df.loc[cls.isin(["Gold", "Silver"]) & (flag != "No BTS"), "strategy_focus"] = "Reliability & Optimization"
    df.loc[(cls == "Bronze") & (flag == "Site Reguler"), "strategy_focus"] = "OPEX Efficiency"
    df.loc[(cls == "Bronze") & flag.isin(["3T", "USO/MP"]), "strategy_focus"] = "Availability & Access"
    df.loc[(cls == "Bronze") & (flag == "Femto"), "strategy_focus"] = "Monitoring Minimal"

    return df


def process_site_master(file_path: str, progress_callback=None) -> dict:
    start_time = time.time()

    def update(phase, detail="", row=0, total=0):
        if progress_callback:
            progress_callback(phase, detail, row, total)

    update("reading", "Membaca file...")

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
    update("reading", f"File dibaca: {total_rows} baris", 0, total_rows)

    result = normalize_headers(df, "upper_case")
    df = result["df"]

    required = ["site_id", "site_name", "site_class", "site_flag"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df["site_id"] = df["site_id"].astype(str).str.strip()
    df["site_name"] = df["site_name"].astype(str).str.strip()
    df["site_class"] = df["site_class"].astype(str).str.strip()
    df["site_flag"] = df["site_flag"].astype(str).str.strip()

    df = df[df["site_id"].str.len() > 0].copy()

    update("processing", "Auto-populasi hierarki...", 0, total_rows)
    _auto_populate_hierarchy(df)

    update("processing", "Memetakan TO...", 0, total_rows)
    to_map = _load_to_map()
    if "sitearea_to" in df.columns:
        df["sitearea_to"] = df["sitearea_to"].astype(str).str.strip()
        df["to_id"] = df["sitearea_to"].map(to_map)
    else:
        df["to_id"] = None

    orphan_to = 0
    if "sitearea_to" in df.columns:
        has_to_name = df["sitearea_to"].notna() & (df["sitearea_to"] != "") & (df["sitearea_to"] != "nan")
        no_to_id = df["to_id"].isna()
        orphan_to = int((has_to_name & no_to_id).sum())

    update("processing", "Enrichment data site...", 0, total_rows)
    df = _vectorized_enrich(df)

    update("writing", "Menyimpan ke database...", 0, total_rows)

    cols = ["site_id", "site_name", "to_id", "site_class", "site_flag",
            "site_category", "site_sub_class", "upgrade_potential",
            "est_technology", "est_transmission", "est_power",
            "est_sector", "complexity_level", "est_opex_level",
            "strategy_focus"]
    insert_df = df[cols].copy()
    insert_df["complexity_level"] = insert_df["complexity_level"].astype(int)
    insert_df = insert_df.drop_duplicates(subset=["site_id"], keep="last")

    with get_write_connection() as conn:
        conn.register("site_staging", insert_df)

        overlap = conn.execute("""
            SELECT COUNT(*) FROM site_staging s
            WHERE EXISTS (SELECT 1 FROM master_site m WHERE m.site_id = s.site_id)
        """).fetchone()[0]

        conn.execute("""
            INSERT INTO master_site (
                site_id, site_name, to_id, site_class, site_flag,
                site_category, site_sub_class, upgrade_potential,
                est_technology, est_transmission, est_power,
                est_sector, complexity_level, est_opex_level,
                strategy_focus
            )
            SELECT
                site_id, site_name, to_id, site_class, site_flag,
                site_category, site_sub_class, upgrade_potential,
                est_technology, est_transmission, est_power,
                est_sector, complexity_level, est_opex_level,
                strategy_focus
            FROM site_staging
            ON CONFLICT (site_id) DO UPDATE SET
                site_name = EXCLUDED.site_name,
                to_id = EXCLUDED.to_id,
                site_class = EXCLUDED.site_class,
                site_flag = EXCLUDED.site_flag,
                site_category = EXCLUDED.site_category,
                site_sub_class = EXCLUDED.site_sub_class,
                upgrade_potential = EXCLUDED.upgrade_potential,
                est_technology = EXCLUDED.est_technology,
                est_transmission = EXCLUDED.est_transmission,
                est_power = EXCLUDED.est_power,
                est_sector = EXCLUDED.est_sector,
                complexity_level = EXCLUDED.complexity_level,
                est_opex_level = EXCLUDED.est_opex_level,
                strategy_focus = EXCLUDED.strategy_focus
        """)

        conn.unregister("site_staging")

    unique_count = len(insert_df)
    updated = overlap
    inserted = unique_count - overlap

    update("completed", "Selesai", total_rows, total_rows)

    duration = round(time.time() - start_time, 2)
    return {
        "total": total_rows,
        "inserted": inserted,
        "updated": updated,
        "orphans": {"regional": 0, "nop": 0, "to": orphan_to},
        "enriched": total_rows,
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
