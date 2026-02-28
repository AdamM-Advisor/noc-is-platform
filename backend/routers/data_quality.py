from fastapi import APIRouter, HTTPException
from backend.database import get_connection

router = APIRouter(prefix="/data-quality")


@router.get("/summary")
async def get_data_quality_summary():
    try:
        with get_connection() as conn:
            hierarchy = _get_hierarchy_completeness(conn)
            alias_coverage = _get_alias_coverage(conn)
            orphan_counts = _get_orphan_counts(conn)
            data_coverage = _get_data_coverage(conn)
            enrichment_coverage = _get_enrichment_coverage(conn)

        return {
            "hierarchy_completeness": hierarchy,
            "alias_coverage": alias_coverage,
            "orphan_counts": orphan_counts,
            "data_coverage": data_coverage,
            "enrichment_coverage": enrichment_coverage,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _get_hierarchy_completeness(conn):
    result = {}
    levels = [
        ("area", "master_area"),
        ("regional", "master_regional"),
        ("nop", "master_nop"),
        ("to", "master_to"),
        ("site", "master_site"),
    ]
    for level, table in levels:
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            active = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE status = 'ACTIVE'"
            ).fetchone()[0]
            result[level] = {"total": total, "active": active, "inactive": total - active}
        except Exception:
            result[level] = {"total": 0, "active": 0, "inactive": 0}
    return result


def _get_alias_coverage(conn):
    coverage = {}
    alias_checks = [
        ("regional", "master_regional", ["regional_alias_site_master", "regional_alias_ticket"]),
        ("nop", "master_nop", ["nop_alias_site_master", "nop_alias_ticket"]),
        ("to", "master_to", ["to_alias_site_master", "to_alias_ticket"]),
    ]
    for level, table, alias_cols in alias_checks:
        try:
            total = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE status = 'ACTIVE'"
            ).fetchone()[0]
            col_coverage = {}
            for col in alias_cols:
                filled = conn.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE status = 'ACTIVE' AND {col} IS NOT NULL AND {col} != ''"
                ).fetchone()[0]
                col_coverage[col] = {
                    "filled": filled,
                    "total": total,
                    "pct": round(filled / total * 100, 1) if total > 0 else 0,
                }
            coverage[level] = col_coverage
        except Exception:
            coverage[level] = {}
    return coverage


def _get_orphan_counts(conn):
    try:
        rows = conn.execute("""
            SELECT level, COUNT(*) as count
            FROM orphan_log
            WHERE resolved = FALSE
            GROUP BY level
        """).fetchall()
        orphans = {row[0]: row[1] for row in rows}
        total = sum(orphans.values())
        return {"by_level": orphans, "total": total}
    except Exception:
        return {"by_level": {}, "total": 0}


def _get_data_coverage(conn):
    try:
        rows = conn.execute("""
            SELECT period, file_type, COUNT(*) as imports, SUM(rows_imported) as total_rows
            FROM import_logs
            WHERE status = 'completed'
            GROUP BY period, file_type
            ORDER BY period DESC, file_type
        """).fetchall()
        matrix = []
        for row in rows:
            matrix.append({
                "period": row[0],
                "file_type": row[1],
                "imports": row[2],
                "total_rows": row[3],
            })
        return matrix
    except Exception:
        return []


def _get_enrichment_coverage(conn):
    try:
        total_sites = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE'"
        ).fetchone()[0]

        with_hierarchy = conn.execute("""
            SELECT COUNT(*)
            FROM master_site s
            INNER JOIN v_hierarchy h ON s.to_id = h.to_id
            WHERE s.status = 'ACTIVE'
        """).fetchone()[0]

        with_class = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND site_class IS NOT NULL AND site_class != ''"
        ).fetchone()[0]

        with_flag = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND site_flag IS NOT NULL AND site_flag != ''"
        ).fetchone()[0]

        with_coords = conn.execute(
            "SELECT COUNT(*) FROM master_site WHERE status = 'ACTIVE' AND latitude IS NOT NULL AND longitude IS NOT NULL"
        ).fetchone()[0]

        return {
            "total_sites": total_sites,
            "with_hierarchy": with_hierarchy,
            "with_class": with_class,
            "with_flag": with_flag,
            "with_coordinates": with_coords,
            "hierarchy_pct": round(with_hierarchy / total_sites * 100, 1) if total_sites > 0 else 0,
            "class_pct": round(with_class / total_sites * 100, 1) if total_sites > 0 else 0,
            "flag_pct": round(with_flag / total_sites * 100, 1) if total_sites > 0 else 0,
            "coordinates_pct": round(with_coords / total_sites * 100, 1) if total_sites > 0 else 0,
        }
    except Exception:
        return {
            "total_sites": 0,
            "with_hierarchy": 0,
            "with_class": 0,
            "with_flag": 0,
            "with_coordinates": 0,
            "hierarchy_pct": 0,
            "class_pct": 0,
            "flag_pct": 0,
            "coordinates_pct": 0,
        }
