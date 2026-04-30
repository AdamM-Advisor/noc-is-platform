import os
import posixpath
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from backend.config import BASE_DIR


def _root_join(root: str | Path, *parts: str) -> str:
    root_str = str(root).rstrip("/\\")
    if _is_remote_uri(root_str):
        return posixpath.join(root_str, *parts)
    return str(Path(root_str, *parts))


def _is_remote_uri(path: str) -> bool:
    return "://" in path


LAKE_ROOT = os.environ.get("NOCIS_LAKE_ROOT") or str(Path(BASE_DIR) / ".parquet_lake")
TICKET_DATASET = _root_join(LAKE_ROOT, "tickets")


@dataclass(frozen=True)
class TicketLakeFilter:
    date_from: str | None = None
    date_to: str | None = None
    year_month_from: str | None = None
    year_month_to: str | None = None
    source: str | None = None
    entity_level: str | None = None
    entity_id: str | None = None
    severities: tuple[str, ...] = field(default_factory=tuple)
    type_ticket: str | None = None
    fault_level: str | None = None
    rc_category: str | None = None


class ParquetTicketLake:
    """Query helper for partitioned NOC ticket Parquet datasets.

    This service is intentionally small and side-effect-light. It gives the
    reimplementation a scalable query surface before the old pandas ingestion
    flow is replaced.
    """

    ENTITY_COLUMNS = {
        "area": "calc_area_id",
        "regional": "calc_regional_id",
        "nop": "calc_nop_id",
        "to": "calc_to_id",
        "site": "site_id",
    }

    def __init__(self, root: Path | str = TICKET_DATASET):
        self.root = str(root).rstrip("/\\")

    def ensure_layout(self) -> dict:
        bronze = _root_join(self.root, "bronze")
        silver = _root_join(self.root, "silver")
        summary = _root_join(LAKE_ROOT, "summaries")
        if _is_remote_uri(self.root):
            return {
                "root": LAKE_ROOT,
                "bronze": bronze,
                "silver": silver,
                "summaries": summary,
                "mode": "remote",
                "message": "Remote object storage layout is created by write jobs.",
            }

        for directory in (bronze, silver, summary):
            Path(directory).mkdir(parents=True, exist_ok=True)
        return {
            "root": LAKE_ROOT,
            "bronze": bronze,
            "silver": silver,
            "summaries": summary,
            "mode": "local",
        }

    def dataset_glob(self, layer: str = "silver", years: Iterable[int] | None = None) -> str:
        if layer not in {"bronze", "silver"}:
            raise ValueError("layer must be 'bronze' or 'silver'")

        base = _root_join(self.root, layer)
        if years:
            year_parts = ",".join(str(y) for y in sorted(set(years)))
            return _duckdb_path(_root_join(base, f"year={{{year_parts}}}", "month=*", "source=*", "*.parquet"))
        return _duckdb_path(_root_join(base, "year=*", "month=*", "source=*", "*.parquet"))

    def read_parquet_sql(self, layer: str = "silver", years: Iterable[int] | None = None) -> str:
        path_glob = self.dataset_glob(layer=layer, years=years)
        return (
            "read_parquet("
            f"'{path_glob}', "
            "hive_partitioning = true, "
            "union_by_name = true"
            ")"
        )

    def build_where(self, filters: TicketLakeFilter) -> tuple[str, list]:
        clauses = []
        params: list = []

        if filters.date_from:
            clauses.append("occured_time >= ?")
            params.append(filters.date_from)
        if filters.date_to:
            clauses.append("occured_time < ?")
            params.append(filters.date_to)
        if filters.year_month_from:
            clauses.append("calc_year_month >= ?")
            params.append(filters.year_month_from)
        if filters.year_month_to:
            clauses.append("calc_year_month <= ?")
            params.append(filters.year_month_to)
        if filters.source:
            clauses.append("source = ?")
            params.append(filters.source)
        if filters.entity_level and filters.entity_id:
            entity_col = self.ENTITY_COLUMNS.get(filters.entity_level)
            if not entity_col:
                raise ValueError(f"Unsupported entity_level: {filters.entity_level}")
            clauses.append(f"{entity_col} = ?")
            params.append(filters.entity_id)
        if filters.severities:
            placeholders = ", ".join("?" for _ in filters.severities)
            clauses.append(f"severity IN ({placeholders})")
            params.extend(filters.severities)
        if filters.type_ticket:
            clauses.append("type_ticket = ?")
            params.append(filters.type_ticket)
        if filters.fault_level:
            clauses.append("fault_level = ?")
            params.append(filters.fault_level)
        if filters.rc_category:
            clauses.append("rc_category = ?")
            params.append(filters.rc_category)

        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def kpi_query(self, filters: TicketLakeFilter, layer: str = "silver") -> tuple[str, list]:
        where_sql, params = self.build_where(filters)
        sql = f"""
            SELECT
                COUNT(*) AS total_tickets,
                SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) AS total_sla_met,
                100.0 * SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS sla_pct,
                AVG(calc_restore_time_min) AS avg_mttr_min,
                AVG(calc_response_time_min) AS avg_response_min,
                100.0 * SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS escalation_pct,
                100.0 * SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS auto_resolve_pct
            FROM {self.read_parquet_sql(layer=layer)}
            {where_sql}
        """
        return _compact_sql(sql), params

    def monthly_summary_query(self, filters: TicketLakeFilter, layer: str = "silver") -> tuple[str, list]:
        where_sql, params = self.build_where(filters)
        sql = f"""
            SELECT
                calc_year_month,
                calc_area_id,
                calc_regional_id,
                calc_nop_id,
                calc_to_id,
                site_id,
                severity,
                type_ticket,
                fault_level,
                COUNT(*) AS total_tickets,
                SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) AS total_sla_met,
                100.0 * SUM(CASE WHEN calc_is_sla_met THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS sla_pct,
                AVG(calc_restore_time_min) AS avg_mttr_min,
                AVG(calc_response_time_min) AS avg_response_min,
                SUM(CASE WHEN UPPER(COALESCE(is_escalate, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS total_escalated,
                SUM(CASE WHEN UPPER(COALESCE(is_auto_resolved, '')) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END) AS total_auto_resolved
            FROM {self.read_parquet_sql(layer=layer)}
            {where_sql}
            GROUP BY
                calc_year_month,
                calc_area_id,
                calc_regional_id,
                calc_nop_id,
                calc_to_id,
                site_id,
                severity,
                type_ticket,
                fault_level
        """
        return _compact_sql(sql), params


def _duckdb_path(path: str | Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _compact_sql(sql: str) -> str:
    return "\n".join(line.rstrip() for line in sql.strip().splitlines())
