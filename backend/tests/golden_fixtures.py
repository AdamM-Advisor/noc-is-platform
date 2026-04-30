from __future__ import annotations

from pathlib import Path


GOLDEN_TICKET_COLUMNS = [
    "ticket_number_inap",
    "ticket_number_swfm",
    "site_id",
    "site_name",
    "site_class",
    "severity",
    "type_ticket",
    "fault_level",
    "cluster_to",
    "nop",
    "regional",
    "area",
    "occured_time",
    "created_at",
    "take_over_date",
    "cleared_time",
    "sla_status",
    "is_escalate",
    "is_auto_resolved",
    "rc_category",
    "summary",
]


def _row(
    ticket_id: str,
    site_id: str,
    severity: str,
    occured_time: str,
    sla_status: str,
    area: str = "AREA-GOLDEN-01",
) -> dict:
    return {
        "ticket_number_inap": ticket_id,
        "ticket_number_swfm": ticket_id.replace("INAP", "SWFM"),
        "site_id": site_id,
        "site_name": site_id.replace("-", " ").title(),
        "site_class": "Silver",
        "severity": severity,
        "type_ticket": "Synthetic",
        "fault_level": "Synthetic Fault",
        "cluster_to": "TO-GOLDEN-03",
        "nop": "NOP-GOLDEN-02",
        "regional": "REG-GOLDEN-02",
        "area": area,
        "occured_time": occured_time,
        "created_at": occured_time,
        "take_over_date": occured_time,
        "cleared_time": occured_time,
        "sla_status": sla_status,
        "is_escalate": "N",
        "is_auto_resolved": "N",
        "rc_category": "Synthetic",
        "summary": "Synthetic anonymized ticket",
    }


GOLDEN_FIXTURE_SPECS = [
    {
        "source": "swfm_realtime",
        "year": 2026,
        "month": 1,
        "filename": "tickets_swfm_realtime_2026-01.parquet",
        "rows": [
            {
                "ticket_number_inap": "GOLDEN-INAP-0001",
                "ticket_number_swfm": "GOLDEN-SWFM-0001",
                "site_id": "SITE-GOLDEN-001",
                "site_name": "Golden Site Alpha",
                "site_class": "Platinum",
                "severity": "Critical",
                "type_ticket": "Power",
                "fault_level": "Site Down",
                "cluster_to": "TO-GOLDEN-01",
                "nop": "NOP-GOLDEN-01",
                "regional": "REG-GOLDEN-01",
                "area": "AREA-GOLDEN-01",
                "occured_time": "2026-01-05 01:00:00",
                "created_at": "2026-01-05 01:10:00",
                "take_over_date": "2026-01-05 01:25:00",
                "cleared_time": "2026-01-05 03:00:00",
                "sla_status": "IN SLA",
                "is_escalate": "Y",
                "is_auto_resolved": "N",
                "rc_category": "Power",
                "summary": "Synthetic critical power outage",
            },
            {
                "ticket_number_inap": "GOLDEN-INAP-0002",
                "ticket_number_swfm": "GOLDEN-SWFM-0002",
                "site_id": "SITE-GOLDEN-001",
                "site_name": "Golden Site Alpha",
                "site_class": "Platinum",
                "severity": "Major",
                "type_ticket": "Transmission",
                "fault_level": "Degraded",
                "cluster_to": "TO-GOLDEN-01",
                "nop": "NOP-GOLDEN-01",
                "regional": "REG-GOLDEN-01",
                "area": "AREA-GOLDEN-01",
                "occured_time": "2026-01-12 08:00:00",
                "created_at": "2026-01-12 08:15:00",
                "take_over_date": "2026-01-12 08:35:00",
                "cleared_time": "2026-01-12 11:00:00",
                "sla_status": "OUT SLA",
                "is_escalate": "YES",
                "is_auto_resolved": "N",
                "rc_category": "Transport",
                "summary": "Synthetic transmission degradation",
            },
            {
                "ticket_number_inap": "GOLDEN-INAP-0003",
                "ticket_number_swfm": "GOLDEN-SWFM-0003",
                "site_id": "SITE-GOLDEN-002",
                "site_name": "Golden Site Beta",
                "site_class": "Gold",
                "severity": "Minor",
                "type_ticket": "Radio",
                "fault_level": "Cell Down",
                "cluster_to": "TO-GOLDEN-02",
                "nop": "NOP-GOLDEN-01",
                "regional": "REG-GOLDEN-01",
                "area": "AREA-GOLDEN-01",
                "occured_time": "2026-01-18 13:00:00",
                "created_at": "2026-01-18 13:05:00",
                "take_over_date": "2026-01-18 13:20:00",
                "cleared_time": "2026-01-18 13:50:00",
                "sla_status": "IN SLA",
                "is_escalate": "N",
                "is_auto_resolved": "AUTO RESOLVED",
                "rc_category": "Radio",
                "summary": "Synthetic minor radio ticket",
            },
            {
                "ticket_number_inap": "GOLDEN-INAP-0002",
                "ticket_number_swfm": "GOLDEN-SWFM-0004",
                "site_id": "SITE-GOLDEN-001",
                "site_name": "Golden Site Alpha",
                "site_class": "Platinum",
                "severity": "Major",
                "type_ticket": "Transmission",
                "fault_level": "Degraded",
                "cluster_to": "TO-GOLDEN-01",
                "nop": "NOP-GOLDEN-01",
                "regional": "REG-GOLDEN-01",
                "area": "AREA-GOLDEN-01",
                "occured_time": "2026-01-12 08:00:00",
                "created_at": "2026-01-12 08:16:00",
                "take_over_date": "2026-01-12 08:36:00",
                "cleared_time": "2026-01-12 11:10:00",
                "sla_status": "OUT SLA",
                "is_escalate": "Y",
                "is_auto_resolved": "N",
                "rc_category": "Transport",
                "summary": "Synthetic duplicate ticket for repeat-risk validation",
            },
        ],
    },
    {
        "source": "swfm_realtime",
        "year": 2026,
        "month": 2,
        "filename": "tickets_swfm_realtime_2026-02.parquet",
        "rows": [
            {
                "ticket_number_inap": f"GOLDEN-INAP-02{i:02d}",
                "ticket_number_swfm": f"GOLDEN-SWFM-02{i:02d}",
                "site_id": "SITE-GOLDEN-001" if i < 5 else "SITE-GOLDEN-002",
                "site_name": "Golden Site Alpha" if i < 5 else "Golden Site Beta",
                "site_class": "Platinum" if i < 5 else "Gold",
                "severity": "Major" if i % 2 else "Critical",
                "type_ticket": "Power",
                "fault_level": "Intermittent",
                "cluster_to": "TO-GOLDEN-01",
                "nop": "NOP-GOLDEN-01",
                "regional": "REG-GOLDEN-01",
                "area": "AREA-GOLDEN-01",
                "occured_time": f"2026-02-{10 + i:02d} 09:00:00",
                "created_at": f"2026-02-{10 + i:02d} 09:10:00",
                "take_over_date": f"2026-02-{10 + i:02d} 09:40:00",
                "cleared_time": f"2026-02-{10 + i:02d} 12:10:00",
                "sla_status": "OUT SLA",
                "is_escalate": "Y",
                "is_auto_resolved": "N",
                "rc_category": "Power",
                "summary": "Synthetic increasing failure pattern",
            }
            for i in range(1, 6)
        ],
    },
    {
        "source": "swfm_event",
        "year": 2026,
        "month": 1,
        "filename": "tickets_swfm_event_2026-01.parquet",
        "rows": [
            _row("GOLDEN-EVENT-0001", "SITE-GOLDEN-003", "Low", "2026-01-07 10:00:00", "IN SLA"),
            _row("GOLDEN-EVENT-0002", "SITE-GOLDEN-003", "Major", "2026-01-21 14:00:00", "IN SLA"),
        ],
    },
    {
        "source": "fault_center",
        "year": 2026,
        "month": 2,
        "filename": "tickets_fault_center_2026-02.parquet",
        "rows": [
            _row("GOLDEN-FC-0001", "SITE-GOLDEN-004", "Critical", "2026-02-03 02:00:00", "OUT SLA", area="UNKNOWN-AREA"),
            _row("GOLDEN-FC-0002", "SITE-GOLDEN-004", "Major", "2026-02-17 06:00:00", "OUT SLA", area="UNKNOWN-AREA"),
            _row("GOLDEN-FC-0003", "SITE-GOLDEN-005", "Minor", "2026-02-25 16:00:00", "IN SLA", area="AREA-GOLDEN-02"),
        ],
    },
]


GOLDEN_EXPECTED = {
    "row_count": 14,
    "files": 4,
    "source_period_counts": {
        ("swfm_realtime", "2026-01"): 4,
        ("swfm_realtime", "2026-02"): 5,
        ("swfm_event", "2026-01"): 2,
        ("fault_center", "2026-02"): 3,
    },
    "swfm_realtime_2026_01": {
        "total_tickets": 4,
        "total_sla_met": 2,
        "count_critical": 1,
        "count_major": 2,
        "count_minor": 1,
        "duplicate_ticket_numbers": 1,
    },
}


def write_golden_fixture_set(root_dir: Path) -> list[dict]:
    raw_dir = Path(root_dir) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for spec in GOLDEN_FIXTURE_SPECS:
        path = raw_dir / spec["filename"]
        write_ticket_parquet(path, spec["rows"])
        records.append(
            {
                "path": path,
                "storage_uri": str(path),
                "filename": spec["filename"],
                "file_type": "ticket",
                "source": spec["source"],
                "year": spec["year"],
                "month": spec["month"],
                "row_count": len(spec["rows"]),
            }
        )
    return records


def write_ticket_parquet(path: Path, rows: list[dict]) -> Path:
    import duckdb

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(database=":memory:")
    try:
        columns_sql = ", ".join(f"{_quote_identifier(column)} VARCHAR" for column in GOLDEN_TICKET_COLUMNS)
        conn.execute(f"CREATE TABLE golden_tickets ({columns_sql})")
        placeholders = ", ".join("?" for _ in GOLDEN_TICKET_COLUMNS)
        conn.executemany(
            f"INSERT INTO golden_tickets VALUES ({placeholders})",
            [[row.get(column) for column in GOLDEN_TICKET_COLUMNS] for row in rows],
        )
        conn.execute(f"COPY golden_tickets TO '{_duckdb_path(path)}' (FORMAT PARQUET)")
    finally:
        conn.close()
    return path


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _duckdb_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")
