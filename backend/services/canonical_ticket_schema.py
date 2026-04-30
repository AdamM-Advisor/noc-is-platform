from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalColumn:
    name: str
    logical_type: str = "string"
    required: bool = False


TICKET_COLUMNS = [
    CanonicalColumn("ticket_number_inap"),
    CanonicalColumn("ticket_number_swfm"),
    CanonicalColumn("ticket_creation"),
    CanonicalColumn("ticket_creator"),
    CanonicalColumn("severity", required=True),
    CanonicalColumn("type_ticket"),
    CanonicalColumn("fault_level"),
    CanonicalColumn("impact"),
    CanonicalColumn("ne_class"),
    CanonicalColumn("incident_priority"),
    CanonicalColumn("site_id", required=True),
    CanonicalColumn("site_name"),
    CanonicalColumn("site_class"),
    CanonicalColumn("cluster_to"),
    CanonicalColumn("sub_cluster"),
    CanonicalColumn("nop"),
    CanonicalColumn("regional"),
    CanonicalColumn("area"),
    CanonicalColumn("hub"),
    CanonicalColumn("occured_time", "timestamp"),
    CanonicalColumn("created_at", "timestamp"),
    CanonicalColumn("cleared_time", "timestamp"),
    CanonicalColumn("submitted_time", "timestamp"),
    CanonicalColumn("take_over_date", "timestamp"),
    CanonicalColumn("check_in_at", "timestamp"),
    CanonicalColumn("dispatch_date", "timestamp"),
    CanonicalColumn("follow_up_at", "timestamp"),
    CanonicalColumn("closed_at", "timestamp"),
    CanonicalColumn("site_cleared_on", "timestamp"),
    CanonicalColumn("rca_validate_at", "timestamp"),
    CanonicalColumn("duration_ticket"),
    CanonicalColumn("age_ticket"),
    CanonicalColumn("rh_start"),
    CanonicalColumn("rh_start_time"),
    CanonicalColumn("rh_stop"),
    CanonicalColumn("rh_stop_time"),
    CanonicalColumn("ticket_inap_status"),
    CanonicalColumn("ticket_swfm_status"),
    CanonicalColumn("sla_status"),
    CanonicalColumn("holding_status"),
    CanonicalColumn("pic_take_over_ticket"),
    CanonicalColumn("is_escalate"),
    CanonicalColumn("escalate_to"),
    CanonicalColumn("is_auto_resolved"),
    CanonicalColumn("assignee_group"),
    CanonicalColumn("dispatch_by"),
    CanonicalColumn("is_force_dispatch"),
    CanonicalColumn("is_excluded_in_kpi"),
    CanonicalColumn("rc_owner"),
    CanonicalColumn("rc_category"),
    CanonicalColumn("rc_1"),
    CanonicalColumn("rc_2"),
    CanonicalColumn("inap_rc_1"),
    CanonicalColumn("inap_rc_2"),
    CanonicalColumn("resolution_action"),
    CanonicalColumn("inap_resolution_action"),
    CanonicalColumn("rc_owner_engineer"),
    CanonicalColumn("rc_category_engineer"),
    CanonicalColumn("rc_1_engineer"),
    CanonicalColumn("rc_2_engineer"),
    CanonicalColumn("rca_validated"),
    CanonicalColumn("rca_validated_by"),
    CanonicalColumn("summary"),
    CanonicalColumn("description"),
    CanonicalColumn("note"),
    CanonicalColumn("nossa_no"),
    CanonicalColumn("rank"),
    CanonicalColumn("pic_email"),
    CanonicalColumn("rat"),
    CanonicalColumn("parking_status"),
    CanonicalColumn("parking_start"),
    CanonicalColumn("parking_end"),
    CanonicalColumn("yearmonth"),
]

CALC_COLUMNS = [
    CanonicalColumn("calc_response_time_min", "double"),
    CanonicalColumn("calc_repair_time_min", "double"),
    CanonicalColumn("calc_restore_time_min", "double"),
    CanonicalColumn("calc_detection_time_min", "double"),
    CanonicalColumn("calc_sla_duration_min", "double"),
    CanonicalColumn("calc_sla_target_min", "double"),
    CanonicalColumn("calc_is_sla_met", "boolean"),
    CanonicalColumn("calc_hour_of_day", "integer"),
    CanonicalColumn("calc_day_of_week", "integer"),
    CanonicalColumn("calc_week_of_month", "integer"),
    CanonicalColumn("calc_month", "integer"),
    CanonicalColumn("calc_year", "integer"),
    CanonicalColumn("calc_year_month"),
    CanonicalColumn("calc_year_week"),
    CanonicalColumn("calc_area_id"),
    CanonicalColumn("calc_regional_id"),
    CanonicalColumn("calc_nop_id"),
    CanonicalColumn("calc_to_id"),
    CanonicalColumn("calc_source"),
]

CANONICAL_TICKET_COLUMN_NAMES = [col.name for col in TICKET_COLUMNS]
CANONICAL_SILVER_COLUMN_NAMES = CANONICAL_TICKET_COLUMN_NAMES + [col.name for col in CALC_COLUMNS]
REQUIRED_TICKET_COLUMNS = [col.name for col in TICKET_COLUMNS if col.required]


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip().lower())
    return re.sub(r"_+", "_", normalized).strip("_")


def normalize_column_names(names: list[str]) -> list[str]:
    return [normalize_column_name(name) for name in names]


def validate_ticket_columns(raw_columns: list[str]) -> dict:
    normalized = normalize_column_names(raw_columns)
    normalized_set = set(normalized)
    canonical_set = set(CANONICAL_TICKET_COLUMN_NAMES)
    required_set = set(REQUIRED_TICKET_COLUMNS)

    missing_required = sorted(required_set - normalized_set)
    known = [col for col in normalized if col in canonical_set]
    unknown = [col for col in normalized if col not in canonical_set]
    missing_optional = sorted(canonical_set - normalized_set - required_set)

    return {
        "valid": len(missing_required) == 0,
        "raw_columns": raw_columns,
        "normalized_columns": normalized,
        "known_columns": known,
        "unknown_columns": unknown,
        "missing_required": missing_required,
        "missing_optional": missing_optional,
        "required_columns": REQUIRED_TICKET_COLUMNS,
        "canonical_columns": CANONICAL_TICKET_COLUMN_NAMES,
    }


def select_bronze_columns_sql(raw_columns: list[str]) -> str:
    normalized = normalize_column_names(raw_columns)
    seen = {}
    select_parts = []

    for original, normalized_name in zip(raw_columns, normalized):
        if normalized_name in seen:
            seen[normalized_name] += 1
            normalized_name = f"{normalized_name}_{seen[normalized_name]}"
        else:
            seen[normalized_name] = 0
        select_parts.append(f"{quote_identifier(original)} AS {quote_identifier(normalized_name)}")

    return ",\n                ".join(select_parts)


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'
