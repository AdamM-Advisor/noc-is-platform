HEADER_MAP = {
    "Ticket Number Inap": "ticket_number_inap",
    "Ticket Number Swfm": "ticket_number_swfm",
    "Ticket Creation": "ticket_creation",
    "Ticket Creator": "ticket_creator",
    "Severity": "severity",
    "Type Ticket": "type_ticket",
    "Fault Level": "fault_level",
    "Impact": "impact",
    "Ne Class": "ne_class",
    "Incident Priority": "incident_priority",
    "Site Id": "site_id",
    "Site Name": "site_name",
    "Site Class": "site_class",
    "Cluster To": "cluster_to",
    "Sub Cluster": "sub_cluster",
    "Nop": "nop",
    "Regional": "regional",
    "Area": "area",
    "Hub": "hub",
    "Occured Time": "occured_time",
    "Created At": "created_at",
    "Cleared Time": "cleared_time",
    "Submitted Time": "submitted_time",
    "Take Over Date": "take_over_date",
    "Check In At": "check_in_at",
    "Dispatch Date": "dispatch_date",
    "Follow Up At": "follow_up_at",
    "Closed At": "closed_at",
    "Site Cleared On": "site_cleared_on",
    "Rca Validate At": "rca_validate_at",
    "Duration Ticket": "duration_ticket",
    "Age Ticket": "age_ticket",
    "Rh Start": "rh_start",
    "Rh Start Time": "rh_start_time",
    "Rh Stop": "rh_stop",
    "Rh Stop Time": "rh_stop_time",
    "Ticket Inap Status": "ticket_inap_status",
    "Ticket Swfm Status": "ticket_swfm_status",
    "Sla Status": "sla_status",
    "Holding Status": "holding_status",
    "Pic Take Over Ticket": "pic_take_over_ticket",
    "Is Escalate": "is_escalate",
    "Escalate To": "escalate_to",
    "Is Auto Resolved": "is_auto_resolved",
    "Assignee Group": "assignee_group",
    "Dispatch By": "dispatch_by",
    "Is Force Dispatch": "is_force_dispatch",
    "Is Excluded In Kpi": "is_excluded_in_kpi",
    "Rc Owner": "rc_owner",
    "Rc Category": "rc_category",
    "Rc 1": "rc_1",
    "Rc 2": "rc_2",
    "Inap Rc 1": "inap_rc_1",
    "Inap Rc 2": "inap_rc_2",
    "Resolution Action": "resolution_action",
    "Inap Resolution Action": "inap_resolution_action",
    "Rc Owner Engineer": "rc_owner_engineer",
    "Rc Category Engineer": "rc_category_engineer",
    "Rc 1 Engineer": "rc_1_engineer",
    "Rc 2 Engineer": "rc_2_engineer",
    "Rca Validated": "rca_validated",
    "Rca Validated By": "rca_validated_by",
    "Summary": "summary",
    "Description": "description",
    "Note": "note",
    "Nossa No": "nossa_no",
    "Rank": "rank",
    "Pic Email": "pic_email",
    "Rat": "rat",
    "Parking Status": "parking_status",
    "Parking Start": "parking_start",
    "Parking End": "parking_end",
}

SITE_MASTER_MAP = {
    "SITE_ID": "site_id",
    "SITE_NAME": "site_name",
    "ID_REGION_NETWORK": "id_region_network",
    "NOP_NAME": "nop_name",
    "SITEAREA_TO": "sitearea_to",
    "CLASS": "site_class",
    "FLAG_SITE": "site_flag",
}

EXPECTED_TICKET_COLUMNS = [
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
    "pic_email", "rat", "parking_status", "parking_start", "parking_end",
]


def normalize_headers(df, header_format: str) -> dict:
    import re
    original_cols = list(df.columns)
    mapped = []
    unmapped = []
    rename_map = {}

    if header_format == "upper_case":
        for col in original_cols:
            col_str = str(col).strip()
            if col_str in SITE_MASTER_MAP:
                target = SITE_MASTER_MAP[col_str]
                rename_map[col] = target
                mapped.append(f"{col_str} -> {target}")
            else:
                target = col_str.lower()
                rename_map[col] = target
                unmapped.append(col_str)
    elif header_format == "snake_case":
        for col in original_cols:
            col_str = str(col).strip().lower()
            rename_map[col] = col_str
            mapped.append(f"{col} -> {col_str}")
    else:
        reverse_map = {v: v for v in HEADER_MAP.values()}
        for col in original_cols:
            col_str = str(col).strip()
            if col_str in HEADER_MAP:
                target = HEADER_MAP[col_str]
                rename_map[col] = target
                mapped.append(f"{col_str} -> {target}")
            else:
                target = re.sub(r'[^a-zA-Z0-9]', '_', col_str.lower()).strip('_')
                target = re.sub(r'_+', '_', target)
                rename_map[col] = target
                if target not in reverse_map:
                    unmapped.append(col_str)

    df = df.rename(columns=rename_map)

    current_cols = set(df.columns)
    missing = [c for c in EXPECTED_TICKET_COLUMNS if c not in current_cols]

    return {
        "df": df,
        "mapped": mapped,
        "unmapped": unmapped,
        "missing": missing,
    }
