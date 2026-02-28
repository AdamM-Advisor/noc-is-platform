def detect_file_type(filename: str, headers: list) -> dict:
    filename_lower = filename.lower()
    header_format = _detect_header_format(headers)
    num_cols = len(headers)

    if _is_site_master(filename_lower, headers, header_format):
        return {
            "file_type": "site_master",
            "confidence": "high" if "naming" in filename_lower or "site_class" in filename_lower else "medium",
            "reason": f"Filename/headers match site master pattern ({num_cols} cols, {header_format})",
            "header_format": header_format,
            "expected_columns": 7,
        }

    if "realtime" in filename_lower or "swfm_realtime" in filename_lower:
        return {
            "file_type": "swfm_realtime",
            "confidence": "high",
            "reason": f"Filename contains 'realtime', headers are {header_format}",
            "header_format": header_format,
            "expected_columns": 73,
        }

    if header_format == "snake_case" and num_cols >= 70:
        has_yearmonth = "yearmonth" in [str(h).lower().strip() for h in headers]
        if has_yearmonth:
            return {
                "file_type": "swfm_realtime",
                "confidence": "medium",
                "reason": f"snake_case headers with yearmonth column ({num_cols} cols)",
                "header_format": header_format,
                "expected_columns": 73,
            }

    if "swfm_ev" in filename_lower or "swfm_event" in filename_lower:
        return {
            "file_type": "swfm_event",
            "confidence": "high",
            "reason": f"Filename contains 'SWFM_ev', headers are {header_format}",
            "header_format": header_format,
            "expected_columns": 72,
        }

    if "swfm_inc" in filename_lower or "swfm_incident" in filename_lower:
        return {
            "file_type": "swfm_incident",
            "confidence": "high",
            "reason": f"Filename contains 'SWFM_inc', headers are {header_format}",
            "header_format": header_format,
            "expected_columns": 72,
        }

    if "fault_center" in filename_lower or "fault center" in filename_lower:
        return {
            "file_type": "fault_center",
            "confidence": "high",
            "reason": f"Filename contains 'Fault_Center', headers are {header_format}",
            "header_format": header_format,
            "expected_columns": 72,
        }

    if header_format == "title_case" and num_cols >= 70:
        headers_lower = [str(h).lower().strip() for h in headers]
        has_yearmonth = "yearmonth" in headers_lower
        if has_yearmonth:
            return {
                "file_type": "swfm_realtime",
                "confidence": "medium",
                "reason": f"Title case headers with yearmonth column ({num_cols} cols)",
                "header_format": header_format,
                "expected_columns": 73,
            }
        return {
            "file_type": "swfm_event",
            "confidence": "low",
            "reason": f"Title case headers, {num_cols} columns, could not determine specific type from filename",
            "header_format": header_format,
            "expected_columns": 72,
        }

    if header_format == "upper_case" and num_cols <= 10:
        return {
            "file_type": "site_master",
            "confidence": "medium",
            "reason": f"Upper case headers with {num_cols} columns",
            "header_format": header_format,
            "expected_columns": 7,
        }

    return {
        "file_type": "unknown",
        "confidence": "low",
        "reason": f"Could not determine file type. {num_cols} columns, {header_format} format",
        "header_format": header_format,
        "expected_columns": num_cols,
    }


def _detect_header_format(headers: list) -> str:
    if not headers:
        return "unknown"

    sample = [str(h).strip() for h in headers[:10] if h and str(h).strip()]
    if not sample:
        return "unknown"

    upper_count = sum(1 for h in sample if h == h.upper() and "_" in h)
    snake_count = sum(1 for h in sample if h == h.lower() and "_" in h)
    title_count = sum(1 for h in sample if h[0].isupper() and " " in h)

    if upper_count > len(sample) * 0.5:
        return "upper_case"
    if snake_count > len(sample) * 0.5:
        return "snake_case"
    if title_count > len(sample) * 0.3:
        return "title_case"

    if all(h == h.upper() for h in sample):
        return "upper_case"
    if all(h == h.lower() for h in sample):
        return "snake_case"

    return "title_case"


def _is_site_master(filename_lower: str, headers: list, header_format: str) -> bool:
    if any(kw in filename_lower for kw in ["naming", "site_class", "site_master"]):
        return True

    if header_format == "upper_case":
        headers_upper = [str(h).upper().strip() for h in headers]
        site_master_cols = {"SITE_ID", "SITE_NAME", "CLASS", "FLAG_SITE"}
        if site_master_cols.issubset(set(headers_upper)):
            return True

    return False
