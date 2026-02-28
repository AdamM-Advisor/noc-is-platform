from backend.database import get_connection


def get_sla_target(site_class, site_flag, area_id=None, regional_id=None, severity=None):
    with get_connection() as conn:
        try:
            rows = conn.execute("SELECT * FROM master_sla_target").fetchall()
            columns = [desc[0] for desc in conn.description]
        except Exception:
            return None

    targets = [dict(zip(columns, row)) for row in rows]

    def matches(target):
        if target["area_id"] != "*":
            if area_id is None or target["area_id"] != area_id:
                return False
        if target["regional_id"] != "*":
            if regional_id is None or target["regional_id"] != regional_id:
                return False
        if target["site_class"] != "*":
            if site_class is None or target["site_class"] != site_class:
                return False
        if target["site_flag"] != "*":
            if site_flag is None or target["site_flag"] != site_flag:
                return False
        if target["severity"] != "*":
            if severity is None or target["severity"] != severity:
                return False
        return True

    matched = [t for t in targets if matches(t)]

    if not matched:
        return None

    matched.sort(key=lambda x: x.get("priority", 0), reverse=True)
    return matched[0]
