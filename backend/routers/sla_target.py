from fastapi import APIRouter, HTTPException
from backend.database import get_connection, get_write_connection

router = APIRouter(prefix="/master/sla-target")


@router.get("")
async def list_sla_targets():
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM master_sla_target ORDER BY priority ASC, id ASC"
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            items = [dict(zip(columns, row)) for row in rows]
        return {"items": items, "total": len(items)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("")
async def create_sla_target(data: dict):
    required = ["sla_target_pct"]
    for field in required:
        if field not in data:
            raise HTTPException(status_code=400, detail=f"{field} is required")

    try:
        with get_write_connection() as conn:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM master_sla_target").fetchone()[0]

            conn.execute("""
                INSERT INTO master_sla_target
                (id, area_id, regional_id, site_class, site_flag, severity,
                 sla_target_pct, mttr_target_min, response_target_min,
                 priority, description, effective_from, effective_to)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                max_id,
                data.get("area_id", "*"),
                data.get("regional_id", "*"),
                data.get("site_class", "*"),
                data.get("site_flag", "*"),
                data.get("severity", "*"),
                data["sla_target_pct"],
                data.get("mttr_target_min"),
                data.get("response_target_min"),
                data.get("priority", 0),
                data.get("description"),
                data.get("effective_from"),
                data.get("effective_to"),
            ])

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM master_sla_target WHERE id = ?", [max_id]).fetchone()
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{rule_id}")
async def update_sla_target(rule_id: int, data: dict):
    try:
        with get_write_connection() as conn:
            existing = conn.execute(
                "SELECT * FROM master_sla_target WHERE id = ?", [rule_id]
            ).fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="SLA target rule not found")

            set_clauses = []
            values = []
            allowed = [
                "area_id", "regional_id", "site_class", "site_flag", "severity",
                "sla_target_pct", "mttr_target_min", "response_target_min",
                "priority", "description", "effective_from", "effective_to",
            ]
            for field in allowed:
                if field in data:
                    set_clauses.append(f"{field} = ?")
                    values.append(data[field])

            if not set_clauses:
                raise HTTPException(status_code=400, detail="No fields to update")

            set_clauses.append("updated_at = CURRENT_TIMESTAMP")
            values.append(rule_id)

            conn.execute(
                f"UPDATE master_sla_target SET {', '.join(set_clauses)} WHERE id = ?",
                values,
            )

        with get_connection() as conn:
            row = conn.execute("SELECT * FROM master_sla_target WHERE id = ?", [rule_id]).fetchone()
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, row))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{rule_id}")
async def delete_sla_target(rule_id: int):
    try:
        with get_write_connection() as conn:
            row = conn.execute(
                "SELECT priority FROM master_sla_target WHERE id = ?", [rule_id]
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="SLA target rule not found")

            if row[0] == 0:
                raise HTTPException(status_code=400, detail="Cannot delete the default rule (priority=0)")

            conn.execute("DELETE FROM master_sla_target WHERE id = ?", [rule_id])

        return {"status": "deleted", "id": rule_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resolve")
async def resolve_sla_target(
    site_class: str = None,
    site_flag: str = None,
    area_id: str = None,
    regional_id: str = None,
    severity: str = None,
):
    try:
        with get_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM master_sla_target ORDER BY priority DESC, id ASC"
            ).fetchall()
            columns = [desc[0] for desc in conn.description]
            all_targets = [dict(zip(columns, row)) for row in rows]

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

        matched = [t for t in all_targets if matches(t)]
        matched.sort(key=lambda x: x.get("priority", 0), reverse=True)

        resolved = matched[0] if matched else None

        priority_chain = []
        for t in matched:
            priority_chain.append({
                "id": t["id"],
                "priority": t["priority"],
                "description": t["description"],
                "sla_target_pct": t["sla_target_pct"],
                "mttr_target_min": t.get("mttr_target_min"),
                "match_criteria": {
                    "area_id": t["area_id"],
                    "regional_id": t["regional_id"],
                    "site_class": t["site_class"],
                    "site_flag": t["site_flag"],
                    "severity": t["severity"],
                },
            })

        return {
            "resolved": resolved,
            "priority_chain": priority_chain,
            "params": {
                "site_class": site_class,
                "site_flag": site_flag,
                "area_id": area_id,
                "regional_id": regional_id,
                "severity": severity,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{rule_id}/impact")
async def get_sla_target_impact(rule_id: int):
    try:
        with get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM master_sla_target WHERE id = ?", [rule_id]
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="SLA target rule not found")

            columns = [desc[0] for desc in conn.description]
            rule = dict(zip(columns, row))

            conditions = []
            params = []

            if rule["site_class"] != "*":
                conditions.append("s.site_class = ?")
                params.append(rule["site_class"])

            if rule["site_flag"] != "*":
                conditions.append("s.site_flag = ?")
                params.append(rule["site_flag"])

            if rule["area_id"] != "*":
                conditions.append("h.area_id = ?")
                params.append(rule["area_id"])

            if rule["regional_id"] != "*":
                conditions.append("h.regional_id = ?")
                params.append(rule["regional_id"])

            where = " AND ".join(conditions) if conditions else "1=1"

            try:
                count = conn.execute(f"""
                    SELECT COUNT(*)
                    FROM master_site s
                    LEFT JOIN v_hierarchy h ON s.to_id = h.to_id
                    WHERE s.status = 'ACTIVE' AND {where}
                """, params).fetchone()[0]
            except Exception:
                count = 0

        return {
            "rule_id": rule_id,
            "affected_sites": count,
            "rule": rule,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
