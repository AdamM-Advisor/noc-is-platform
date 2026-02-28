import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from backend.database import get_connection
from backend.services.saved_views_service import (
    list_saved_views, get_saved_view, get_saved_view_with_delta,
    create_saved_view, update_saved_view, delete_saved_view,
    record_access, toggle_pin, reorder_pinned,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/saved-views", tags=["saved-views"])


class SavedViewCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    entity_level: str
    entity_id: str
    entity_name: Optional[str] = ""
    granularity: Optional[str] = "monthly"
    date_from: Optional[str] = ""
    date_to: Optional[str] = ""
    type_ticket: Optional[str] = ""
    severities: Optional[list] = None
    fault_level: Optional[str] = ""
    rc_category: Optional[str] = ""
    snapshot_sla: Optional[float] = None
    snapshot_mttr: Optional[float] = None
    snapshot_volume: Optional[int] = None
    snapshot_escalation: Optional[float] = None
    snapshot_auto_resolve: Optional[float] = None
    snapshot_repeat: Optional[float] = None
    snapshot_behavior: Optional[str] = ""
    snapshot_status: Optional[str] = ""
    snapshot_risk_score: Optional[float] = None
    is_pinned: Optional[bool] = False
    url_params: Optional[str] = ""


class SavedViewUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    is_pinned: Optional[bool] = None
    sort_order: Optional[int] = None


class ReorderItem(BaseModel):
    id: int
    sort_order: int


class ReorderRequest(BaseModel):
    items: List[ReorderItem]


@router.get("")
async def list_views():
    with get_connection() as conn:
        views = list_saved_views(conn)
    return {"views": views, "total": len(views)}


@router.post("")
async def create_view(data: SavedViewCreate):
    with get_connection() as conn:
        result = create_saved_view(conn, data.model_dump())
    return result


@router.get("/{view_id}")
async def get_view(view_id: int):
    with get_connection() as conn:
        view = get_saved_view_with_delta(conn, view_id)
    if not view:
        raise HTTPException(404, "Saved view not found")
    return view


@router.put("/{view_id}")
async def update_view(view_id: int, data: SavedViewUpdate):
    with get_connection() as conn:
        result = update_saved_view(conn, view_id, data.model_dump(exclude_none=True))
    return result


@router.delete("/{view_id}")
async def delete_view(view_id: int):
    with get_connection() as conn:
        result = delete_saved_view(conn, view_id)
    return result


@router.post("/{view_id}/access")
async def access_view(view_id: int):
    with get_connection() as conn:
        result = record_access(conn, view_id)
    return result


@router.put("/{view_id}/pin")
async def pin_view(view_id: int):
    with get_connection() as conn:
        result = toggle_pin(conn, view_id)
    return result


@router.put("/reorder")
async def reorder_views(data: ReorderRequest):
    with get_connection() as conn:
        result = reorder_pinned(conn, [i.model_dump() for i in data.items])
    return result
