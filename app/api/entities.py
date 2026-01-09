from fastapi import APIRouter, Query
from typing import Optional

from app.services.graph_service import graph_service

router = APIRouter()

@router.get("/")
async def list_entities(
    label: Optional[str] = Query(default=None, description="Filter by entity label"),
):
    """
    Read-only endpoint to list entities.
    """
    entities = graph_service.get_entities(label=label)
    return entities
