from fastapi import APIRouter, Query, HTTPException
from typing import Optional

from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/entities", tags=["Entities"])

@router.get("/")
async def list_entities(
    label: Optional[str] = Query(default=None, description="Filter by entity label"),
):
    """
    Read-only endpoint to list entities.
    Updated to support async execution to prevent 500 errors.
    """
    try:
        # Added 'await' here because the service method is now async
        entities = await graph_service.get_entities(label=label)
        return entities
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entities: {str(e)}")