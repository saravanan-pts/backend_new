from fastapi import APIRouter, Query
from app.services.graph_service import graph_service

router = APIRouter()

@router.get("/")
async def list_relationships(
    entity_id: str = Query(..., description="Entity ID to fetch relationships for"),
):
    """
    Read-only endpoint to fetch relationships for an entity.
    """
    relationships = graph_service.get_relationships_for_entity(entity_id)
    return relationships
