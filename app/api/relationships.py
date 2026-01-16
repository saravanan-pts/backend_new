from fastapi import APIRouter, Query, HTTPException
from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/relationships", tags=["Relationships"])

@router.get("/")
async def list_relationships(
    entity_id: str = Query(..., description="Entity ID to fetch relationships for"),
):
    """
    Read-only endpoint to fetch relationships for an entity.
    Fixed: Added await and error handling for the async transition.
    """
    try:
        # We must 'await' here because the service method is now asynchronous
        relationships = await graph_service.get_relationships_for_entity(entity_id)
        return relationships
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to fetch relationships for entity {entity_id}: {str(e)}"
        )