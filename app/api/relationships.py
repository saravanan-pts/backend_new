from fastapi import APIRouter, Query, HTTPException
from app.services.graph_service import graph_service
import logging

router = APIRouter(prefix="/api/relationships", tags=["Relationships"])
logger = logging.getLogger(__name__)

@router.get("/")
async def list_relationships(
    entity_id: str = Query(..., description="Entity ID to fetch relationships for"),
):
    """
    Read-only endpoint to fetch relationships for an entity.
    If the entity has been analyzed, edges will contain 'riskCategory' (Cause/Effect).
    """
    try:
        # Check if service is initialized
        if not graph_service.repo.client:
             raise HTTPException(status_code=503, detail="Database connection not available")

        # Fetch relationships (Await the service call)
        relationships = await graph_service.get_relationships_for_entity(entity_id)
        
        if not relationships:
            return []
            
        return relationships

    except Exception as e:
        logger.error(f"Error fetching relationships for {entity_id}: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to fetch relationships for entity {entity_id}: {str(e)}"
        )