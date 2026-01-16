from fastapi import APIRouter, Body, HTTPException
from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/graph", tags=["Admin"])

@router.post("/clear")
async def clear_graph(scope: str = Body(default="all", embed=True)):
    """
    Clear graph data.
    Scope: all | documents | entities | relationships
    
    FIX: Fully async to prevent 'RuntimeError: Cannot run the event loop while another loop is running'
    """
    # 1. Validation Logic
    valid_scopes = {"all", "documents", "entities", "relationships"}
    if scope not in valid_scopes:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scope value. Must be one of: {', '.join(valid_scopes)}",
        )

    try:
        # 2. Business Logic Execution
        # We MUST await this because graph_service.clear_graph is now an async function
        result = await graph_service.clear_graph(scope)
        
        return {
            "status": "ok",
            "cleared": result,
            "scope": scope
        }
    except Exception as e:
        # 3. Error Handling
        # Catches Gremlin connection issues or query failures
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to clear graph data: {str(e)}"
        )