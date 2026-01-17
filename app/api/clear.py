from fastapi import APIRouter, Body, HTTPException
from app.services.graph_service import graph_service

# Remove inner prefix so main.py controls the URL (e.g., POST /clear)
router = APIRouter(tags=["Admin"])

@router.post("") 
async def clear_graph(scope: str = Body(default="all", embed=True)):
    """
    Clear graph data.
    URL: POST /clear
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
        # We await because graph_service.clear_graph is async
        result = await graph_service.clear_graph(scope)
        
        return {
            "status": "ok",
            "cleared": result,
            "scope": scope
        }
    except Exception as e:
        # 3. Error Handling
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to clear graph data: {str(e)}"
        )