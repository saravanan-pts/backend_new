from fastapi import APIRouter, Body, HTTPException
from app.services.graph_service import graph_service

router = APIRouter()

@router.post("/")
async def clear_graph(scope: str = Body(default="all", embed=True)):
    """
    Clear graph data.
    Scope: all | documents | entities | relationships
    """
    if scope not in {"all", "documents", "entities", "relationships"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid scope value",
        )

    result = graph_service.clear_graph(scope)
    return {
        "status": "ok",
        "cleared": result,
    }
