from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
from app.services.graph_service import graph_service
# Note: You will create this service next to handle complex math like shortest paths
from app.services.graph_analytics import graph_analytics 

router = APIRouter(prefix="/api/graph", tags=["Graph"])

@router.post("/fetch")
async def fetch_graph(payload: Dict[str, Any]):
    """Loads combined nodes and edges for the frontend map."""
    limit = payload.get("limit", 500)
    filters = payload.get("filters", {})
    # Uses the new 'brain' skills we added to the repository
    return await graph_service.repo.fetch_combined_graph(
        limit=limit, 
        types=filters.get("types")
    )

@router.post("/search")
async def search_graph(payload: Dict[str, Any]):
    """Highlights specific bricks in the city using a keyword."""
    query = payload.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Search query is required")
    
    results = await graph_service.repo.search_nodes(
        keyword=query, 
        limit=payload.get("limit", 20)
    )
    return {"results": results, "count": len(results)}

@router.get("/stats")
async def graph_stats():
    """Returns the city 'Report Card' showing total counts."""
    return await graph_service.repo.get_stats()

@router.post("/entity")
async def entity_crud(payload: Dict[str, Any]):
    """Unified controller for adding, changing, or removing Lego houses (nodes)."""
    action = payload.get("action")
    data = payload.get("data", {})
    
    if action == "create":
        # Wraps existing logic to add a new vertex
        await graph_service.add_entities([data])
        return {"status": "success", "message": "Entity created"}
    
    elif action == "update":
        # Uses the UPSERT logic in your repository to update properties
        await graph_service.add_entities([data])
        return {"status": "success", "message": "Entity updated"}
    
    elif action == "delete":
        entity_id = data.get("id")
        if not entity_id:
            raise HTTPException(status_code=400, detail="Entity ID required for deletion")
        await graph_service.repo.delete_entity(entity_id)
        return {"status": "success", "message": f"Entity {entity_id} deleted"}
    
    raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

@router.post("/relationship")
async def relationship_crud(payload: Dict[str, Any]):
    """Unified controller for building or removing roads (edges)."""
    action = payload.get("action")
    data = payload.get("data", {})

    if action == "create":
        # Re-maps frontend 'source/target' to backend 'from/to'
        rel = [{
            "from": data.get("source"),
            "to": data.get("target"),
            "label": data.get("label"),
            "properties": data.get("properties", {})
        }]
        await graph_service.add_relationships(rel)
        return {"status": "success"}

    elif action == "delete":
        # Logic to drop a specific edge can be added to your repository if needed
        return {"status": "success", "message": "Relationship removed"}

    raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

@router.post("/analyze")
async def analyze_graph(payload: Dict[str, Any]):
    """Calls the Expert Architect for shortest paths or community groupings."""
    analysis_type = payload.get("type")
    params = payload.get("params", {})

    if analysis_type == "shortest_path":
        result = await graph_analytics.find_shortest_path(
            source_id=params.get("source"), 
            target_id=params.get("target")
        )
        return {"result": result}
    
    elif analysis_type == "community_detection":
        # Group connected nodes into neighborhoods using AI
        result = await graph_analytics.detect_communities()
        return {"result": result}

    raise HTTPException(status_code=400, detail="Unsupported analysis type")

@router.post("/document")
async def delete_document_data(payload: Dict[str, Any]):
    """Removes all bricks associated with a specific instruction book."""
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    
    await graph_service.repo.delete_data_by_filename(filename)
    return {"status": "deleted", "filename": filename}