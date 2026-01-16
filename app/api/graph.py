from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
from app.services.graph_service import graph_service
# Note: This service handles complex math like shortest paths and community detection
from app.services.graph_analytics import graph_analytics

router = APIRouter(prefix="/api/graph", tags=["Graph"])

@router.post("/fetch")
async def fetch_graph(payload: Dict[str, Any]):
    """Loads combined nodes and edges for the frontend map."""
    limit = payload.get("limit", 500)
    filters = payload.get("filters", {})
    # Uses the 'brain' skills added to the repository to fetch nodes and edges together
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
    
    # Searches nodes based on labels or property values containing the keyword
    results = await graph_service.repo.search_nodes(
        keyword=query, 
        limit=payload.get("limit", 20)
    )
    return {"results": results, "count": len(results)}

@router.get("/stats")
async def graph_stats():
    """Returns the city 'Report Card' showing total counts."""
    # Aggregates total node/edge counts and counts per type for the dashboard
    return await graph_service.repo.get_stats()

@router.post("/entity")
async def entity_crud(payload: Dict[str, Any]):
    """Unified controller for adding, changing, or removing Lego houses (nodes)."""
    action = payload.get("action")
    data = payload.get("data", {})
    
    if action == "create":
        # Wraps logic to add a new vertex with the UPSERT pattern
        await graph_service.add_entities([data])
        return {"status": "success", "message": "Entity created"}
    
    elif action == "update":
        # Uses the UPSERT logic in the repository to update properties without duplication
        await graph_service.add_entities([data])
        return {"status": "success", "message": "Entity updated"}
    
    elif action == "delete":
        entity_id = data.get("id")
        if not entity_id:
            raise HTTPException(status_code=400, detail="Entity ID required for deletion")
        # Removes the vertex and all its connected edges from the graph
        await graph_service.repo.delete_entity(entity_id)
        return {"status": "success", "message": f"Entity {entity_id} deleted"}
    
    raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

@router.post("/relationship")
async def relationship_crud(payload: Dict[str, Any]):
    """Unified controller for building or removing roads (edges)."""
    action = payload.get("action")
    data = payload.get("data", {})

    if action == "create":
        # Re-maps frontend 'source/target' terminology to backend 'from/to' labels
        rel = [{
            "from": data.get("source"),
            "to": data.get("target"),
            "label": data.get("label"),
            "properties": data.get("properties", {})
        }]
        # Persists the relationship using the edge UPSERT pattern
        await graph_service.add_relationships(rel)
        return {"status": "success"}

    elif action == "delete":
        # Placeholder for specific edge removal logic if needed in the future
        return {"status": "success", "message": "Relationship removed"}

    raise HTTPException(status_code=400, detail=f"Unknown action: {action}")

@router.post("/analyze")
async def analyze_graph(payload: Dict[str, Any]):
    """Calls the Expert Architect for shortest paths or community groupings."""
    analysis_type = payload.get("type")
    params = payload.get("params", {})

    if analysis_type == "shortest_path":
        # Calculates the path between two vertices using Gremlin's path() step
        result = await graph_analytics.find_shortest_path(
            source_id=params.get("source"), 
            target_id=params.get("target")
        )
        return {"result": result}
    
    elif analysis_type == "community_detection":
        # Groups connected nodes into neighborhoods using connected components and AI summaries
        result = await graph_analytics.detect_communities()
        return {"result": result}

    raise HTTPException(status_code=400, detail="Unsupported analysis type")

@router.post("/document")
async def delete_document_data(payload: Dict[str, Any]):
    """Removes all bricks associated with a specific instruction book."""
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    
    # Deletes all nodes and edges tagged with the specified sourceDocumentId
    await graph_service.repo.delete_data_by_filename(filename)
    return {"status": "deleted", "filename": filename}