from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any, List
import logging
from app.services.graph_service import graph_service
# Note: This service handles complex math like shortest paths and community detection
from app.services.graph_analytics import graph_analytics

router = APIRouter(prefix="/api/graph", tags=["Graph"])
logger = logging.getLogger(__name__)

@router.post("/fetch")
async def fetch_graph(payload: Dict[str, Any] = Body(...)):
    """Loads combined nodes and edges for the frontend map."""
    try:
        limit = payload.get("limit", 1000)
        filters = payload.get("filters", {})
        document_id = filters.get("document_id")
        
        # Log if we are filtering
        if document_id:
            logger.info(f"Fetching graph for document: {document_id}")

        # Uses the 'brain' skills added to the repository to fetch nodes and edges together
        # We continue to use the Repo method here because it handles the combined logic perfectly
        return await graph_service.repo.fetch_combined_graph(
            limit=limit,
            types=filters.get("types"),
            document_id=document_id
        )
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return {"nodes": [], "edges": []}

@router.post("/search")
async def search_graph(payload: Dict[str, Any] = Body(...)):
    """Highlights specific bricks in the city using a keyword."""
    query = payload.get("query")
    if not query:
        # Return empty list instead of error for smoother UI
        return {"results": [], "count": 0}
    
    # Searches nodes based on labels or property values containing the keyword
    results = await graph_service.search_nodes(query)
    return {"results": results, "count": len(results)}

@router.get("/stats")
async def graph_stats():
    """Returns the city 'Report Card' showing total counts."""
    # Aggregates total node/edge counts and counts per type for the dashboard
    return await graph_service.get_stats()

@router.post("/entity")
async def entity_crud(payload: Dict[str, Any] = Body(...)):
    """Unified controller for adding, changing, or removing Lego houses (nodes)."""
    try:
        action = payload.get("action")
        # Support both wrapped "data" and direct payload for flexibility
        data = payload.get("data", payload)
        
        if action == "create":
            # Wraps logic to add a new vertex with the UPSERT pattern
            await graph_service.add_entities([data])
            return {"status": "success", "message": "Entity created"}
        
        elif action == "update":
            # Uses the UPSERT logic in the repository to update properties without duplication
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID required")
            await graph_service.update_entity(entity_id, data)
            return {"status": "success", "message": "Entity updated"}
        
        elif action == "delete":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID required for deletion")
            # Removes the vertex and all its connected edges from the graph
            await graph_service.delete_entity(entity_id)
            return {"status": "success", "message": f"Entity {entity_id} deleted"}
        
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")
    except Exception as e:
        logger.error(f"Entity CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/relationship")
async def relationship_crud(payload: Dict[str, Any] = Body(...)):
    """Unified controller for building or removing roads (edges)."""
    try:
        action = payload.get("action")
        data = payload.get("data", payload)

        if action == "create":
            # Re-maps frontend 'source/target' terminology to backend 'from/to' labels
            await graph_service.add_relationship(
                from_id=data.get("source") or data.get("from"),
                to_id=data.get("target") or data.get("to"),
                rel_type=data.get("label") or data.get("type"),
                properties=data.get("properties", {})
            )
            return {"status": "success", "message": "Relationship created"}
        
        # --- FIX: ADDED UPDATE LOGIC ---
        elif action == "update":
            rel_id = data.get("id")
            if not rel_id:
                raise HTTPException(status_code=400, detail="Relationship ID required")
            
            # Extract properties to update
            props = data.get("properties", data)
            await graph_service.update_relationship(rel_id, props)
            return {"status": "success", "message": "Relationship updated"}

        # --- FIX: IMPLEMENTED DELETE LOGIC ---
        elif action == "delete":
            rel_id = data.get("id")
            if not rel_id:
                # If ID is missing, we can't delete a specific edge easily
                raise HTTPException(status_code=400, detail="Relationship ID required for deletion")
            
            await graph_service.delete_relationship(rel_id)
            return {"status": "success", "message": "Relationship removed"}

        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")
    except Exception as e:
        logger.error(f"Relationship CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze")
async def analyze_graph(payload: Dict[str, Any] = Body(...)):
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
async def delete_document_data(payload: Dict[str, Any] = Body(...)):
    """Removes all bricks associated with a specific instruction book."""
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    
    # --- FIX: Use Robust Service Method ---
    # Deletes all nodes and edges tagged with the specified documentId using the loop method
    count = await graph_service.delete_document_data(filename)
    
    return {"status": "deleted", "filename": filename, "nodes_removed": count}