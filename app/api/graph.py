from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any
import logging
from app.services.graph_service import graph_service
from app.services.graph_analytics import graph_analytics

router = APIRouter(prefix="/api/graph", tags=["Graph"])
logger = logging.getLogger(__name__)

# --- FETCH & STATS ---

@router.post("/fetch")
async def fetch_graph(payload: Dict[str, Any] = Body(...)):
    """Loads combined nodes and edges for the frontend map."""
    try:
        limit = payload.get("limit", 1000)
        filters = payload.get("filters", {})
        document_id = filters.get("document_id")
        
        if document_id:
            logger.info(f"Fetching graph for document: {document_id}")

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
        return {"results": [], "count": 0}
    
    results = await graph_service.search_nodes(query)
    return {"results": results, "count": len(results)}

@router.get("/stats")
async def graph_stats():
    return await graph_service.get_stats()

# --- ENTITY (NODE) MANAGEMENT ---

@router.post("/entity")
async def entity_crud(payload: Dict[str, Any] = Body(...)):
    """Unified controller for adding, changing, or removing nodes."""
    try:
        action = payload.get("action")
        data = payload.get("data", payload)
        
        if action == "create":
            # --- ROBUST FIX FOR CREATION ---
            # Ensure 'normType' and 'documentId' are preserved correctly
            properties = data.get("properties", {}).copy()
            
            # 1. Persist Type: Copy 'type' to 'normType' immediately
            if "type" in data and data["type"]:
                properties["normType"] = data["type"]
            
            # 2. Assign Properties back to data
            data["properties"] = properties
            
            # 3. Create
            await graph_service.add_entities([data])
            return {"status": "success", "message": "Entity created"}
        
        elif action == "update":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID required")
            
            # --- FIX: PERSIST TYPE CHANGE ---
            # If the user changed the type in UI, we save it as 'normType' 
            # because Gremlin Labels are immutable.
            properties = data.get("properties", {}).copy()
            if "type" in data and data["type"]:
                properties["normType"] = data["type"]
            
            data["properties"] = properties
            
            await graph_service.update_entity(entity_id, data["properties"])
            return {"status": "success", "message": "Entity updated"}
        
        elif action == "delete":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID required")
            await graph_service.delete_entity(entity_id)
            return {"status": "success", "message": f"Entity {entity_id} deleted"}
        
        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")
    except Exception as e:
        logger.error(f"Entity CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- RELATIONSHIP (EDGE) MANAGEMENT ---

@router.post("/relationship")
async def relationship_crud(payload: Dict[str, Any] = Body(...)):
    """Unified controller for edges."""
    try:
        action = payload.get("action")
        data = payload.get("data", payload)

        if action == "create":
            # Validates that we can link ANY entities (including Files)
            # Files are just nodes with type="Document", so this works natively.
            await graph_service.add_relationship(
                from_id=data.get("source") or data.get("from"),
                to_id=data.get("target") or data.get("to"),
                rel_type=data.get("label") or data.get("type"),
                properties=data.get("properties", {})
            )
            return {"status": "success", "message": "Relationship created"}
        
        elif action == "update":
            rel_id = data.get("id")
            if not rel_id:
                raise HTTPException(status_code=400, detail="Relationship ID required")
            
            props = data.get("properties", data)
            await graph_service.update_relationship(rel_id, props)
            return {"status": "success", "message": "Relationship updated"}

        elif action == "delete":
            rel_id = data.get("id")
            if not rel_id:
                raise HTTPException(status_code=400, detail="Relationship ID required")
            
            await graph_service.delete_relationship(rel_id)
            return {"status": "success", "message": "Relationship removed"}

        raise HTTPException(status_code=400, detail=f"Unknown action: {action}")
    except Exception as e:
        logger.error(f"Relationship CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- ANALYTICS & DOCUMENTS ---

@router.post("/analyze")
async def analyze_graph(payload: Dict[str, Any] = Body(...)):
    analysis_type = payload.get("type")
    params = payload.get("params", {})

    if analysis_type == "shortest_path":
        result = await graph_analytics.find_shortest_path(
            source_id=params.get("source"), 
            target_id=params.get("target")
        )
        return {"result": result}
    
    elif analysis_type == "community_detection":
        result = await graph_analytics.detect_communities()
        return {"result": result}

    raise HTTPException(status_code=400, detail="Unsupported analysis type")

@router.post("/document")
async def delete_document_data(payload: Dict[str, Any] = Body(...)):
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")
    
    count = await graph_service.delete_document_data(filename)
    return {"status": "deleted", "filename": filename, "nodes_removed": count}