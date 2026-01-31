from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any, List
import logging

# Import services
from app.services.graph_service import graph_service
from app.services.graph_analytics import graph_analytics

router = APIRouter(prefix="/api/graph", tags=["Graph"])
logger = logging.getLogger(__name__)

# ==========================================
# 1. FETCH & SEARCH OPERATIONS
# ==========================================

@router.post("/fetch")
async def fetch_graph(payload: Dict[str, Any] = Body(...)):
    """
    Loads combined nodes and edges for the frontend map.
    FIX: Now correctly reads 'documentId' from either the root payload OR nested filters.
    """
    try:
        limit = payload.get("limit", 1000)
        filters = payload.get("filters", {})
        
        # --- CRITICAL FIX FOR DROPDOWN ---
        # The frontend might send 'documentId' in the root payload OR inside 'filters'.
        # We check all possible locations to ensure filtering works.
        document_id = (
            payload.get("documentId") or 
            payload.get("document_id") or 
            filters.get("document_id")
        )
        
        if document_id:
            logger.info(f"Fetching graph for specific document: {document_id}")
        else:
            logger.info(f"Fetching entire graph (Limit: {limit})")

        # Execute the robust query in the repository
        result = await graph_service.repo.fetch_combined_graph(
            limit=limit,
            types=filters.get("types"),
            document_id=document_id
        )
        return result

    except Exception as e:
        logger.error(f"Fetch Graph Error: {e}")
        # Return empty structure instead of 500 error to keep Frontend alive
        return {"nodes": [], "edges": [], "meta": {"count": {"nodes": 0, "edges": 0}}}


@router.post("/search")
async def search_graph(payload: Dict[str, Any] = Body(...)):
    """Highlights specific nodes using a keyword search."""
    try:
        query = payload.get("query")
        if not query:
            return {"results": [], "count": 0}
        
        results = await graph_service.search_nodes(query)
        return {"results": results, "count": len(results)}
    except Exception as e:
        logger.error(f"Search Error: {e}")
        return {"results": [], "count": 0, "error": str(e)}


@router.get("/stats")
async def graph_stats():
    """Returns total count of nodes and edges."""
    try:
        return await graph_service.get_stats()
    except Exception as e:
        logger.error(f"Stats Error: {e}")
        return {"nodes": 0, "edges": 0}


# ==========================================
# 2. ENTITY (NODE) MANAGEMENT
# ==========================================

@router.post("/entity")
async def entity_crud(payload: Dict[str, Any] = Body(...)):
    """
    Unified controller for adding, updating, or deleting nodes.
    Preserves 'normType' logic for UI compatibility.
    """
    try:
        action = payload.get("action")
        data = payload.get("data", payload)
        
        # --- CREATE ---
        if action == "create":
            # Logic: Ensure 'type' is saved as 'normType' property
            properties = data.get("properties", {}).copy()
            
            # Check 'type' in root data or properties
            node_type = data.get("type") or properties.get("type")
            if node_type:
                properties["normType"] = node_type
            
            # Apply changes back to data
            data["properties"] = properties
            
            await graph_service.add_entities([data])
            return {"status": "success", "message": "Entity created successfully"}
        
        # --- UPDATE ---
        elif action == "update":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID is required for update")
            
            # Persist type change if user edited it
            properties = data.get("properties", {}).copy()
            node_type = data.get("type") or properties.get("type")
            if node_type:
                properties["normType"] = node_type
            
            await graph_service.update_entity(entity_id, properties)
            return {"status": "success", "message": "Entity updated successfully"}
        
        # --- DELETE ---
        elif action == "delete":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID is required for delete")
            
            await graph_service.delete_entity(entity_id)
            return {"status": "success", "message": f"Entity {entity_id} deleted"}
        
        else:
            raise HTTPException(status_code=400, detail=f"Unknown entity action: {action}")

    except Exception as e:
        logger.error(f"Entity CRUD Error: {e}")
        # Return 500 so frontend knows the save failed
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 3. RELATIONSHIP (EDGE) MANAGEMENT
# ==========================================

@router.post("/relationship")
async def relationship_crud(payload: Dict[str, Any] = Body(...)):
    """Unified controller for creating/updating/deleting edges."""
    try:
        action = payload.get("action")
        data = payload.get("data", payload)

        # --- CREATE ---
        if action == "create":
            # Handle aliases (source/from, target/to, label/type) - CRITICAL FOR FRONTEND COMPATIBILITY
            source_id = data.get("source") or data.get("from")
            target_id = data.get("target") or data.get("to")
            rel_label = data.get("label") or data.get("type") or "related_to"

            if not source_id or not target_id:
                raise HTTPException(status_code=400, detail="Source and Target IDs are required")

            await graph_service.add_relationship(
                from_id=source_id,
                to_id=target_id,
                rel_type=rel_label,
                properties=data.get("properties", {})
            )
            return {"status": "success", "message": "Relationship created"}
        
        # --- UPDATE ---
        elif action == "update":
            rel_id = data.get("id")
            if not rel_id:
                raise HTTPException(status_code=400, detail="Relationship ID required")
            
            props = data.get("properties", data)
            await graph_service.update_relationship(rel_id, props)
            return {"status": "success", "message": "Relationship updated"}

        # --- DELETE ---
        elif action == "delete":
            rel_id = data.get("id")
            if not rel_id:
                raise HTTPException(status_code=400, detail="Relationship ID required")
            
            await graph_service.delete_relationship(rel_id)
            return {"status": "success", "message": "Relationship removed"}

        else:
            raise HTTPException(status_code=400, detail=f"Unknown relationship action: {action}")

    except Exception as e:
        logger.error(f"Relationship CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 4. ANALYTICS & DOCUMENTS
# ==========================================

@router.post("/analyze")
async def analyze_graph(payload: Dict[str, Any] = Body(...)):
    """Runs graph algorithms like Shortest Path or Community Detection."""
    try:
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

        raise HTTPException(status_code=400, detail=f"Unsupported analysis type: {analysis_type}")

    except Exception as e:
        logger.error(f"Analysis Error: {e}")
        # Return error inside JSON so UI can display it
        return {"result": None, "error": str(e)}


@router.post("/document")
async def delete_document_data(payload: Dict[str, Any] = Body(...)):
    """Deletes all nodes and edges associated with a specific file."""
    try:
        filename = payload.get("filename")
        if not filename:
            raise HTTPException(status_code=400, detail="Filename is required")
        
        logger.info(f"Request received to delete document: {filename}")
        count = await graph_service.delete_document_data(filename)
        
        return {
            "status": "deleted", 
            "filename": filename, 
            "nodes_removed": count
        }
    except Exception as e:
        logger.error(f"Document Delete Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))