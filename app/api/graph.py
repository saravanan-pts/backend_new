import uuid
import logging
from fastapi import APIRouter, HTTPException, Body
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional, Literal

# Import Configuration
from app.config import settings

# Import Services
from app.services.graph_service import graph_service
# Note: graph_analytics import removed as it was only used by the deleted analyze endpoint

router = APIRouter(prefix="/api/graph", tags=["Graph"])
logger = logging.getLogger(__name__)

# ==========================================
# 0. DATA MODELS
# ==========================================

class FetchPayload(BaseModel):
    limit: int = 2000 #change value here for number of nodes while loading
    filters: Dict[str, Any] = {}
    documentId: Optional[str] = None
    document_id: Optional[str] = None

class SearchPayload(BaseModel):
    query: str

class EntityPayload(BaseModel):
    action: Literal["create", "update", "delete"] = Field(..., description="Action to perform")
    data: Dict[str, Any] = Field(..., description="Entity data (id, label, properties)")
    documentId: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "action": "create",
                "documentId": "accounts_lifecycle_log.csv",
                "data": {
                    "label": "Person",
                    "type": "User",
                    "properties": {
                        "name": "Janani",
                        "role": "Admin",
                        "score": 100
                    }
                }
            }
        }

class RelationshipPayload(BaseModel):
    action: Literal["create", "update", "delete"] = Field(..., description="Action to perform")
    data: Dict[str, Any] = Field(..., description="Relationship data (source, target, label)")
    documentId: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "action": "create",
                "documentId": "accounts_lifecycle_log.csv",
                "data": {
                    "source": "uuid-of-person-node",
                    "target": "uuid-of-account-node",
                    "label": "OWNS",
                    "properties": {
                        "since": "2024-01-01"
                    }
                }
            }
        }

class DocumentPayload(BaseModel):
    filename: str

# ==========================================
# 1. FETCH & SEARCH OPERATIONS
# ==========================================

@router.post("/fetch")
async def fetch_graph(payload: FetchPayload):
    """
    Loads combined nodes and edges for the frontend map.
    Robustly reads 'documentId' from root, snake_case field, or nested filters.
    """
    try:
        # Check all possible locations for document_id to support various UI calls
        doc_id = (
            payload.documentId or 
            payload.document_id or 
            payload.filters.get("document_id")
        )
        
        if doc_id:
            logger.info(f"Fetching graph for specific document: {doc_id}")
        else:
            logger.info(f"Fetching entire graph (Limit: {payload.limit})")

        # Execute query via repository
        result = await graph_service.repo.fetch_combined_graph(
            limit=payload.limit,
            types=payload.filters.get("types"),
            document_id=doc_id
        )
        return result

    except Exception as e:
        logger.error(f"Fetch Graph Error: {e}")
        # Return empty structure instead of 500 error to keep Frontend alive
        return {"nodes": [], "edges": [], "meta": {"count": {"nodes": 0, "edges": 0}}}


@router.post("/search")
async def search_graph(payload: SearchPayload):
    """Highlights specific nodes using a keyword search."""
    try:
        if not payload.query:
            return {"results": [], "count": 0}
        
        results = await graph_service.search_nodes(payload.query)
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
async def entity_crud(payload: EntityPayload):
    """
    Unified controller for adding, updating, or deleting nodes.
    Dynamically handles Partition Keys based on environment configuration.
    """
    try:
        action = payload.action
        data = payload.data
        
        # Retrieve the specific key name from .env (default to 'pk')
        pk_name = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")

        # --- CREATE ---
        if action == "create":
            # 1. Robust ID Generation: Prevent 500 errors on missing IDs
            if "id" not in data or not data["id"]:
                data["id"] = str(uuid.uuid4())

            # 2. Prepare Properties
            properties = data.get("properties", {}).copy()
            
            # --- DYNAMIC PARTITION KEY ---
            # If the partition key is missing in properties, force it to match ID.
            if pk_name not in properties:
                properties[pk_name] = data["id"]

            # 3. Auto-Tagging for Visibility
            if payload.documentId and "documentId" not in properties:
                properties["documentId"] = payload.documentId

            # 4. Type Normalization (UI 'type' -> DB 'normType')
            node_type = data.get("type") or properties.get("type")
            if node_type:
                properties["normType"] = node_type
            
            # Apply cleaned properties back to data object
            data["properties"] = properties
            
            # Ensure Partition Key is also at the root level if the service layer checks there
            data[pk_name] = properties[pk_name] 
            
            await graph_service.add_entities([data])
            
            return {
                "status": "success", 
                "message": "Entity created successfully",
                "id": data["id"],
                "partitionKey": pk_name 
            }
        
        # --- UPDATE ---
        elif action == "update":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID is required for update")
            
            # FIXED: Get Partition Key for Update
            # Try getting it from data root, or properties, or default to ID
            partition_key = data.get(pk_name) or data.get("partitionKey") or entity_id

            # Persist type change if user edited it
            properties = data.get("properties", {}).copy()
            node_type = data.get("type") or properties.get("type")
            if node_type:
                properties["normType"] = node_type
            
            # Pass partition_key to service
            await graph_service.update_entity(entity_id, properties, partition_key)
            return {"status": "success", "message": "Entity updated successfully"}
        
        # --- DELETE ---
        elif action == "delete":
            entity_id = data.get("id")
            if not entity_id:
                raise HTTPException(status_code=400, detail="Entity ID is required for delete")
            
            # FIXED: Get Partition Key for Delete
            partition_key = data.get(pk_name) or data.get("partitionKey") or entity_id
            
            # Pass partition_key to service
            await graph_service.delete_entity(entity_id, partition_key)
            return {"status": "success", "message": f"Entity {entity_id} deleted"}
        
        else:
            raise HTTPException(status_code=400, detail=f"Unknown entity action: {action}")

    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Entity CRUD Error: {e}")
        # Return 500 so frontend knows the save failed
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 3. RELATIONSHIP (EDGE) MANAGEMENT
# ==========================================

@router.post("/relationship")
async def relationship_crud(payload: RelationshipPayload):
    """Unified controller for creating/updating/deleting edges."""
    try:
        action = payload.action
        data = payload.data

        # --- CREATE ---
        if action == "create":
            # Handle aliases (source/from, target/to, label/type) for frontend compatibility
            source_id = data.get("source") or data.get("from")
            target_id = data.get("target") or data.get("to")
            rel_label = data.get("label") or data.get("type") or "related_to"

            if not source_id or not target_id:
                raise HTTPException(status_code=400, detail="Source and Target IDs are required")
            
            # Normalize properties
            properties = data.get("properties", {})
            
            # Auto-tag edge with document ID if available
            if payload.documentId:
                 properties["doc"] = payload.documentId

            await graph_service.add_relationship(
                from_id=source_id,
                to_id=target_id,
                rel_type=rel_label,
                properties=properties
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

    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Relationship CRUD Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 4. DOCUMENTS
# ==========================================

@router.post("/document")
async def delete_document_data(payload: DocumentPayload):
    """Deletes all nodes and edges associated with a specific file."""
    try:
        filename = payload.filename
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