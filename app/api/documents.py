from fastapi import APIRouter, HTTPException, Body
from typing import List, Dict, Any
import logging

from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/documents", tags=["Documents"])
logger = logging.getLogger(__name__)

@router.get("")
async def list_documents() -> List[Dict[str, Any]]:
    """
    Fetch all uploaded documents.
    """
    try:
        # Fetch all entities to filter for documents
        all_entities = await graph_service.get_entities()
        
        documents = []
        for entity in all_entities:
            # Flexible checking for "Document" type
            e_type = str(entity.get("type", "")).lower()
            label = str(entity.get("label", "")).lower()
            props = entity.get("properties", {})
            norm_type = str(props.get("normType", "")).lower()
            
            # Check if this node represents a file
            if "document" in e_type or "document" in label or "document" in norm_type:
                documents.append({
                    "id": entity.get("id"),
                    "filename": props.get("filename", entity.get("label")), # Fallback to label
                    "entityCount": props.get("nodeCount", 0),
                    "uploadDate": props.get("uploadDate", ""),
                    "status": props.get("status", "processed")
                })
        
        return documents

    except Exception as e:
        logger.error(f"Error listing docs: {e}")
        return []

@router.delete("")
async def delete_document(payload: Dict[str, Any] = Body(...)):
    """
    Delete a document AND all its associated entities.
    """
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename required")
        
    logger.info(f"Deleting document: {filename}")
    
    # 1. Fetch ALL nodes to find children
    all_nodes = await graph_service.get_entities()
    ids_to_delete = []
    
    for node in all_nodes:
        node_id = node.get("id")
        props = node.get("properties", {})
        
        # Check if node is the doc itself OR belongs to it
        if node_id == filename:
            ids_to_delete.append(node_id)
        elif props.get("documentId") == filename:
            ids_to_delete.append(node_id)
    
    # 2. Delete them
    for nid in ids_to_delete:
        await graph_service.repo.delete_entity(nid)
    
    return {"status": "success", "deleted_count": len(ids_to_delete)}