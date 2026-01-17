from fastapi import APIRouter, HTTPException, Body
from typing import List, Dict, Any
import logging

from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/documents", tags=["Documents"])
logger = logging.getLogger(__name__)

@router.get("")
async def list_documents() -> List[Dict[str, Any]]:
    """
    Fetch all uploaded documents with correct node and edge counts.
    """
    try:
        # Fetch all entities to filter for documents
        all_entities = await graph_service.get_entities()
        
        documents = []
        for entity in all_entities:
            # 1. Flexible checking to find Document nodes
            e_type = str(entity.get("type", "")).lower()
            label = str(entity.get("label", "")).lower()
            props = entity.get("properties", {})
            norm_type = str(props.get("normType", "")).lower()
            
            # Check if this node represents a file
            if "document" in e_type or "document" in label or "document" in norm_type:
                
                # --- FIX: ROBUST COUNT EXTRACTION ---
                # Cosmos DB often returns properties as lists (e.g. ['94'])
                raw_node_count = props.get("nodeCount", 0)
                raw_edge_count = props.get("edgeCount", 0)
                
                # Unwrap list if necessary
                if isinstance(raw_node_count, list) and len(raw_node_count) > 0:
                    raw_node_count = raw_node_count[0]
                if isinstance(raw_edge_count, list) and len(raw_edge_count) > 0:
                    raw_edge_count = raw_edge_count[0]
                
                # Convert to integer safely
                try:
                    final_nodes = int(float(str(raw_node_count)))
                except (ValueError, TypeError):
                    final_nodes = 0

                try:
                    final_edges = int(float(str(raw_edge_count)))
                except (ValueError, TypeError):
                    final_edges = 0
                # ------------------------------------

                documents.append({
                    "id": entity.get("id"),
                    "filename": props.get("filename", entity.get("label")), 
                    "entityCount": final_nodes,
                    "relationCount": final_edges, # Returning edge count as well
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
    # (Optimization: Ideally, the repository would support delete_by_property directly)
    all_nodes = await graph_service.get_entities()
    ids_to_delete = []
    
    for node in all_nodes:
        node_id = node.get("id")
        props = node.get("properties", {})
        
        # Check if node is the doc itself OR belongs to it (via documentId tag)
        if node_id == filename:
            ids_to_delete.append(node_id)
        elif props.get("documentId") == filename:
            ids_to_delete.append(node_id)
    
    # 2. Delete them
    for nid in ids_to_delete:
        await graph_service.repo.delete_entity(nid)
    
    return {"status": "success", "deleted_count": len(ids_to_delete)}