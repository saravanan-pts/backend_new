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

                raw_filename = props.get("filename", entity.get("label"))
                if raw_filename and '.' in raw_filename:
                    display_filename = raw_filename.rsplit('.', 1)[0]
                else:
                    display_filename = raw_filename
                
                # The documentId used for filtering entities
                filter_id = props.get("documentId", display_filename)
                
                # Unwrap list if necessary for filter_id
                if isinstance(filter_id, list) and len(filter_id) > 0:
                    filter_id = filter_id[0]

                documents.append({
                    "id": filter_id,
                    "filename": display_filename,
                    "entityCount": final_nodes,
                    "relationCount": final_edges,
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
    
    await graph_service.repo.delete_data_by_filename(filename)
    
    return {"status": "success", "deleted": filename}