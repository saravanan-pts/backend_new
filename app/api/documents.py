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
    CRITICAL: Sends full filename as ID so Repo can split it correctly later.
    """
    try:
        all_entities = await graph_service.get_entities()
        
        documents = []
        for entity in all_entities:
            # Flexible checking to find Document nodes
            e_type = str(entity.get("type", "")).lower()
            label = str(entity.get("label", "")).lower()
            props = entity.get("properties", {})
            norm_type = str(props.get("normType", "")).lower()
            
            # Check if this node represents a file
            if "document" in e_type or "document" in label or "document" in norm_type:
                
                # --- Count Extraction (Robust) ---
                raw_node_count = props.get("nodeCount", 0)
                raw_edge_count = props.get("edgeCount", 0)
                
                if isinstance(raw_node_count, list) and len(raw_node_count) > 0: 
                    raw_node_count = raw_node_count[0]
                if isinstance(raw_edge_count, list) and len(raw_edge_count) > 0: 
                    raw_edge_count = raw_edge_count[0]
                
                try: 
                    final_nodes = int(float(str(raw_node_count)))
                except (ValueError, TypeError): 
                    final_nodes = 0

                try: 
                    final_edges = int(float(str(raw_edge_count)))
                except (ValueError, TypeError): 
                    final_edges = 0

                # --- Filename Logic ---
                # 1. Get the REAL filename (e.g. "car_ins_call.csv")
                real_filename = props.get("filename", entity.get("label"))
                if isinstance(real_filename, list):
                    real_filename = real_filename[0]
                real_filename = str(real_filename)

                # 2. Create display name (e.g. "car_ins_call")
                if '.' in real_filename:
                    display_filename = real_filename.rsplit('.', 1)[0]
                else:
                    display_filename = real_filename
                
                # --- CRITICAL FIX ---
                # We MUST use the full 'real_filename' as the ID.
                # This ensures that when Frontend sends this ID back to Fetch/Delete,
                # the Repository receives "car_ins_call.csv" and can correctly
                # split it into domain="car" and id="ins_call".
                
                documents.append({
                    "id": real_filename,          # <--- The Fix: Use full name
                    "filename": display_filename, # Display only
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
    Uses the Repository's smart split logic.
    """
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename required")
        
    logger.info(f"Deleting document: {filename}")
    
    try:
        # Pass the full filename (e.g., "car_ins_call.csv") to the repo.
        # The repo will split it -> domain="car", id="ins_call" -> delete matching data.
        await graph_service.repo.delete_data_by_filename(filename)
            
        return {"status": "success", "deleted": filename}
        
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail=str(e))