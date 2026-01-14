from fastapi import APIRouter, HTTPException
from typing import Dict, Any
# Use the existing graph_service instead of creating a new repo
from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/documents", tags=["Documents"])

@router.get("")
async def list_documents():
    """
    GET: Lists all instruction books (Documents) in your library.
    """
    try:
        # Standard Gremlin pattern used in your project
        query = "g.V().hasLabel('Document').valueMap(true)"
        docs = graph_service.repo.client.submit(query).all().result()
        return {"files": docs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch documents: {str(e)}")

@router.delete("")
async def delete_document(payload: Dict[str, Any]):
    """
    DELETE: Removes a file and all the graph data (nodes/edges) it created.
    """
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    try:
        # 1. Clean up all the bricks (nodes/edges) created from this file
        graph_service.repo.delete_data_by_filename(filename)
        
        # 2. Throw the instruction book (Document node) in the trash
        delete_meta_query = "g.V().hasLabel('Document').has('filename', name).drop()"
        graph_service.repo.client.submit(delete_meta_query, bindings={"name": filename}).all().result()
        
        return {"status": "deleted", "filename": filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {str(e)}")