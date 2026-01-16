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
        # Fixed: Changed .submit().all().result() to await submit_async()
        query = "g.V().hasLabel('Document').valueMap(true)"
        
        result_set = await graph_service.repo.client.submit_async(query)
        docs = await result_set.all()
        
        return {"files": docs}
    except Exception as e:
        # This will now catch actual DB errors instead of Event Loop errors
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
        # Note: Ensure delete_data_by_filename in graph_repository is also async!
        await graph_service.repo.delete_data_by_filename(filename)
        
        # 2. Fixed: Use await submit_async for the metadata deletion
        delete_meta_query = "g.V().hasLabel('Document').has('filename', name).drop()"
        result_set = await graph_service.repo.client.submit_async(
            delete_meta_query, 
            bindings={"name": filename}
        )
        await result_set.all()
        
        return {"status": "deleted", "filename": filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {str(e)}")