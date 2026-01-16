from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging
# Use the existing graph_service instead of creating a new repo
from app.services.graph_service import graph_service

# Configure logging to catch errors in the console
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/documents", tags=["Documents"])

@router.get("")
async def list_documents():
    """
    GET: Lists all instruction books (Documents) in your library.
    """
    try:
        query = "g.V().hasLabel('Document').valueMap(true)"
        
        # FIX: Use .result() instead of await.
        # The Gremlin Python driver returns a Future that works best 
        # in FastAPI when blocked synchronously using nest_asyncio.
        future = graph_service.repo.client.submit_async(query)
        result_set = future.result()
        
        # Fetch all results from the set
        future_results = result_set.all()
        raw_docs = future_results.result()
        
        # Helper: CosmosDB returns properties as lists (e.g., {'filename': ['doc.pdf']})
        # We need to flatten them to strings for the frontend
        clean_docs = []
        for d in raw_docs:
            clean_item = {}
            for k, v in d.items():
                # If it's a list with 1 item, take the item. Otherwise keep it.
                if isinstance(v, list) and len(v) == 1:
                    clean_item[k] = v[0]
                else:
                    clean_item[k] = v
            clean_docs.append(clean_item)
        
        return {"files": clean_docs}
    except Exception as e:
        # Print actual error to terminal for debugging
        print(f"CRITICAL ERROR in list_documents: {str(e)}")
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
        # Note: Ensure delete_data_by_filename in graph_repository is also updated or compatible
        await graph_service.repo.delete_data_by_filename(filename)
        
        # 2. Delete the Document metadata vertex
        delete_meta_query = "g.V().hasLabel('Document').has('filename', name).drop()"
        
        # FIX: Use .result() for the direct client call
        future = graph_service.repo.client.submit_async(
            delete_meta_query, 
            bindings={"name": filename}
        )
        result_set = future.result()
        result_set.all().result()
        
        return {"status": "deleted", "filename": filename}
    except Exception as e:
        print(f"CRITICAL ERROR in delete_document: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {str(e)}")