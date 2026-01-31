from fastapi import APIRouter, HTTPException, Body
from typing import List, Dict, Any
import logging
from concurrent.futures import Future
import traceback

# Import the service (which uses the repo -> which uses the fixed client)
from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/documents", tags=["Documents"])
logger = logging.getLogger(__name__)

@router.get("")
async def list_documents() -> List[Dict[str, Any]]:
    """
    Fetch documents using Aggregation.
    Safe version: Handles Cosmos DB '404' (Empty DB) gracefully.
    """
    try:
        client = graph_service.repo.client
        if not client:
            logger.warning("Graph client is not initialized.")
            return []

        # 1. The Aggregation Query (Group by documentId)
        query = "g.V().has('documentId').group().by('documentId').by(count())"
        
        # 2. Execute Safely
        try:
            future = client.submit_async(query)
            if hasattr(future, 'result'):
                result_set = future.result()       # Block until query is done
                result = result_set.all().result() # Block until data is returned
            else:
                result = await future
        except Exception as query_exc:
            error_msg = str(query_exc)
            # If 404 (NotFound), it just means the graph is empty. Return empty list.
            if "404" in error_msg or "NotFound" in error_msg:
                logger.info("Documents query returned 404 (Database is empty). Returning [].")
                return []
            # Real error? Re-raise it.
            raise query_exc

        documents = []

        if result:
            # Gremlin .group() returns a single dict inside a list: [{'file.csv': 708}]
            data_map = result[0] if isinstance(result, list) and len(result) > 0 else {}
            
            # Helper to iterate safely
            iterator = []
            if isinstance(data_map, dict):
                iterator = data_map.items()
            elif hasattr(data_map, 'items'):
                 iterator = data_map.items()

            for doc_id_key, count_val in iterator:
                # 1. Formatting
                raw_name = str(doc_id_key)
                display_filename = raw_name
                
                # Remove extension
                if '.' in display_filename:
                    display_filename = display_filename.rsplit('.', 1)[0]
                
                # Pretty Print
                if "_" in display_filename:
                    display_filename = display_filename.replace("_", " ").title()
                elif "-" in display_filename:
                    display_filename = display_filename.replace("-", " ").title()
                
                # 2. Build Response
                documents.append({
                    "id": raw_name,
                    "documentId": raw_name,
                    "filename": f"{raw_name}.csv", # Guessing extension for display
                    "displayName": display_filename,
                    "entityCount": count_val,
                    "type": "file"
                })

        return documents

    except Exception as e:
        # Catch unexpected errors to prevent 500 response to frontend
        logger.error(f"Error listing documents: {e}")
        traceback.print_exc()
        return []

@router.delete("")
async def delete_document(payload: Dict[str, Any] = Body(...)):
    filename = payload.get("filename")
    if not filename:
        raise HTTPException(status_code=400, detail="Filename required")
    try:
        clean_name = filename.replace(".csv", "")
        await graph_service.repo.delete_data_by_filename(clean_name)
        return {"status": "success", "deleted": filename}
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail=str(e))