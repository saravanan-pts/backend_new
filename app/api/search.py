from fastapi import APIRouter, Body
from typing import List, Dict, Any
from pydantic import BaseModel
import logging
from app.services.graph_service import graph_service

router = APIRouter(prefix="/api/graph", tags=["Graph"]) 
logger = logging.getLogger(__name__)

class SearchQuery(BaseModel):
    query: str

@router.post("/search")
async def search_nodes(body: SearchQuery) -> Dict[str, Any]:
    """
    Search Nodes matching the query string.
    """
    q = body.query
    try:
        client = graph_service.repo.client
        if not client: 
            return {"results": [], "count": 0}

        # Sanitize input
        clean_q = q.replace("'", "").strip()
        
        # Gremlin Query
        gremlin_query = f"""
            g.V().or(
                has('id', containing('{clean_q}')),
                has('name', containing('{clean_q}')),
                has('label', containing('{clean_q}')),
                has('originalLabel', containing('{clean_q}'))
            ).limit(10)
        """
        
        future = client.submit_async(gremlin_query)
        if hasattr(future, 'result'):
            result = future.result().all().result()
        else:
            result = await future

        nodes = []
        for r in result:
            props = r.get('properties', {})
            clean_props = {}
            
            # Flatten properties
            for k, v in props.items():
                if isinstance(v, list) and len(v) > 0:
                    clean_props[k] = v[0].get('value')
                else:
                    clean_props[k] = v

            # ✅ PRIORITY FIX: 
            # 1. Use 'name' property if it exists
            # 2. Use 'id' (e.g., "Savings") <--- This fixes your issue
            # 3. Only use 'label' (e.g., "Product") as a last resort
            display_label = clean_props.get('name') or r.get('id') or clean_props.get('originalLabel') or r.get('label')

            nodes.append({
                "id": r.get('id'),
                "label": display_label,  # This will now be "Savings"
                "type": clean_props.get('normType') or r.get('label') or "Concept",
                "properties": clean_props
            })

        return {"results": nodes, "count": len(nodes)}

    except Exception as e:
        logger.error(f"Search Error: {e}")
        return {"results": [], "count": 0}