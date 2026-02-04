from fastapi import APIRouter, Query
from typing import List, Dict, Any
import logging
from app.services.graph_service import graph_service

# Prefix is crucial: This matches the frontend call "/api/search"
router = APIRouter(prefix="/api/search", tags=["Search"])
logger = logging.getLogger(__name__)

@router.get("")
async def search_nodes(q: str = Query(..., min_length=1)) -> List[Dict[str, Any]]:
    """
    Search Nodes: Optimized for Cosmos DB Gremlin.
    """
    try:
        client = graph_service.repo.client
        if not client: 
            logger.warning("Search failed: DB Client not connected")
            return []

        # Sanitize input to prevent injection
        clean_q = q.replace("'", "").strip()
        lower_q = clean_q.lower()

        # OPTIMIZED GREMLIN QUERY
        # 1. We start with g.V() which hits all partitions (Cross-Partition).
        # 2. We allow this because 'Search' is global.
        # 3. We use coalesce to return something if found.
        
        gremlin_query = f"""
            g.V().or(
                has('id', containing('{clean_q}')),
                has('label', containing('{clean_q}')),
                has('name', containing('{clean_q}')),
                has('originalLabel', containing('{clean_q}'))
            ).limit(10)
        """
        
        logger.info(f"Executing Search: {clean_q}")
        
        future = client.submit_async(gremlin_query)
        if hasattr(future, 'result'):
            result = future.result().all().result()
        else:
            result = await future

        nodes = []
        for r in result:
            props = r.get('properties', {})
            clean_props = {}
            
            # Cosmos DB Gremlin properties come as lists [{value: ..., id: ...}]
            # We flatten them for the frontend
            for k, v in props.items():
                if isinstance(v, list) and len(v) > 0:
                    clean_props[k] = v[0].get('value')
                else:
                    clean_props[k] = v

            # Determine best display label
            display = clean_props.get('originalLabel') or clean_props.get('name') or r.get('label') or r.get('id')

            nodes.append({
                "id": r.get('id'),
                "label": display, 
                "type": clean_props.get('normType') or clean_props.get('type') or "Concept",
                "properties": clean_props
            })

        logger.info(f"Search found {len(nodes)} results")
        return nodes

    except Exception as e:
        logger.error(f"Search Error: {e}")
        return []