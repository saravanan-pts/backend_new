import logging
import asyncio
import random
from typing import List, Dict, Any, Optional
from app.db.cosmos_client import get_gremlin_client
from app.config import settings 

logger = logging.getLogger(__name__)

class GraphRepository:
    def __init__(self):
        """
        Initialize using the shared singleton client.
        Loads the correct Partition Key name from settings to prevent 404 writes.
        """
        self.client = get_gremlin_client()
        # LOAD THE REAL PARTITION KEY NAME FROM CONFIG (Defaults to 'partitionKey')
        self.pk_key = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "partitionKey")
        logger.info(f"GraphRepository initialized. Using Partition Key: '{self.pk_key}'")

    def _escape(self, value: Any) -> str:
        """Helper to escape single quotes for Gremlin string queries."""
        if value is None: return ""
        return str(value).replace("'", "\\'")

    async def _execute_query(self, query: str, bindings: Dict[str, Any] = None) -> Any:
        """
        Centralized query execution with retry logic for rate limiting (429)
        and error handling for 404s.
        """
        if not self.client:
            logger.error("Client not initialized.")
            return []

        retries = 0
        MAX_RETRIES = 5
        
        while True:
            try:
                # We prefer executing without bindings (string interpolation) 
                # because it has proven more stable with your specific Cosmos setup.
                if bindings:
                    result_set = self.client.submit_async(query, bindings=bindings).result()
                else:
                    result_set = self.client.submit_async(query).result()
                
                return result_set.all().result()

            except Exception as exc:
                error_msg = str(exc)
                
                # 1. Handle Rate Limiting (429)
                if "429" in error_msg or "RequestRateTooLarge" in error_msg:
                    retries += 1
                    if retries > MAX_RETRIES:
                        logger.error(f"Max retries exceeded for query: {query}")
                        raise exc
                    wait_time = (0.5 * (2 ** retries)) + (random.randint(0, 100) / 1000.0)
                    await asyncio.sleep(wait_time)
                
                # 2. Handle Not Found (404) - Valid for empty DBs
                elif "404" in error_msg:
                    # logger.warning(f"Gremlin 404 (Normal if empty): {query[:60]}...")
                    return []
                
                # 3. Handle Other Errors
                else:
                    logger.error(f"Query Execution Error: {exc}")
                    raise exc

    # --- 1. FETCH COMBINED GRAPH (Fixed: Exact Filename Match) ---
    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None, document_id: str = None) -> Dict[str, Any]:
        try:
            node_query = "g.V()"
            edge_query = "g.E()"

            if document_id:
                # FIX: Use the exact filename. Do not split into domain/id.
                safe_id = self._escape(document_id)
                
                # Query nodes that exactly match the filename
                node_query += f".has('documentId', '{safe_id}')"
                
                # Assume edges might be tagged with 'doc' or just limit them 
                # (Filtering edges strictly by doc property is safer if the property exists)
                edge_query += f".has('doc', '{safe_id}')"
            
            else:
                # No filter? Apply default limit
                node_query += f".limit({limit})"
                edge_query += f".limit({limit*2})"

            if types:
                types_str = "','".join(types)
                node_query += f".hasLabel('{types_str}')"

            # Finalize Queries
            node_query += ".valueMap(true)"
            
            # Use project() is the robust alternative to elementMap()
            edge_query += (
                ".project('id', 'label', 'source', 'target', 'properties')"
                ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
            )

            logger.info(f"Fetching Graph: {node_query}")
            
            # Execute
            raw_nodes = await self._execute_query(node_query)
            raw_edges = await self._execute_query(edge_query)

            return {
                "nodes": raw_nodes or [], 
                "edges": raw_edges or [], 
                "meta": {"count": {"nodes": len(raw_nodes or []), "edges": len(raw_edges or [])}}
            }
        except Exception as exc:
            logger.error(f"Fetch failed: {exc}")
            return {"nodes": [], "edges": [], "error": str(exc)}

    # --- 2. CREATE ENTITY (Robust F-String Version) ---
    async def create_entity(self, entity_id: str, label: str, properties: Dict[str, Any]) -> None:
        """Creates an entity using f-strings to ensure writes succeed."""
        prop_str = ""
        for key, value in properties.items():
            # Skip keys we handle manually
            if key in ["id", "pk", "partitionKey"] or value is None: continue
            safe_val = self._escape(value)
            prop_str += f".property('{key}', '{safe_val}')"
        
        # Explicitly set the Partition Key using the name found in config (self.pk_key)
        query = (
            f"g.V('{entity_id}').fold().coalesce("
            f"unfold(), "
            f"addV('{label}').property('id', '{entity_id}').property('{self.pk_key}', '{entity_id}')"
            f"{prop_str})" 
        )
        await self._execute_query(query)

    # --- 3. CREATE RELATIONSHIP (Robust F-String Version) ---
    async def create_relationship(self, from_id: str, to_id: str, label: str, properties: Dict[str, Any] = None) -> None:
        """Creates a relationship using f-strings."""
        prop_str = ""
        if properties:
            for key, value in properties.items():
                if value is None: continue
                safe_val = self._escape(value)
                prop_str += f".property('{key}', '{safe_val}')"

        query = (
            f"g.V('{from_id}').coalesce("
            f"outE('{label}').where(inV().hasId('{to_id}')),"
            f"addE('{label}').to(g.V('{to_id}')){prop_str})"
        )
        await self._execute_query(query)

    # --- 4. DELETE DATA BY FILENAME (Fixed: Exact Match) ---
    async def delete_data_by_filename(self, filename: str) -> None:
        BATCH_SIZE = 20
        try:
            # FIX: Use exact filename match
            safe_id = self._escape(filename)
            logger.info(f"Deleting data for documentId='{safe_id}'")
            
            # Query to delete nodes belonging to this file
            query = f"g.V().has('documentId', '{safe_id}').limit({BATCH_SIZE}).drop()"
            count_query = f"g.V().has('documentId', '{safe_id}').count()"
            
            while True:
                res = await self._execute_query(count_query)
                # If count is 0 or result is empty, we are done
                if not res or res[0] == 0: 
                    break
                
                await self._execute_query(query)
                await asyncio.sleep(0.2) # Yield to event loop
            
            # Delete the file entity itself (if it exists as a separate node)
            await self._execute_query(f"g.V('{filename}').drop()")
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error(f"Failed to clear document data for {filename}: {exc}")
            raise exc

    # --- STANDARD OPERATIONS ---

    async def clear_graph(self, scope: str = "all") -> bool:
        try:
            BATCH_SIZE = 500
            if scope == "relationships": 
                query = f"g.E().limit({BATCH_SIZE}).drop()"
                check = "g.E().count()"
            else: 
                query = f"g.V().limit({BATCH_SIZE}).drop()"
                check = "g.V().count()"
            
            while True:
                res = await self._execute_query(check)
                if not res or res[0] == 0: break
                await self._execute_query(query)
                await asyncio.sleep(0.2)
            return True
        except: return False

    async def get_entities(self, label: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"g.V().hasLabel('{label}').valueMap(true)" if label else "g.V().valueMap(true)"
        return await self._execute_query(q)

    async def get_relationships(self) -> List[Dict[str, Any]]:
        # Using project() for consistency and safety
        return await self._execute_query("g.E().project('id', 'label', 'source', 'target', 'properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())")

    async def update_entity(self, entity_id: str, properties: Dict[str, Any]) -> None:
        query = f"g.V('{entity_id}')"
        for k, v in properties.items():
            safe_v = self._escape(v)
            query += f".property('{k}', '{safe_v}')"
        await self._execute_query(query)

    async def delete_entity(self, entity_id: str) -> None:
        await self._execute_query(f"g.V('{entity_id}').drop()")

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]) -> None:
        query = f"g.E('{rel_id}')"
        for k, v in properties.items():
            safe_v = self._escape(v)
            query += f".property('{k}', '{safe_v}')"
        await self._execute_query(query)

    async def delete_relationship(self, rel_id: str) -> None:
        await self._execute_query(f"g.E('{rel_id}').drop()")

    async def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        # Using TextP for substring search
        return await self._execute_query(f"g.V().hasLabel(TextP.containing('{keyword}')).limit({limit}).valueMap(true)")

    async def get_stats(self) -> Dict[str, Any]:
        nodes_res = await self._execute_query("g.V().count()")
        edges_res = await self._execute_query("g.E().count()")
        
        nodes = nodes_res[0] if nodes_res else 0
        edges = edges_res[0] if edges_res else 0
        
        return {"nodes": nodes, "edges": edges}

    async def get_graph(self) -> Dict[str, Any]:
        return {
            "nodes": await self.get_entities(),
            "edges": await self.get_relationships()
        }

    async def get_relationships_for_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        return await self._execute_query(f"g.V('{entity_id}').bothE().elementMap()")

# Initialize the repository instance
graph_repository = GraphRepository()