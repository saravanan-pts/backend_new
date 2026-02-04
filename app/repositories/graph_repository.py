import logging
import asyncio
import random
from typing import List, Dict, Any, Optional

# Kept your original imports
from app.db.cosmos_client import get_gremlin_client 
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0
from app.config import settings 

logger = logging.getLogger(__name__)

class GraphRepository:
    def __init__(self):
        """
        Initialize using the shared singleton client.
        Loads the correct Partition Key name from settings to prevent 404 writes.
        """
        # We will use this 'client' for string queries to avoid "source_instructions" error
        self.client = None 
        
        # Keep your original property
        self.pk_key = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")
        logger.info(f"GraphRepository initialized. Using Partition Key: '{self.pk_key}'")

    # --- CONNECTION MANAGEMENT (FIXED URL DUPLICATION) ---
    async def connect(self):
        """Initializes the Gremlin client connection."""
        if self.client:
            return

        try:
            # --- FIX: SANITIZE THE URL TO PREVENT DUPLICATION ---
            # Remove protocol and port if they exist in the env var
            raw_endpoint = settings.COSMOS_GREMLIN_ENDPOINT.replace("wss://", "").replace("https://", "")
            if ":" in raw_endpoint:
                raw_endpoint = raw_endpoint.split(":")[0]
            
            # Construct clean endpoint
            endpoint = f"wss://{raw_endpoint}:443/"
            
            # FIX: Check for CONTAINER first, fallback to COLLECTION
            container = getattr(settings, "COSMOS_GREMLIN_CONTAINER", None) or \
                        getattr(settings, "COSMOS_GREMLIN_COLLECTION", "insurance_graph")
            
            username = f"/dbs/{settings.COSMOS_GREMLIN_DATABASE}/colls/{container}"
            password = settings.COSMOS_GREMLIN_KEY
            
            logger.info(f"Connecting to Cosmos DB Gremlin API at {endpoint}")

            # FIX: Use Client directly for string queries
            self.client = Client(
                endpoint,
                'g',
                username=username,
                password=password,
                message_serializer=GraphSONSerializersV2d0()
            )
            logger.info("Successfully connected to Cosmos DB")
        except Exception as e:
            logger.error(f"Failed to connect to Cosmos DB: {e}")
            raise e

    async def close(self):
        """Closes the Gremlin client connection."""
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Cosmos DB connection closed")

    def _escape(self, value: Any) -> str:
        """Helper to escape single quotes for Gremlin string queries."""
        if value is None: return ""
        return str(value).replace("'", "\\'")

    # --- FINAL UPDATED HELPER: FLATTEN + NEST + SWAP LABEL ---
    def _clean_gremlin_data(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        1. Flattens lists (['Beta'] -> 'Beta').
        2. Nests custom fields into 'properties' (Supports ANY CSV column).
        3. SWAPS Label with Type so the UI displays the correct Category Pill.
        """
        cleaned_list = []
        for item in data_list:
            # 1. First, flatten everything from Cosmos DB lists
            flat_item = {}
            for key, val in item.items():
                if isinstance(val, list) and len(val) == 1:
                    flat_item[key] = val[0]
                else:
                    flat_item[key] = val
            
            # Extract core fields
            node_id = str(flat_item.get("id", ""))
            original_label = str(flat_item.get("label", "Node"))
            
            # Find the "Real" Type (Unit, Person, etc.) to show in the UI Pill
            # We prioritize 'type' or 'normType'.
            real_type = flat_item.get("type") or flat_item.get("normType") or original_label

            # 2. Construct the object exactly how the UI wants it
            final_item = {
                "id": node_id,
                "label": real_type, # <--- TRICK: Send "Unit" as the label for UI Pill
                "properties": {} 
            }

            # 3. Move all dynamic CSV data into 'properties'
            for key, val in flat_item.items():
                # Skip system keys at the top level
                if key in ["id", "label"]: 
                    continue
                final_item["properties"][key] = val
            
            # 4. Ensure important metadata is visible in the properties panel
            final_item["properties"]["originalLabel"] = original_label 
            final_item["properties"]["type"] = real_type 
            # This ensures the original CSV Label (e.g. "TestNode") is visible
            final_item["properties"]["label"] = original_label 

            cleaned_list.append(final_item)
        return cleaned_list

    async def _execute_query(self, query: str, bindings: Dict[str, Any] = None) -> Any:
        """
        Centralized query execution with retry logic for rate limiting (429).
        """
        if not self.client: await self.connect()

        retries = 0
        MAX_RETRIES = 5
        
        while True:
            try:
                # We prefer executing without bindings for stability in this specific setup
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
                    return []
                
                # 3. Handle Other Errors
                else:
                    logger.error(f"Query Execution Error: {exc} | Query: {query}")
                    raise exc

    # --- 1. FETCH COMBINED GRAPH (Applied Cleaner) ---
    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None, document_id: str = None) -> Dict[str, Any]:
        try:
            node_query = "g.V()"
            edge_query = "g.E()"

            if document_id:
                safe_id = self._escape(document_id)
                node_query += f".has('documentId', '{safe_id}')"
                edge_query += f".has('doc', '{safe_id}')"
            else:
                node_query += f".limit({limit})"
                edge_query += f".limit({limit*2})"

            if types:
                types_str = "','".join(types)
                node_query += f".hasLabel('{types_str}')"

            # Finalize Queries
            node_query += ".valueMap(true)"
            
            # Using project() is the robust alternative to elementMap()
            edge_query += (
                ".project('id', 'label', 'source', 'target', 'properties')"
                ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
            )

            # Execute
            raw_nodes = await self._execute_query(node_query)
            raw_edges = await self._execute_query(edge_query)

            # CLEAN THE NODES BEFORE RETURNING
            clean_nodes = self._clean_gremlin_data(raw_nodes or [])

            return {
                "nodes": clean_nodes, 
                "edges": raw_edges or [], 
                "meta": {"count": {"nodes": len(clean_nodes), "edges": len(raw_edges or [])}}
            }
        except Exception as exc:
            logger.error(f"Fetch failed: {exc}")
            return {"nodes": [], "edges": [], "error": str(exc)}

    # --- 2. CREATE ENTITY (UPSERT) ---
    async def create_entity(self, entity_id: str, label: str, properties: Dict[str, Any]) -> None:
        """Creates or updates an entity."""
        prop_str = ""
        
        # Keys to skip in the loop
        skip_keys = ["id", "pk", "partitionKey", self.pk_key]

        for key, value in properties.items():
            if key in skip_keys or value is None: continue
            safe_val = self._escape(value)
            prop_str += f".property('{key}', '{safe_val}')"
        
        # Ensure the partition key is set to the ID if not provided, 
        # because Cosmos DB requires a PK.
        pk_val = properties.get(self.pk_key) or properties.get("partitionKey") or entity_id

        query = (
            f"g.V('{entity_id}').fold().coalesce("
            f"unfold(), "
            f"addV('{label}')"
            f".property('id', '{entity_id}')"
            f".property('{self.pk_key}', '{pk_val}')"
            f")" 
            f"{prop_str}" 
        )
        await self._execute_query(query)

    # --- 3. CREATE RELATIONSHIP ---
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

    # --- 4. DELETE DATA BY FILENAME (FIXED TYPO) ---
    async def delete_data_by_filename(self, filename: str) -> None:
        BATCH_SIZE = 20
        try:
            safe_id = self._escape(filename)
            logger.info(f"Deleting data for documentId='{safe_id}'")
            
            query = f"g.V().has('documentId', '{safe_id}').limit({BATCH_SIZE}).drop()"
            count_query = f"g.V().has('documentId', '{safe_id}').count()"
            
            while True:
                res = await self._execute_query(count_query)
                if not res or res[0] == 0: 
                    break
                
                await self._execute_query(query)
                await asyncio.sleep(0.2) 
            
            await self._execute_query(f"g.V('{filename}').drop()")
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error(f"Failed to clear document data for {filename}: {exc}")
            raise exc

    # --- CRITICAL FIXES FOR UPDATE & DELETE ---

    async def update_entity(self, entity_id: str, properties: Dict[str, Any], partition_key: str = None) -> None:
        """
        Updates an entity. 
        CRITICAL FIX: Uses partition_key to ensure the node is found.
        """
        # Fallback: If no PK passed, assume PK == ID (common pattern)
        pk_val = partition_key if partition_key else entity_id
        
        # Start query targeting the specific node in the specific partition
        query = f"g.V('{entity_id}').has('{self.pk_key}', '{pk_val}')"
        
        for k, v in properties.items():
            safe_v = self._escape(v)
            query += f".property('{k}', '{safe_v}')"
            
        await self._execute_query(query)

    async def delete_entity(self, entity_id: str, partition_key: str = None) -> None:
        """
        Deletes an entity.
        CRITICAL FIX: Uses partition_key to ensure the node is found.
        """
        # Fallback: If no PK passed, assume PK == ID
        pk_val = partition_key if partition_key else entity_id

        # Target specific partition to ensure delete happens
        query = f"g.V('{entity_id}').has('{self.pk_key}', '{pk_val}').drop()"
        
        await self._execute_query(query)

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
        raw = await self._execute_query(q)
        # Applied Cleaner Here
        return self._clean_gremlin_data(raw)

    async def get_relationships(self) -> List[Dict[str, Any]]:
        return await self._execute_query("g.E().project('id', 'label', 'source', 'target', 'properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())")

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]) -> None:
        query = f"g.E('{rel_id}')"
        for k, v in properties.items():
            safe_v = self._escape(v)
            query += f".property('{k}', '{safe_v}')"
        await self._execute_query(query)

    async def delete_relationship(self, rel_id: str) -> None:
        await self._execute_query(f"g.E('{rel_id}').drop()")

    async def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        # Using has(label, containing) because TextP is robust in string queries
        query = f"g.V().has('label', TextP.containing('{keyword}')).limit({limit}).valueMap(true)"
        raw = await self._execute_query(query)
        # Applied Cleaner Here
        return self._clean_gremlin_data(raw)

    async def get_stats(self) -> Dict[str, Any]:
        nodes_res = await self._execute_query("g.V().count()")
        edges_res = await self._execute_query("g.E().count()")
        return {"nodes": nodes_res[0] if nodes_res else 0, "edges": edges_res[0] if edges_res else 0}

    async def get_graph(self) -> Dict[str, Any]:
        return {
            "nodes": await self.get_entities(),
            "edges": await self.get_relationships()
        }

    # THIS WAS THE MISSING METHOD - ADDED BACK
    async def get_relationships_for_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        return await self._execute_query(f"g.V('{entity_id}').bothE().elementMap()")

# Initialize the repository instance
graph_repository = GraphRepository()