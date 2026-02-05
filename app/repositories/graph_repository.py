import logging
import asyncio
import random
from typing import List, Dict, Any, Optional

# Core Gremlin Imports
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0
# ✅ Added TextP import to fix Search crash
from gremlin_python.process.traversal import TextP
from app.config import settings 

logger = logging.getLogger(__name__)

class GraphRepository:
    def __init__(self):
        """
        Initialize the repository.
        Loads the correct Partition Key name from settings to prevent 404 writes.
        """
        self.client = None 
        # Defines the property key used for partitioning (e.g., 'pk' or 'partitionKey')
        self.pk_key = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")
        logger.info(f"GraphRepository initialized. Using Partition Key: '{self.pk_key}'")

    # ==========================================
    # 1. CONNECTION MANAGEMENT
    # ==========================================
    async def connect(self):
        """Initializes the Gremlin client connection with URL sanitization."""
        if self.client:
            return

        try:
            # Remove protocol and port if they exist in the env var to prevent duplication
            raw_endpoint = settings.COSMOS_GREMLIN_ENDPOINT.replace("wss://", "").replace("https://", "")
            if ":" in raw_endpoint:
                raw_endpoint = raw_endpoint.split(":")[0]
            
            endpoint = f"wss://{raw_endpoint}:443/"
            
            # Check for CONTAINER first, fallback to COLLECTION
            container = getattr(settings, "COSMOS_GREMLIN_CONTAINER", None) or \
                        getattr(settings, "COSMOS_GREMLIN_COLLECTION", "insurance_graph")
            
            username = f"/dbs/{settings.COSMOS_GREMLIN_DATABASE}/colls/{container}"
            password = settings.COSMOS_GREMLIN_KEY
            
            logger.info(f"Connecting to Cosmos DB Gremlin API at {endpoint}")

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
        if self.client:
            self.client.close()
            self.client = None
            logger.info("Cosmos DB connection closed")

    # ==========================================
    # 2. HELPER METHODS
    # ==========================================
    def _escape(self, value: Any) -> str:
        """Helper to escape single quotes for Gremlin string queries."""
        if value is None: return ""
        return str(value).replace("'", "\\'")

    def _clean_gremlin_data(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        CRITICAL UI HELPER (FIXED FOR EDIT FORM):
        1. Flattens lists (['Beta'] -> 'Beta').
        2. Sets 'label' to Display Name (for Edit Form).
        3. Sets 'type' to Category (for Color/Shape).
        """
        cleaned_list = []
        for item in data_list:
            flat_item = {}
            # Flatten lists from Cosmos DB
            for key, val in item.items():
                if isinstance(val, list) and len(val) == 1:
                    flat_item[key] = val[0]
                else:
                    flat_item[key] = val
            
            node_id = str(flat_item.get("id", ""))
            
            # The Gremlin 'label' is actually the Category (e.g. "Organization")
            gremlin_category = str(flat_item.get("label", "Node"))
            
            # Determine the visual Display Name (Label)
            # Prefer 'name' property -> fallback to ID
            display_name = flat_item.get("name") or node_id

            final_item = {
                "id": node_id,
                "label": display_name,    # ✅ FIX: Shows "irmai" in UI
                "type": gremlin_category, # ✅ FIX: Shows "Organization" in UI
                "properties": {} 
            }

            for key, val in flat_item.items():
                if key in ["id", "label"]: continue
                final_item["properties"][key] = val
            
            # Metadata for side panel
            final_item["properties"]["originalLabel"] = display_name 
            final_item["properties"]["type"] = gremlin_category 
            final_item["properties"]["label"] = display_name 
            # Ensure Partition Key is visible so UI can send it back on delete
            if self.pk_key in flat_item:
                final_item["properties"][self.pk_key] = flat_item[self.pk_key]

            cleaned_list.append(final_item)
        return cleaned_list

    async def _execute_query(self, query: str, bindings: Dict[str, Any] = None) -> Any:
        """Centralized execution with Retry Logic (429/404 handling)."""
        if not self.client: await self.connect()

        retries = 0
        MAX_RETRIES = 5
        
        while True:
            try:
                if bindings:
                    result_set = self.client.submit_async(query, bindings=bindings).result()
                else:
                    result_set = self.client.submit_async(query).result()
                
                return result_set.all().result()

            except Exception as exc:
                error_msg = str(exc)
                
                # Handle Rate Limiting
                if "429" in error_msg or "RequestRateTooLarge" in error_msg:
                    retries += 1
                    if retries > MAX_RETRIES:
                        logger.error(f"Max retries exceeded: {query}")
                        raise exc
                    wait_time = (0.5 * (2 ** retries)) + (random.randint(0, 100) / 1000.0)
                    await asyncio.sleep(wait_time)
                
                # Handle Not Found
                elif "404" in error_msg:
                    return []
                
                else:
                    logger.error(f"Query Error: {exc} | Query: {query}")
                    raise exc

    # ==========================================
    # 3. CORE GRAPH OPERATIONS
    # ==========================================

    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None, document_id: str = None) -> Dict[str, Any]:
        """Main fetch for the visualizer."""
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

            node_query += ".valueMap(true)"
            edge_query += ".project('id', 'label', 'source', 'target', 'properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"

            raw_nodes = await self._execute_query(node_query)
            raw_edges = await self._execute_query(edge_query)

            # Apply UI Cleaner
            clean_nodes = self._clean_gremlin_data(raw_nodes or [])

            return {
                "nodes": clean_nodes, 
                "edges": raw_edges or [], 
                "meta": {"count": {"nodes": len(clean_nodes), "edges": len(raw_edges or [])}}
            }
        except Exception as exc:
            logger.error(f"Fetch failed: {exc}")
            return {"nodes": [], "edges": [], "error": str(exc)}

    # ==========================================
    # 4. CRUD OPERATIONS (✅ FIXED WITH PK)
    # ==========================================

    async def create_entity(self, entity_id: str, label: str, properties: Dict[str, Any]) -> None:
        """Creates or Updates (Upsert) a node."""
        prop_str = ""
        skip_keys = ["id", "pk", "partitionKey", self.pk_key]

        for key, value in properties.items():
            if key in skip_keys or value is None: continue
            safe_val = self._escape(value)
            prop_str += f".property('{key}', '{safe_val}')"
        
        # Ensure PK is set
        pk_val = properties.get(self.pk_key) or properties.get("partitionKey") or entity_id

        # Upsert Logic: Fold -> Coalesce(Unfold, AddV)
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

    async def update_entity(self, entity_id: str, properties: Dict[str, Any], partition_key: str = None) -> None:
        """
        Updates a node. 
        ✅ STRICT PK ENFORCEMENT: Uses partition_key to find the exact node.
        """
        # Fallback: If no PK passed, assume PK == ID
        pk_val = partition_key if partition_key else entity_id
        
        # Target specific node in specific partition
        query = f"g.V('{entity_id}').has('{self.pk_key}', '{pk_val}')"
        
        for k, v in properties.items():
            if k in ["id", self.pk_key]: continue
            safe_v = self._escape(v)
            query += f".property('{k}', '{safe_v}')"
            
        await self._execute_query(query)

    async def delete_entity(self, entity_id: str, partition_key: str = None) -> None:
        """
        Deletes a node.
        ✅ STRICT PK ENFORCEMENT: Uses partition_key to ensure delete works in Cosmos.
        """
        # Fallback: If no PK passed, assume PK == ID
        pk_val = partition_key if partition_key else entity_id

        # Target specific partition
        query = f"g.V('{entity_id}').has('{self.pk_key}', '{pk_val}').drop()"
        await self._execute_query(query)

    async def create_relationship(self, from_id: str, to_id: str, label: str, properties: Dict[str, Any] = None) -> None:
        """Creates an edge if it doesn't exist."""
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

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]) -> None:
        """Updates an existing edge."""
        query = f"g.E('{rel_id}')"
        for k, v in properties.items():
            safe_val = self._escape(v)
            query += f".property('{k}', '{safe_val}')"
        await self._execute_query(query)

    async def delete_relationship(self, rel_id: str) -> None:
        """Deletes an edge."""
        await self._execute_query(f"g.E('{rel_id}').drop()")

    async def delete_data_by_filename(self, filename: str) -> None:
        """Batched delete for documents."""
        BATCH_SIZE = 500
        try:
            safe_id = self._escape(filename)
            logger.info(f"Deleting data for documentId='{safe_id}'")
            
            count_query = f"g.V().has('documentId', '{safe_id}').count()"
            while True:
                res = await self._execute_query(count_query)
                if not res or res[0] == 0: break
                
                await self._execute_query(f"g.V().has('documentId', '{safe_id}').limit({BATCH_SIZE}).drop()")
                await asyncio.sleep(0.1) 
            
            await self._execute_query(f"g.E().has('doc', '{safe_id}').drop()")
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error(f"Failed to clear document data for {filename}: {exc}")
            pass

    # ==========================================
    # 5. DATA RETRIEVAL (RESTORED METHODS)
    # ==========================================

    async def get_stats(self) -> Dict[str, Any]:
        nodes_res = await self._execute_query("g.V().count()")
        edges_res = await self._execute_query("g.E().count()")
        return {"nodes": nodes_res[0] if nodes_res else 0, "edges": edges_res[0] if edges_res else 0}

    async def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        # ✅ Using TextP correctly now that it's imported
        query = f"g.V().has('label', TextP.containing('{keyword}')).limit({limit}).valueMap(true)"
        raw = await self._execute_query(query)
        return self._clean_gremlin_data(raw)

    async def clear_graph(self, scope: str = "all") -> bool:
        try:
            if scope == "all": await self._execute_query("g.V().drop()")
            return True
        except: return False
    
    async def get_entities(self, label: Optional[str] = None) -> List[Dict[str, Any]]:
        q = f"g.V().hasLabel('{label}').valueMap(true)" if label else "g.V().valueMap(true)"
        raw = await self._execute_query(q)
        return self._clean_gremlin_data(raw)

    async def get_relationships(self) -> List[Dict[str, Any]]:
        return await self._execute_query("g.E().project('id', 'label', 'source', 'target', 'properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())")

    async def get_graph(self) -> Dict[str, Any]:
        return {
            "nodes": await self.get_entities(),
            "edges": await self.get_relationships()
        }

    async def get_relationships_for_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        return await self._execute_query(f"g.V('{entity_id}').bothE().elementMap()")

graph_repository = GraphRepository()