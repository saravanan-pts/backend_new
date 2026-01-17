import logging
import asyncio
import random
from typing import List, Dict, Any, Optional

from gremlin_python.driver.protocol import GremlinServerError
from app.db.cosmos_client import get_gremlin_client

logger = logging.getLogger(__name__)

class GraphRepository:
    """
    Repository responsible for all Gremlin DB operations.
    Handles graph persistence, retrieval, and 429/Throttle management.
    """

    def __init__(self):
        self.client = get_gremlin_client()

    # -------------------------
    # Internal Helper: Retry Logic
    # -------------------------
    
    async def _execute_query(self, query: str, bindings: Dict[str, Any] = None) -> Any:
        """
        Executes a Gremlin query with built-in retry logic for 429 (Too Many Requests).
        """
        retries = 0
        MAX_RETRIES = 10
        
        while True:
            try:
                # Execute the query
                result_set = self.client.submit_async(query, bindings=bindings).result()
                return result_set.all().result()
            except Exception as exc:
                error_msg = str(exc)
                # Check for Azure Cosmos DB throttling (429)
                if "429" in error_msg or "RequestRateTooLarge" in error_msg:
                    retries += 1
                    if retries > MAX_RETRIES:
                        logger.error(f"Max retries exceeded for query: {query}")
                        raise exc
                    
                    # Exponential backoff
                    wait_time = (2 ** retries) + (random.randint(0, 1000) / 1000.0)
                    logger.warning(f"429 Throttled. Retrying in {wait_time:.2f}s... (Attempt {retries}/{MAX_RETRIES})")
                    await asyncio.sleep(wait_time)
                else:
                    # Not a throttling error, raise immediately
                    raise exc

    # -------------------------
    # Graph write operations
    # -------------------------

    async def clear_graph(self, scope: str = "all") -> bool:
        """
        Delete graph data in batches to avoid 429 RequestRateTooLarge errors.
        Scope: all | documents | entities | relationships
        """
        # FIX 1: Increase Batch Size for speed
        BATCH_SIZE = 500 
        
        try:
            logger.info(f"Starting batched clear of graph (scope={scope}, batch={BATCH_SIZE})")
            
            # FIX 2: REMOVED .iterate() - Cosmos DB executes scripts automatically
            if scope == "relationships":
                drop_query = f"g.E().limit({BATCH_SIZE}).drop()"
                count_query = "g.E().count()"
            elif scope == "entities" or scope == "documents":
                drop_query = f"g.V().limit({BATCH_SIZE}).drop()"
                count_query = "g.V().count()"
            else:
                # Default: all
                drop_query = f"g.V().limit({BATCH_SIZE}).drop()"
                count_query = "g.V().count()"

            while True:
                count_result = await self._execute_query(count_query)
                remaining = count_result[0] if count_result else 0
                
                if remaining == 0:
                    break
                
                logger.info(f"Clearing graph... ~{remaining} items remaining.")
                await self._execute_query(drop_query)
                # Small sleep to let DB breathe
                await asyncio.sleep(0.2)
            
            logger.info("Graph cleared successfully.")
            return True

        except Exception as exc:
            logger.exception(f"Failed to clear graph (scope={scope})")
            raise exc

    async def create_entity(
        self,
        entity_id: str,
        label: str,
        properties: Dict[str, Any],
    ) -> None:
        """
        Create or update a vertex using UPSERT pattern.
        """
        try:
            prop_assignments = []
            bindings = {
                "entity_id": entity_id,
                "label": label,
            }
            
            for key, value in properties.items():
                # Skip 'id' and 'pk' to prevent "Partition key is readonly" error
                if key in ["id", "pk"]:
                    continue
                
                if value is not None:
                    prop_key = f"prop_{key}"
                    prop_assignments.append(f".property('{key}', {prop_key})")
                    bindings[prop_key] = value
            
            props_str = "".join(prop_assignments)
            
            # Note: We explicitly set 'pk' inside the addV() step.
            query = f"""
            g.V(entity_id)
              .fold()
              .coalesce(
                unfold(),
                addV(label)
                  .property('id', entity_id)
                  .property('pk', entity_id)
              )
              {props_str}
            """

            logger.debug("Upserting entity: %s", entity_id)
            await self._execute_query(query, bindings)

        except Exception as exc:
            logger.error("Failed to upsert entity '%s': %s", entity_id, exc)
            raise exc

    async def delete_entity(self, entity_id: str) -> None:
        """Delete a vertex and its associated edges."""
        try:
            # FIX: Removed .iterate() here too
            query = "g.V(id).drop()"
            await self._execute_query(query, bindings={"id": entity_id})
            logger.info("Deleted entity: %s", entity_id)
        except Exception as exc:
            logger.error("Failed to delete entity %s: %s", entity_id, exc)
            raise exc

    async def create_relationship(
        self,
        from_id: str,
        to_id: str,
        label: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create an edge between two existing vertices using UPSERT pattern."""
        try:
            prop_assignments = []
            bindings = {
                "from_id": from_id,
                "to_id": to_id,
                "label": label,
            }

            if properties:
                for key, value in properties.items():
                    if key in ["id", "pk"]:
                        continue
                        
                    if value is not None:
                        prop_key = f"prop_{key}"
                        prop_assignments.append(f".property('{key}', {prop_key})")
                        bindings[prop_key] = value

            props_str = "".join(prop_assignments)

            query = f"""
            g.V(from_id)
              .coalesce(
                outE(label).where(inV().hasId(to_id)),
                addE(label).to(g.V(to_id))
              )
              {props_str}
            """

            logger.debug("Upserting relationship: %s -> %s", from_id, to_id)
            await self._execute_query(query, bindings)

        except Exception as exc:
            logger.error("Failed to upsert relationship %s -> %s: %s", from_id, to_id, exc)
            raise exc

    # -------------------------
    # Graph read operations
    # -------------------------

    async def get_entities(self, label: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch all vertices with properties."""
        try:
            if label:
                query = f"g.V().hasLabel('{label}').valueMap(true)"
            else:
                query = "g.V().valueMap(true)"
            
            result = await self._execute_query(query)
            logger.info("Fetched %d entities from graph", len(result))
            return result
        except Exception as exc:
            logger.exception("Failed to fetch entities")
            raise exc

    async def get_relationships(self) -> List[Dict[str, Any]]:
        """Fetch all edges with metadata."""
        try:
            query = (
                "g.E()"
                ".project('id','label','outV','inV','properties')"
                ".by(id)"
                ".by(label)"
                ".by(outV().id())"
                ".by(inV().id())"
                ".by(valueMap())"
            )
            result = await self._execute_query(query)
            logger.info("Fetched %d relationships from graph", len(result))
            return result
        except Exception as exc:
            logger.exception("Failed to fetch relationships")
            raise exc

    async def get_graph(self) -> Dict[str, Any]:
        """Fetch entities + relationships."""
        return {
            "entities": await self.get_entities(),
            "relationships": await self.get_relationships(),
        }

    async def get_relationships_for_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        """Fetch relationships specific to one entity."""
        try:
            query = (
                f"g.V('{entity_id}').bothE()"
                ".project('id','label','outV','inV','properties')"
                ".by(id)"
                ".by(label)"
                ".by(outV().id())"
                ".by(inV().id())"
                ".by(valueMap())"
            )
            result = await self._execute_query(query)
            return result
        except Exception as exc:
            logger.exception(f"Failed to fetch relationships for {entity_id}")
            raise exc

    # -------------------------
    # Advanced Read Operations
    # -------------------------

    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None) -> Dict[str, Any]:
        """Unified fetch for graph visualization."""
        try:
            node_query = "g.V()"
            if types:
                types_str = ",".join([f"'{t}'" for t in types])
                node_query += f".hasLabel(within({types_str}))"
            node_query += f".limit({limit}).valueMap(true)"

            edge_query = (
                "g.E().limit(limit_val).project('id','label','source','target','properties')"
                ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
            )

            raw_nodes = await self._execute_query(node_query)
            raw_edges = await self._execute_query(edge_query, bindings={"limit_val": limit * 2})

            return {
                "nodes": raw_nodes,
                "edges": raw_edges,
                "meta": {"count": {"nodes": len(raw_nodes), "edges": len(raw_edges)}}
            }
        except Exception as exc:
            logger.error("Failed to fetch combined graph: %s", exc)
            raise exc

    async def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search nodes by label containing keyword."""
        try:
            query = f"g.V().hasLabel(TextP.containing('{keyword}')).limit({limit}).valueMap(true)"
            result = await self._execute_query(query)
            return result
        except Exception as exc:
            logger.error("Search failed for '%s': %s", keyword, exc)
            # Fallback simple search
            try:
                query = f"g.V().hasLabel('{keyword}').limit({limit}).valueMap(true)"
                return await self._execute_query(query)
            except:
                raise exc

    async def get_stats(self) -> Dict[str, Any]:
        """Get summary metrics for graph dashboard."""
        try:
            total_nodes = (await self._execute_query("g.V().count()"))[0]
            total_edges = (await self._execute_query("g.E().count()"))[0]
            
            node_types_list = await self._execute_query("g.V().groupCount().by(label)")
            node_types = node_types_list[0] if node_types_list else {}

            edge_types_list = await self._execute_query("g.E().groupCount().by(label)")
            edge_types = edge_types_list[0] if edge_types_list else {}

            return {
                "nodes": total_nodes,
                "edges": total_edges,
                "nodeTypes": node_types,
                "edgeTypes": edge_types
            }
        except Exception as exc:
            logger.error("Failed to get stats: %s", exc)
            raise exc

    async def delete_data_by_filename(self, filename: str) -> None:
        """Delete all data associated with a specific file (Batch Delete)."""
        BATCH_SIZE = 20
        try:
            # FIX: Removed .iterate() here too
            query = f"g.V().has('sourceDocumentId', '{filename}').limit({BATCH_SIZE}).drop()"
            count_query = f"g.V().has('sourceDocumentId', '{filename}').count()"
            
            while True:
                count_res = await self._execute_query(count_query)
                remaining = count_res[0] if count_res else 0
                if remaining == 0:
                    break
                await self._execute_query(query)
                await asyncio.sleep(0.2)
                
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error("Failed to clear document data for %s: %s", filename, exc)
            raise exc

    # -------------------------
    # Utility operations
    # -------------------------

    async def entity_exists(self, entity_id: str) -> bool:
        """Check if a vertex exists by ID."""
        try:
            query = "g.V(id).count()"
            result = await self._execute_query(query, bindings={"id": entity_id})
            return result[0] > 0 if result else False
        except Exception as exc:
            logger.exception("Failed to check existence for: %s", entity_id)
            raise exc

    async def get_entity_by_id(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single entity by ID."""
        try:
            query = "g.V(id).valueMap(true)"
            result = await self._execute_query(query, bindings={"id": entity_id})
            return result[0] if result else None
        except Exception as exc:
            logger.exception("Failed to fetch entity: %s", entity_id)
            raise exc