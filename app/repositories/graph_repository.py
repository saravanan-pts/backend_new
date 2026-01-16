import logging
from typing import List, Dict, Any, Optional

from gremlin_python.driver.protocol import GremlinServerError
from app.db.cosmos_client import get_gremlin_client

logger = logging.getLogger(__name__)

class GraphRepository:
    """
    Repository responsible for all Gremlin DB operations.
    Handles only graph persistence & retrieval.
    """

    def __init__(self):
        self.client = get_gremlin_client()

    # -------------------------
    # Graph write operations
    # -------------------------

    async def clear_graph(self) -> None:
        """Delete all vertices + edges in graph."""
        try:
            logger.info("Clearing entire graph")
            # FIXED: Use submit_async and await result
            result_set = await self.client.submit_async("g.V().drop()")
            await result_set.all()
        except Exception as exc:
            logger.exception("Failed to clear graph")
            raise exc

    async def create_entity(
        self,
        entity_id: str,
        label: str,
        properties: Dict[str, Any],
    ) -> None:
        """
        Create or update a vertex using UPSERT pattern.
        Partition key (pk) is only set during creation, not updates.
        """
        try:
            prop_assignments = []
            bindings = {
                "entity_id": entity_id,
                "label": label,
            }
            
            for key, value in properties.items():
                if value is not None:
                    prop_key = f"prop_{key}"
                    prop_assignments.append(f".property('{key}', {prop_key})")
                    bindings[prop_key] = value
            
            props_str = "".join(prop_assignments)
            
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
            # FIXED: Use submit_async and await result
            result_set = await self.client.submit_async(query, bindings=bindings)
            return await result_set.all()

        except Exception as exc:
            logger.error("Failed to upsert entity '%s': %s", entity_id, exc)
            raise exc

    async def delete_entity(self, entity_id: str) -> None:
        """Delete a vertex and its associated edges."""
        try:
            # FIXED: Use submit_async
            result_set = await self.client.submit_async("g.V(id).drop()", bindings={"id": entity_id})
            await result_set.all()
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
            # FIXED: Use submit_async
            result_set = await self.client.submit_async(query, bindings=bindings)
            return await result_set.all()

        except Exception as exc:
            logger.error("Failed to upsert relationship %s -> %s: %s", from_id, to_id, exc)
            raise exc

    # -------------------------
    # Graph read operations
    # -------------------------

    async def get_entities(self) -> List[Dict[str, Any]]:
        """Fetch all vertices with properties."""
        try:
            # FIXED: Use submit_async
            result_set = await self.client.submit_async("g.V().valueMap(true)")
            result = await result_set.all()
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
            # FIXED: Use submit_async
            result_set = await self.client.submit_async(query)
            result = await result_set.all()
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

    # -------------------------
    # Advanced Read Operations
    # -------------------------

    async def fetch_combined_graph(self, limit: int = 500, types: List[str] = None) -> Dict[str, Any]:
        """Unified fetch for graph visualization."""
        try:
            node_query = "g.V()"
            if types:
                node_query += f".hasLabel(within({types}))"
            node_query += f".limit({limit}).valueMap(true)"

            edge_query = (
                "g.E().limit(limit_val).project('id','label','source','target','properties')"
                ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
            )

            # FIXED: Use submit_async for both
            node_rs = await self.client.submit_async(node_query)
            raw_nodes = await node_rs.all()

            edge_rs = await self.client.submit_async(edge_query, bindings={"limit_val": limit * 2})
            raw_edges = await edge_rs.all()

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
            query = f"g.V().hasLabel(containing('{keyword}')).limit({limit}).valueMap(true)"
            # FIXED: Use submit_async
            result_set = await self.client.submit_async(query)
            return await result_set.all()
        except Exception as exc:
            logger.error("Search failed for '%s': %s", keyword, exc)
            raise exc

    async def get_stats(self) -> Dict[str, Any]:
        """Get summary metrics for graph dashboard."""
        try:
            # FIXED: Use submit_async for all count/group queries
            n_rs = await self.client.submit_async("g.V().count()")
            total_nodes = (await n_rs.all())[0]

            e_rs = await self.client.submit_async("g.E().count()")
            total_edges = (await e_rs.all())[0]
            
            nt_rs = await self.client.submit_async("g.V().groupCount().by(label)")
            node_types = await nt_rs.all()

            et_rs = await self.client.submit_async("g.E().groupCount().by(label)")
            edge_types = await et_rs.all()

            return {
                "nodes": total_nodes,
                "edges": total_edges,
                "nodeTypes": node_types[0] if node_types else {},
                "edgeTypes": edge_types[0] if edge_types else {}
            }
        except Exception as exc:
            logger.error("Failed to get stats: %s", exc)
            raise exc

    async def delete_data_by_filename(self, filename: str) -> None:
        """Delete all data associated with a specific file."""
        try:
            query = "g.V().has('sourceDocumentId', filename).drop()"
            # FIXED: Use submit_async
            result_set = await self.client.submit_async(query, bindings={"filename": filename})
            await result_set.all()
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
            # FIXED: Use submit_async
            result_set = await self.client.submit_async("g.V(id).count()", bindings={"id": entity_id})
            result = await result_set.all()
            return result[0] > 0 if result else False
        except Exception as exc:
            logger.exception("Failed to check existence for: %s", entity_id)
            raise exc

    async def get_entity_by_id(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single entity by ID."""
        try:
            # FIXED: Use submit_async
            result_set = await self.client.submit_async("g.V(id).valueMap(true)", bindings={"id": entity_id})
            result = await result_set.all()
            return result[0] if result else None
        except Exception as exc:
            logger.exception("Failed to fetch entity: %s", entity_id)
            raise exc