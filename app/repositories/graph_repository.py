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

    def clear_graph(self) -> None:
        """Delete all vertices + edges in graph."""
        try:
            logger.info("Clearing entire graph")
            self.client.submit("g.V().drop()").all().result()
        except GremlinServerError as exc:
            logger.exception("Failed to clear graph")
            raise exc

    def create_entity(
        self,
        entity_id: str,
        label: str,
        properties: Dict[str, Any],
    ) -> None:
        """
        Create or update a vertex using UPSERT pattern.
        Partition key (pk) is only set during creation, not updates.
        Cosmos Gremlin requires partition key 'pk' and it's immutable.
        """
        try:
            # Build property assignments dynamically
            prop_assignments = []
            bindings = {
                "entity_id": entity_id,
                "label": label,
            }
            
            # Add other properties (these can be updated)
            for key, value in properties.items():
                if value is not None:
                    # Use prefixed binding names to avoid conflicts
                    prop_key = f"prop_{key}"
                    prop_assignments.append(f".property('{key}', {prop_key})")
                    bindings[prop_key] = value
            
            # Join all property assignments
            props_str = "".join(prop_assignments)
            
            # UPSERT pattern with partition key handling:
            # - pk is ONLY set during vertex creation (inside addV)
            # - pk is NOT updated on existing vertices (outside coalesce)
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

            logger.debug("Upserting entity: %s with properties: %s", entity_id, properties)
            result = self.client.submit(query, bindings=bindings).all().result()
            logger.info("Successfully upserted entity '%s' with label '%s'", entity_id, label)
            return result

        except GremlinServerError as exc:
            logger.error("Failed to upsert entity '%s': %s", entity_id, exc)
            raise exc
        except Exception as exc:
            logger.error("Unexpected error upserting entity '%s': %s", entity_id, exc)
            raise exc

    def delete_entity(self, entity_id: str) -> None:
        """Delete a vertex and its associated edges."""
        try:
            self.client.submit("g.V(id).drop()", bindings={"id": entity_id}).all().result()
            logger.info("Deleted entity: %s", entity_id)
        except Exception as exc:
            logger.error("Failed to delete entity %s: %s", entity_id, exc)
            raise exc

    def create_relationship(
        self,
        from_id: str,
        to_id: str,
        label: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Create an edge between two existing vertices using UPSERT pattern.
        Prevents duplicate edges between the same vertices with the same label.
        """
        try:
            # Build property assignments for edge
            prop_assignments = []
            bindings = {
                "from_id": from_id,
                "to_id": to_id,
                "label": label,
            }

            # Add edge properties if provided
            if properties:
                for key, value in properties.items():
                    if value is not None:
                        prop_key = f"prop_{key}"
                        prop_assignments.append(f".property('{key}', {prop_key})")
                        bindings[prop_key] = value

            props_str = "".join(prop_assignments)

            # UPSERT pattern for edges:
            # 1. Check if edge already exists with same label
            # 2. If not, create it
            query = f"""
            g.V(from_id)
              .coalesce(
                outE(label).where(inV().hasId(to_id)),
                addE(label).to(g.V(to_id))
              )
              {props_str}
            """

            logger.debug("Upserting relationship: %s -[%s]-> %s", from_id, label, to_id)
            result = self.client.submit(query, bindings=bindings).all().result()
            logger.info("Successfully upserted relationship: %s -[%s]-> %s", from_id, label, to_id)
            return result

        except GremlinServerError as exc:
            logger.error("Failed to upsert relationship %s -> %s: %s", from_id, to_id, exc)
            raise exc
        except Exception as exc:
            logger.error("Unexpected error upserting relationship %s -> %s: %s", from_id, to_id, exc)
            raise exc

    # -------------------------
    # Graph read operations
    # -------------------------

    def get_entities(self) -> List[Dict[str, Any]]:
        """Fetch all vertices with properties."""
        try:
            result = (
                self.client
                .submit("g.V().valueMap(true)")
                .all()
                .result()
            )
            logger.info("Fetched %d entities from graph", len(result))
            return result

        except GremlinServerError as exc:
            logger.exception("Failed to fetch entities")
            raise exc

    def get_relationships(self) -> List[Dict[str, Any]]:
        """Fetch all edges with metadata."""
        try:
            result = (
                self.client
                .submit(
                    "g.E()"
                    ".project('id','label','outV','inV','properties')"
                    ".by(id)"
                    ".by(label)"
                    ".by(outV().id())"
                    ".by(inV().id())"
                    ".by(valueMap())"
                )
                .all()
                .result()
            )
            logger.info("Fetched %d relationships from graph", len(result))
            return result

        except GremlinServerError as exc:
            logger.exception("Failed to fetch relationships")
            raise exc

    def get_graph(self) -> Dict[str, Any]:
        """Fetch entities + relationships."""
        return {
            "entities": self.get_entities(),
            "relationships": self.get_relationships(),
        }

    # ---------------------------------------------------------
    # NEW: Advanced Read Operations for api/graph/fetch
    # ---------------------------------------------------------

    def fetch_combined_graph(self, limit: int = 500, types: List[str] = None) -> Dict[str, Any]:
        """
        Unified fetch for graph visualization.
        Returns format: { "nodes": [...], "edges": [...], "meta": {...} }
        """
        try:
            # 1. Build Node Query
            node_query = "g.V()"
            if types:
                node_query += f".hasLabel(within({types}))"
            node_query += f".limit({limit}).valueMap(true)"

            # 2. Build Edge Query (fetch edges connecting visible nodes)
            edge_query = "g.E().limit(limit * 2).project('id','label','source','target','properties').by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"

            raw_nodes = self.client.submit(node_query).all().result()
            raw_edges = self.client.submit(edge_query, bindings={"limit": limit}).all().result()

            return {
                "nodes": raw_nodes,
                "edges": raw_edges,
                "meta": {
                    "count": {"nodes": len(raw_nodes), "edges": len(raw_edges)}
                }
            }
        except Exception as exc:
            logger.error("Failed to fetch combined graph: %s", exc)
            raise exc

    def search_nodes(self, keyword: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search nodes by label or property values containing keyword."""
        try:
            # Simple keyword search across labels
            query = f"g.V().hasLabel(containing('{keyword}')).limit({limit}).valueMap(true)"
            return self.client.submit(query).all().result()
        except Exception as exc:
            logger.error("Search failed for '%s': %s", keyword, exc)
            raise exc

    def get_stats(self) -> Dict[str, Any]:
        """Get summary metrics for graph dashboard."""
        try:
            total_nodes = self.client.submit("g.V().count()").all().result()[0]
            total_edges = self.client.submit("g.E().count()").all().result()[0]
            
            node_types = self.client.submit("g.V().groupCount().by(label)").all().result()
            edge_types = self.client.submit("g.E().groupCount().by(label)").all().result()

            return {
                "nodes": total_nodes,
                "edges": total_edges,
                "nodeTypes": node_types[0] if node_types else {},
                "edgeTypes": edge_types[0] if edge_types else {}
            }
        except Exception as exc:
            logger.error("Failed to get stats: %s", exc)
            raise exc

    def delete_data_by_filename(self, filename: str) -> None:
        """Delete all vertices and edges associated with a specific file."""
        try:
            # Delete vertices (and their edges) tagged with this filename
            query = "g.V().has('sourceDocumentId', filename).drop()"
            self.client.submit(query, bindings={"filename": filename}).all().result()
            logger.info("Cleared graph data for document: %s", filename)
        except Exception as exc:
            logger.error("Failed to clear document data for %s: %s", filename, exc)
            raise exc

    # -------------------------
    # Utility operations
    # -------------------------

    def entity_exists(self, entity_id: str) -> bool:
        """Check if a vertex exists by ID."""
        try:
            result = (
                self.client
                .submit("g.V(entity_id).count()", bindings={"entity_id": entity_id})
                .all()
                .result()
            )
            exists = result[0] > 0 if result else False
            logger.debug("Entity '%s' exists: %s", entity_id, exists)
            return exists

        except GremlinServerError as exc:
            logger.exception("Failed to check if entity exists: %s", entity_id)
            raise exc

    def get_entity_by_id(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single entity by ID."""
        try:
            result = (
                self.client
                .submit("g.V(entity_id).valueMap(true)", bindings={"entity_id": entity_id})
                .all()
                .result()
            )
            entity = result[0] if result else None
            logger.debug("Fetched entity '%s': %s", entity_id, entity)
            return entity

        except GremlinServerError as exc:
            logger.exception("Failed to fetch entity by ID: %s", entity_id)
            raise exc