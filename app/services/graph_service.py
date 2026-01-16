import logging
from typing import List, Dict, Any, Optional

from app.repositories.graph_repository import GraphRepository

logger = logging.getLogger(__name__)


class GraphService:
    """
    Service layer for graph-related business logic.
    Orchestrates repositories, applies rules, and ensures consistency.
    FIXED: All methods converted to async to prevent Event Loop conflicts.
    """

    def __init__(self):
        self.repo = GraphRepository()

    # -------------------------
    # Graph lifecycle
    # -------------------------

    async def clear_graph(self, scope: str = "all") -> bool:
        """
        Clear graph data based on scope.
        Scope: all | documents | entities | relationships
        """
        logger.info(f"GraphService: clearing graph with scope: {scope}")
        
        # Logic: Delegates the query generation and execution to the repo
        # Ensure your repo.clear_graph accepts the 'scope' argument
        result = await self.repo.clear_graph(scope)
        return result

    # -------------------------
    # Entity logic
    # -------------------------

    async def add_entities(self, entities: List[Dict[str, Any]]) -> None:
        """
        Add multiple entities to the graph.
        [LOGIC RETAINED]: Validates each entity before calling the repository.
        """
        logger.info("GraphService: adding %d entities", len(entities))

        for entity in entities:
            # Synchronous validation logic
            self._validate_entity(entity)

            # FIXED: Await the repository call
            await self.repo.create_entity(
                entity_id=entity["id"],
                label=entity["label"],
                properties=entity.get("properties", {}),
            )

    # -------------------------
    # Relationship logic
    # -------------------------

    async def add_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Add multiple relationships to the graph.
        [LOGIC RETAINED]: Validates each relationship before calling the repository.
        """
        logger.info(
            "GraphService: adding %d relationships",
            len(relationships),
        )

        for rel in relationships:
            # Synchronous validation logic
            self._validate_relationship(rel)

            # FIXED: Await the repository call
            await self.repo.create_relationship(
                from_id=rel["from"],
                to_id=rel["to"],
                label=rel["label"],
                properties=rel.get("properties"),
            )

    # -------------------------
    # Graph queries
    # -------------------------

    async def get_graph(self) -> Dict[str, Any]:
        """
        Fetch complete graph (entities + relationships).
        """
        logger.info("GraphService: fetching full graph")
        # FIXED: Await the repository call
        return await self.repo.get_graph()

    async def get_entities(self, label: Optional[str] = None):
        """
        Fetch entities, optionally filtered by label.
        """
        logger.info(f"GraphService: fetching entities (filter: {label})")
        # FIXED: Await the repository call
        return await self.repo.get_entities(label=label)

    async def get_relationships(self):
        """
        Fetch all relationships.
        """
        logger.info("GraphService: fetching relationships")
        # FIXED: Await the repository call
        return await self.repo.get_relationships()

    async def get_relationships_for_entity(self, entity_id: str):
        """
        Fetch relationships specific to one entity.
        """
        logger.info(f"GraphService: fetching relationships for {entity_id}")
        # FIXED: Await the repository call
        return await self.repo.get_relationships_for_entity(entity_id)

    # -------------------------
    # Internal validation rules (Stay synchronous as they are pure logic)
    # -------------------------

    def _validate_entity(self, entity: Dict[str, Any]) -> None:
        required_fields = ["id", "label"]
        for field in required_fields:
            if field not in entity:
                raise ValueError(f"Entity missing required field: {field}")

    def _validate_relationship(self, relationship: Dict[str, Any]) -> None:
        required_fields = ["from", "to", "label"]
        for field in required_fields:
            if field not in relationship:
                raise ValueError(
                    f"Relationship missing required field: {field}"
                )


# Instantiate the service singleton
graph_service = GraphService()