import logging
from typing import List, Dict, Any

from app.repositories.graph_repository import GraphRepository

logger = logging.getLogger(__name__)


class GraphService:
    """
    Service layer for graph-related business logic.
    Orchestrates repositories, applies rules, and ensures consistency.
    """

    def __init__(self):
        self.repo = GraphRepository()

    # -------------------------
    # Graph lifecycle
    # -------------------------

    def clear_graph(self) -> None:
        """
        Clear entire graph.
        Business decision: full reset allowed.
        """
        logger.info("GraphService: clearing graph")
        self.repo.clear_graph()

    # -------------------------
    # Entity logic
    # -------------------------

    def add_entities(self, entities: List[Dict[str, Any]]) -> None:
        """
        Add multiple entities to the graph.

        Expected entity format:
        {
            "id": "entity_id",
            "label": "Person",
            "properties": {...}
        }
        """
        logger.info("GraphService: adding %d entities", len(entities))

        for entity in entities:
            self._validate_entity(entity)

            self.repo.create_entity(
                entity_id=entity["id"],
                label=entity["label"],
                properties=entity.get("properties", {}),
            )

    # -------------------------
    # Relationship logic
    # -------------------------

    def add_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Add multiple relationships to the graph.

        Expected relationship format:
        {
            "from": "entity_id",
            "to": "entity_id",
            "label": "RELATED_TO",
            "properties": {...}
        }
        """
        logger.info(
            "GraphService: adding %d relationships",
            len(relationships),
        )

        for rel in relationships:
            self._validate_relationship(rel)

            self.repo.create_relationship(
                from_id=rel["from"],
                to_id=rel["to"],
                label=rel["label"],
                properties=rel.get("properties"),
            )

    # -------------------------
    # Graph queries
    # -------------------------

    def get_graph(self) -> Dict[str, Any]:
        """
        Fetch complete graph (entities + relationships).
        """
        logger.info("GraphService: fetching full graph")
        return self.repo.get_graph()

    def get_entities(self):
        logger.info("GraphService: fetching entities")
        return self.repo.get_entities()

    def get_relationships(self):
        logger.info("GraphService: fetching relationships")
        return self.repo.get_relationships()

    # -------------------------
    # Internal validation rules
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


graph_service = GraphService()
