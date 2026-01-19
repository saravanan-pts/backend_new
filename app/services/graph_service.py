import logging
import uuid
from typing import List, Dict, Any, Optional

# Import the repository instance
from app.repositories.graph_repository import graph_repository 
from app.utils.normalizer import normalize_entity_type

logger = logging.getLogger(__name__)

class GraphService:
    """
    Service layer for graph-related business logic.
    Orchestrates repositories, applies rules (Normalization), and ensures consistency.
    """

    def __init__(self):
        self.repo = graph_repository

    # -------------------------
    # Graph lifecycle
    # -------------------------

    async def clear_graph(self, scope: str = "all") -> bool:
        """
        Clear graph data based on scope.
        """
        logger.info(f"GraphService: clearing graph with scope: {scope}")
        return await self.repo.clear_graph(scope)

    # -------------------------
    # Entity logic
    # -------------------------

    async def add_entities(self, entities: List[Dict[str, Any]]) -> None:
        """
        Add multiple entities to the graph.
        Auto-generates IDs if missing and applies Normalization.
        """
        logger.info("GraphService: adding %d entities", len(entities))

        for entity in entities:
            # --- 1. AUTO-GENERATE ID IF MISSING ---
            if "id" not in entity or not entity["id"]:
                entity["id"] = str(uuid.uuid4())

            # --- 2. APPLY NORMALIZATION LOGIC ---
            raw_type = entity.get("type", "Concept")
            raw_label = entity.get("label", str(entity.get("id", "")))
            
            # Get the clean type (e.g., "Account", "Job", "Person")
            clean_type = normalize_entity_type(raw_type, raw_label)
            
            # Update the entity object properties
            if "properties" not in entity:
                entity["properties"] = {}
            
            # Store standardized type and original label in properties
            entity["properties"]["type"] = clean_type
            entity["properties"]["normType"] = clean_type 
            entity["properties"]["label"] = raw_label 

            # --- 3. VALIDATION ---
            self._validate_entity(entity)

            # --- 4. PERSISTENCE ---
            # Pass clean_type as the Label
            await self.repo.create_entity(
                entity_id=entity["id"],
                label=clean_type,          
                properties=entity["properties"]
            )

    async def update_entity(self, entity_id: str, properties: Dict[str, Any]):
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str):
        return await self.repo.delete_entity(entity_id)

    # --- FIX 1: ROBUST DOCUMENT DELETION (The Loop) ---
    async def delete_document_data(self, doc_id: str) -> int:
        """
        Deletes the document node AND all child nodes tagged with its ID.
        Uses an explicit loop to ensure no orphans are left behind.
        """
        logger.info(f"Starting cleanup for document: {doc_id}")
        
        # 1. Fetch all entities from the graph
        all_entities = await self.repo.get_entities()
        ids_to_delete = []

        # 2. Identify nodes to delete
        for entity in all_entities:
            nid = str(entity.get("id", ""))
            props = entity.get("properties", {})
            
            # Check A: Is this the document node itself?
            if nid == doc_id:
                ids_to_delete.append(nid)
                continue

            # Check B: Is this a child node? (documentId matches)
            child_doc_ref = props.get("documentId")
            if isinstance(child_doc_ref, list) and len(child_doc_ref) > 0:
                child_doc_ref = child_doc_ref[0]
            
            if str(child_doc_ref) == doc_id:
                ids_to_delete.append(nid)

        # 3. Execute Deletion
        count = 0
        for nid in ids_to_delete:
            await self.repo.delete_entity(nid)
            count += 1
            
        logger.info(f"Deleted {count} nodes for document {doc_id}")
        return count

    # -------------------------
    # Relationship logic
    # -------------------------

    async def add_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        logger.info("GraphService: adding %d relationships", len(relationships))
        for rel in relationships:
            self._validate_relationship(rel)
            await self.repo.create_relationship(
                from_id=rel["from"],
                to_id=rel["to"],
                label=rel["label"],
                properties=rel.get("properties"),
            )
            
    async def add_relationship(self, from_id: str, to_id: str, rel_type: str, properties: Dict[str, Any] = None):
        if not from_id or not to_id or not rel_type:
            raise ValueError("Missing required relationship fields")
        
        rel_data = {"from": from_id, "to": to_id, "label": rel_type, "properties": properties}
        self._validate_relationship(rel_data)
        
        return await self.repo.create_relationship(from_id, to_id, rel_type, properties)

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]):
        if not rel_id:
            raise ValueError("Relationship ID required")
        return await self.repo.update_relationship(rel_id, properties)

    async def delete_relationship(self, rel_id: str):
        return await self.repo.delete_relationship(rel_id)

    # -------------------------
    # Graph queries
    # -------------------------

    async def get_graph(self) -> Dict[str, Any]:
        """Fetch complete graph (normalized)."""
        logger.info("GraphService: fetching full graph")
        data = await self.repo.get_graph()

        # Normalize outgoing data
        nodes = data.get("nodes", data.get("entities", []))
        for node in nodes:
            props = node.get("properties", {})
            raw_type = node.get("type") or props.get("type", "")
            label = node.get("label", "")
            
            clean_type = normalize_entity_type(raw_type, label)
            
            node["type"] = clean_type
            if isinstance(props, dict):
                props["normType"] = clean_type

        return data

    # --- FIX 2: WRAPPER FOR DOCUMENT FILTERING ---
    async def get_graph_for_document(self, doc_id: str) -> Dict[str, Any]:
        """
        Fetches nodes/edges filtered by documentId.
        This is called when you select a file from the dropdown.
        """
        return await self.repo.fetch_combined_graph(document_id=doc_id, limit=2000)

    # --- FIX 3: WRAPPERS FOR SEARCH & STATS ---
    async def search_nodes(self, query: str):
        return await self.repo.search_nodes(query)

    async def get_stats(self):
        return await self.repo.get_stats()

    async def get_entities(self, label: Optional[str] = None):
        return await self.repo.get_entities(label=label)

    async def get_relationships(self):
        return await self.repo.get_relationships()

    async def get_relationships_for_entity(self, entity_id: str):
        return await self.repo.get_relationships_for_entity(entity_id)
        
    async def run_community_detection(self):
        return {"message": "Community detection not implemented on Cosmos DB yet."}

    # -------------------------
    # Internal validation
    # -------------------------

    def _validate_entity(self, entity: Dict[str, Any]) -> None:
        if "id" not in entity: raise ValueError("Entity missing required field: id")

    def _validate_relationship(self, relationship: Dict[str, Any]) -> None:
        required = ["from", "to", "label"]
        for field in required:
            if field not in relationship:
                raise ValueError(f"Relationship missing required field: {field}")

# Instantiate the service singleton
graph_service = GraphService()