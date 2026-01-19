import logging
import uuid
import pandas as pd
from typing import List, Dict, Any, Optional

# Repository and Normalizer
from app.repositories.graph_repository import graph_repository 
from app.utils.normalizer import normalize_entity_type

# IMPORTS (No Circular Dependency now)
from app.services.document_processor import document_processor
from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

class GraphService:
    """
    Service layer for graph-related business logic.
    Orchestrates repositories, applies rules (Normalization), and ensures consistency.
    """

    def __init__(self):
        self.repo = graph_repository

    # -------------------------
    # 1. File Processing (The Orchestrator)
    # -------------------------
    async def process_and_ingest_file(self, file_content: bytes, filename: str):
        """
        Orchestrates: Raw File -> Text -> Chunking -> AI -> Cleaning -> DB
        """
        logger.info(f"Starting processing for file: {filename}")

        # A. Get Narrative Text (Using DocumentProcessor)
        full_text = document_processor.process_file(file_content, filename)
        
        if not full_text:
            logger.warning(f"No text extracted from {filename}")
            return {"status": "empty", "filename": filename}

        # B. Chunking (Split into 4000 char blocks)
        CHUNK_SIZE = 4000
        chunks = [full_text[i:i+CHUNK_SIZE] for i in range(0, len(full_text), CHUNK_SIZE)]
        total_chunks = len(chunks)
        
        logger.info(f"Document split into {total_chunks} chunks.")

        all_entities = []
        all_relationships = []
        
        # Meta-data parsing
        domain, doc_id = document_processor._parse_filename(filename)

        # C. Processing Loop
        for i, chunk in enumerate(chunks):
            current_step = i + 1
            # Terminal Progress Bar (The Logic you requested)
            logger.info(f"Processing Chunk {current_step}/{total_chunks} ({len(chunk)} chars)...")
            
            try:
                # Call AI
                result = await extract_entities_and_relationships(chunk)
                extracted_entities = result.get("entities", [])
                extracted_rels = result.get("relationships", [])

                # --- D. APPLY CRITICAL CLEANING LOGIC ---
                # We use the helper functions from DocumentProcessor here
                
                # Clean Entities
                for ent in extracted_entities:
                    raw_label = ent.get("label", "")
                    
                    # Apply your Standardizer (e.g. remove "Activity ")
                    final_label = document_processor.standardize_label(raw_label)
                    
                    # Apply your Deterministic ID Generator
                    clean_id = document_processor.generate_id(final_label)
                    
                    # Update Entity
                    ent["id"] = clean_id
                    ent["label"] = final_label
                    
                    if "properties" not in ent: ent["properties"] = {}
                    ent["properties"]["documentId"] = doc_id
                    ent["properties"]["domain"] = domain
                    
                    # Sanitize properties using helper
                    ent["properties"] = document_processor._sanitize_properties(ent["properties"])

                    all_entities.append(ent)

                # Clean Relationships (Ensure IDs match Nodes)
                for rel in extracted_rels:
                    from_id = document_processor.generate_id(rel["from"])
                    to_id = document_processor.generate_id(rel["to"])
                    
                    rel["from"] = from_id
                    rel["to"] = to_id
                    
                    if "properties" not in rel: rel["properties"] = {}
                    rel["properties"]["documentId"] = doc_id
                    rel["properties"]["domain"] = domain
                    
                    all_relationships.append(rel)

            except Exception as e:
                logger.error(f"Error processing chunk {current_step}: {e}")
                continue

        # E. Create Document Parent Node
        doc_node = {
            "id": filename,
            "label": filename,
            "type": "Document",
            "properties": {
                "filename": filename,
                "uploadDate": str(pd.Timestamp.now()),
                "chunks": total_chunks,
                "nodeCount": len(all_entities),
                "domain": domain,
                "documentId": doc_id,
                "normType": "Document"
            }
        }
        all_entities.append(doc_node)

        # F. Bulk Save
        if all_entities:
            logger.info(f"Saving {len(all_entities)} entities and {len(all_relationships)} relationships...")
            await self.add_entities(all_entities)
            await self.add_relationships(all_relationships)
            logger.info(f"File '{filename}' successfully ingested!")
        else:
            logger.warning("AI found no entities.")

        return {
            "filename": filename,
            "chunks_processed": total_chunks,
            "entities_found": len(all_entities),
            "relationships_found": len(all_relationships)
        }

    # -------------------------
    # 2. Graph Lifecycle
    # -------------------------
    async def clear_graph(self, scope: str = "all") -> bool:
        """Clear graph data based on scope."""
        logger.info(f"GraphService: clearing graph with scope: {scope}")
        return await self.repo.clear_graph(scope)

    # -------------------------
    # 3. Entity Logic
    # -------------------------
    async def add_entities(self, entities: List[Dict[str, Any]]) -> None:
        """Add multiple entities with normalization."""
        logger.info("GraphService: adding %d entities", len(entities))

        for entity in entities:
            # Auto-generate ID if missing
            if "id" not in entity or not entity["id"]:
                entity["id"] = str(uuid.uuid4())

            # Normalization
            raw_type = entity.get("type", "Concept")
            raw_label = entity.get("label", str(entity.get("id", "")))
            
            clean_type = normalize_entity_type(raw_type, raw_label)
            
            if "properties" not in entity: entity["properties"] = {}
            
            # Store standardized type and original label
            entity["properties"]["type"] = clean_type
            entity["properties"]["normType"] = clean_type 
            entity["properties"]["label"] = raw_label 

            self._validate_entity(entity)
            
            # Persist
            await self.repo.create_entity(
                entity_id=entity["id"],
                label=clean_type,           
                properties=entity["properties"]
            )

    async def update_entity(self, entity_id: str, properties: Dict[str, Any]):
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str):
        return await self.repo.delete_entity(entity_id)

    # --- DOCUMENT DELETION (The Loop) ---
    async def delete_document_data(self, doc_id: str) -> int:
        """
        Deletes the document node AND all child nodes tagged with its ID.
        """
        logger.info(f"Starting cleanup for document: {doc_id}")
        
        all_entities = await self.repo.get_entities()
        ids_to_delete = []

        for entity in all_entities:
            nid = str(entity.get("id", ""))
            props = entity.get("properties", {})
            
            # Check A: Is this the document node itself?
            if nid == doc_id:
                ids_to_delete.append(nid)
                continue

            # Check B: Is this a child node?
            child_doc_ref = props.get("documentId")
            if isinstance(child_doc_ref, list) and len(child_doc_ref) > 0:
                child_doc_ref = child_doc_ref[0]
            
            if str(child_doc_ref) == doc_id:
                ids_to_delete.append(nid)

        count = 0
        for nid in ids_to_delete:
            await self.repo.delete_entity(nid)
            count += 1
            
        logger.info(f"Deleted {count} nodes for document {doc_id}")
        return count

    # -------------------------
    # 4. Relationship Logic
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
        if not rel_id: raise ValueError("Relationship ID required")
        return await self.repo.update_relationship(rel_id, properties)

    async def delete_relationship(self, rel_id: str):
        return await self.repo.delete_relationship(rel_id)

    # -------------------------
    # 5. Graph Queries
    # -------------------------
    async def get_graph(self) -> Dict[str, Any]:
        """Fetch complete graph (normalized)."""
        logger.info("GraphService: fetching full graph")
        data = await self.repo.get_graph()

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

    async def get_graph_for_document(self, doc_id: str) -> Dict[str, Any]:
        """Fetches nodes/edges filtered by documentId."""
        return await self.repo.fetch_combined_graph(document_id=doc_id, limit=2000)

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
    # 6. Internal Validation
    # -------------------------
    def _validate_entity(self, entity: Dict[str, Any]) -> None:
        if "id" not in entity: raise ValueError("Entity missing required field: id")

    def _validate_relationship(self, relationship: Dict[str, Any]) -> None:
        required = ["from", "to", "label"]
        for field in required:
            if field not in relationship:
                if field == "label" and "type" in relationship:
                    relationship["label"] = relationship["type"]
                    continue
                raise ValueError(f"Relationship missing required field: {field}")

# Singleton Instance
graph_service = GraphService()