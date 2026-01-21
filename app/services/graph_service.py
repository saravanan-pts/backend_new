import logging
import uuid
import re
import pandas as pd
from typing import List, Dict, Any, Optional

# Repository and Normalizer
from app.repositories.graph_repository import graph_repository 
from app.utils.normalizer import normalize_entity_type

# IMPORTS
from app.services.document_processor import document_processor
from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

class GraphService:
    """
    Service layer for graph-related business logic.
    Uses a 'Hybrid' approach: AI for extraction + Hardcoded Rules for strict typing.
    """

    def __init__(self):
        self.repo = graph_repository

    # -------------------------
    # 0. HYBRID SCHEMA MAPPER
    # -------------------------
    def _apply_hybrid_typing(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Double-Lock Validation:
        Overrides generic AI types (like 'Concept') with specific business types
        based on known keywords from your CSV data.
        """
        label = entity.get("label", "").strip()
        
        # 1. Detect Activities (Exact Matches)
        activities = [
            "Outbound Call Started", "Call Ended (No Sale)", "Sale Closed", 
            "Follow Up", "Meeting Booked"
        ]
        if label in activities:
            entity["type"] = "Activity"
            return entity

        # 2. Detect Jobs/Roles
        jobs = [
            "management", "blue-collar", "technician", "admin.", 
            "services", "retired", "self-employed", "unemployed", 
            "entrepreneur", "housemaid", "student"
        ]
        if label.lower() in jobs:
            entity["type"] = "Job"
            return entity

        # 3. Detect Marital Status
        statuses = ["married", "single", "divorced", "widowed"]
        if label.lower() in statuses:
            entity["type"] = "MaritalStatus"
            return entity

        # 4. Detect Outcomes
        outcomes = ["No_Result", "failure", "success", "other"]
        if label in outcomes:
            entity["type"] = "Outcome"
            return entity

        # 5. Detect Case IDs (Numeric strings)
        if label.isdigit():
            entity["type"] = "Case"
            entity["label"] = f"Case {label}" 

        return entity

    # -------------------------
    # 1. Narrative Processing (With Live Updates)
    # -------------------------
    async def process_narrative(self, narrative_text: str, filename: str) -> Dict[str, Any]:
        """
        Orchestrates: Text Narrative -> Chunking -> AI Extraction -> Hybrid Typing -> DB Saving.
        """
        logger.info(f"Starting graph processing for file: {filename}")

        if not narrative_text:
            logger.warning(f"No text provided for {filename}")
            return {"status": "empty", "filename": filename}

        # A. Chunking
        CHUNK_SIZE = 4000
        chunks = [narrative_text[i:i+CHUNK_SIZE] for i in range(0, len(narrative_text), CHUNK_SIZE)]
        total_chunks = len(chunks)
        
        # Force print to ensure visibility immediately
        print(f"--- Document split into {total_chunks} chunks. Starting AI... ---", flush=True)

        all_entities = []
        all_relationships = []
        
        # Meta-data parsing
        domain = "general"
        doc_id = filename
        
        try:
            domain, doc_id = document_processor._parse_filename(filename)
        except:
            if "_" in filename:
                parts = filename.split('_', 1)
                domain = parts[0]
                doc_id = parts[1]

        # B. Processing Loop
        for i, chunk in enumerate(chunks):
            current_step = i + 1
            
            # --- LIVE UPDATE FIX: FORCE FLUSH ---
            # This ensures the log appears instantly in the terminal
            print(f"--> Processing Chunk {current_step}/{total_chunks} ({len(chunk)} chars)...", flush=True)
            
            try:
                # Call AI
                result = await extract_entities_and_relationships(chunk)
                extracted_entities = result.get("entities", [])
                extracted_rels = result.get("relationships", [])

                # --- C. HYBRID PROCESSING & CLEANING ---
                
                # Clean Entities
                for ent in extracted_entities:
                    # 1. Apply Hybrid Typing (Double-Lock)
                    ent = self._apply_hybrid_typing(ent)

                    raw_label = ent.get("label", "")
                    
                    # 2. Standardize Label
                    final_label = document_processor.standardize_label(raw_label)
                    
                    # 3. Generate ID
                    clean_id = document_processor.generate_id(final_label)
                    
                    ent["id"] = clean_id
                    ent["label"] = final_label
                    
                    if "properties" not in ent: ent["properties"] = {}
                    ent["properties"]["documentId"] = doc_id
                    ent["properties"]["domain"] = domain
                    
                    # 4. Sanitize
                    ent["properties"] = document_processor._sanitize_properties(ent["properties"])

                    all_entities.append(ent)

                # Clean Relationships
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
                print(f"Error on chunk {current_step}: {e}", flush=True)
                continue

        # D. Create Document Parent Node
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

        # E. Bulk Save
        if all_entities:
            print(f"--- Saving {len(all_entities)} entities to DB... ---", flush=True)
            await self.add_entities(all_entities)
            await self.add_relationships(all_relationships)
            print(f"--- Success! '{filename}' ingested. ---", flush=True)
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
        logger.info(f"GraphService: clearing graph with scope: {scope}")
        return await self.repo.clear_graph(scope)

    # -------------------------
    # 3. Entity Logic
    # -------------------------
    async def add_entities(self, entities: List[Dict[str, Any]]) -> None:
        logger.info("GraphService: adding %d entities", len(entities))

        for entity in entities:
            if "id" not in entity or not entity["id"]:
                entity["id"] = str(uuid.uuid4())

            # Normalization
            raw_type = entity.get("type", "Concept")
            raw_label = entity.get("label", str(entity.get("id", "")))
            
            clean_type = normalize_entity_type(raw_type, raw_label)
            
            if "properties" not in entity: entity["properties"] = {}
            
            entity["properties"]["type"] = clean_type
            entity["properties"]["normType"] = clean_type 
            entity["properties"]["label"] = raw_label 

            self._validate_entity(entity)
            
            await self.repo.create_entity(
                entity_id=entity["id"],
                label=clean_type,            
                properties=entity["properties"]
            )

    async def update_entity(self, entity_id: str, properties: Dict[str, Any]):
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str):
        return await self.repo.delete_entity(entity_id)

    # --- DOCUMENT DELETION ---
    async def delete_document_data(self, doc_id: str) -> int:
        logger.info(f"Starting cleanup for document: {doc_id}")
        
        all_entities = await self.repo.get_entities()
        ids_to_delete = []

        for entity in all_entities:
            nid = str(entity.get("id", ""))
            props = entity.get("properties", {})
            
            if nid == doc_id:
                ids_to_delete.append(nid)
                continue

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