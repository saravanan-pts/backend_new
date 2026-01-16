from typing import Dict, Any, List
from fastapi import UploadFile
import pandas as pd
import json
import logging
from app.utils.chunking import chunk_text
from app.services.openai_extractor import extract_entities_and_relationships
from app.services.graph_service import graph_service

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """
    Orchestrates document ingestion:
    - parse input
    - chunk text
    - extract entities & relationships via LLM
    - persist graph via GraphService (tagged with sourceDocumentId)
    """
    def __init__(self):
        # Business Logic: Use the singleton graph service
        self.graph_service = graph_service

    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        """
        [LOGIC RETAINED]: Converts CSV rows into sentences for the LLM.
        """
        narratives = []
        columns = df.columns.tolist()
        for idx, row in df.iterrows():
            if row.isna().all():
                continue
            row_text_parts = []
            for col in columns:
                value = row[col]
                if pd.notna(value):
                    row_text_parts.append(f"{col}: {value}")
            if row_text_parts:
                row_text = ", ".join(row_text_parts) + "."
                narratives.append(row_text)
        
        header = f"The following data contains information with columns: {', '.join(columns)}.\n\n"
        return header + "\n".join(narratives)

    def _sanitize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        [LOGIC RETAINED]: Ensures Cosmos DB only receives strings, numbers, or booleans.
        """
        sanitized = {}
        for key, value in properties.items():
            if isinstance(value, (list, dict)):
                sanitized[key] = json.dumps(value)
            elif isinstance(value, (str, int, float, bool)):
                sanitized[key] = value
            elif value is None:
                continue
            else:
                sanitized[key] = str(value)
        return sanitized

    async def process_text(self, text: str, filename: str = "raw_text") -> Dict[str, Any]:
        """
        [FIXED]: Changed to full async to prevent the 'RuntimeError'.
        All business logic for tagging and extraction is identical to your original.
        """
        logger.info("Processing text input for file: %s", filename)
        
        # Logic: Create Document metadata ID
        timestamp = int(pd.Timestamp.now().timestamp())
        doc_id = f"doc_{filename.replace('.', '_')}_{timestamp}"

        # Logic: Define Document Node
        doc_node = [{
            "id": doc_id,
            "label": "Document",
            "properties": {
                "filename": filename,
                "processed_at": str(pd.Timestamp.now()),
                "pk": "Document" 
            }
        }]
        
        # [FIX]: Use 'await' directly instead of run_in_threadpool
        await self.graph_service.add_entities(doc_node)

        # Logic: Chunking and LLM Extraction
        chunks = chunk_text(text)
        extracted_entities: List[Dict[str, Any]] = []
        extracted_relationships: List[Dict[str, Any]] = []

        for chunk in chunks:
            result = await extract_entities_and_relationships(chunk)
            extracted_entities.extend(result.get("entities", []))
            extracted_relationships.extend(result.get("relationships", []))

        # Logic: Normalize and Tag Entities with sourceDocumentId
        entities = []
        for ent in extracted_entities:
            sanitized_props = self._sanitize_properties(ent.get("properties", {}))
            sanitized_props["sourceDocumentId"] = doc_id 
            
            entities.append({
                "id": ent["label"].lower().replace(" ", "_"),
                "label": ent["label"],
                "properties": sanitized_props,
            })

        # Logic: Normalize and Tag Relationships with sourceDocumentId
        relationships = []
        for rel in extracted_relationships:
            rel_props = {
                "confidence": rel.get("confidence"),
                "sourceDocumentId": doc_id
            }
            
            relationships.append({
                "from": rel["from"].lower().replace(" ", "_"),
                "to": rel["to"].lower().replace(" ", "_"),
                "label": rel["type"],
                "properties": rel_props,
            })

        # [FIX]: Persist results using async await (Replacing threadpool)
        if entities:
            await self.graph_service.add_entities(entities)
        
        if relationships:
            await self.graph_service.add_relationships(relationships)

        return {
            "entities_added": len(entities),
            "relationships_added": len(relationships),
            "document_id": doc_id
        }

    async def process_file(self, file: UploadFile) -> Dict[str, Any]:
        """
        [LOGIC RETAINED]: Handles both CSV and Text files.
        """
        filename = file.filename
        logger.info("Processing file: %s", filename)

        if filename.lower().endswith(".csv"):
            df = pd.read_csv(file.file)
            text = self._csv_to_narrative(df)
        else:
            # Added 'await' for file reading (Standard FastAPI practice)
            content = await file.read()
            text = content.decode("utf-8")

        return await self.process_text(text, filename=filename)

# ---- Singleton ----
document_processor = DocumentProcessor()