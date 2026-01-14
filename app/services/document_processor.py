from typing import Dict, Any, List
from fastapi import UploadFile
import pandas as pd
import json
import logging
from app.utils.chunking import chunk_text
from app.services.openai_extractor import extract_entities_and_relationships
from app.services.graph_service import GraphService
from fastapi.concurrency import run_in_threadpool

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
        self.graph_service = GraphService()

    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        """
        Convert CSV DataFrame to narrative text that's easier for LLM to extract entities from.
        """
        narratives = []
        
        # Get column names
        columns = df.columns.tolist()
        
        # Convert each row to a sentence
        for idx, row in df.iterrows():
            # Skip rows with all NaN values
            if row.isna().all():
                continue
                
            # Create a descriptive sentence for each row
            row_text_parts = []
            for col in columns:
                value = row[col]
                if pd.notna(value):  # Only include non-null values
                    row_text_parts.append(f"{col}: {value}")
            
            if row_text_parts:
                # Join with commas and add period
                row_text = ", ".join(row_text_parts) + "."
                narratives.append(row_text)
        
        # Add a header context
        header = f"The following data contains information with columns: {', '.join(columns)}.\n\n"
        
        return header + "\n".join(narratives)

    def _sanitize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert non-primitive property values to strings.
        Azure Cosmos DB Gremlin only supports primitive types (string, number, boolean).
        """
        sanitized = {}
        for key, value in properties.items():
            if isinstance(value, (list, dict)):
                # Convert lists and dicts to JSON strings
                sanitized[key] = json.dumps(value)
            elif isinstance(value, (str, int, float, bool)):
                # Keep primitive types as-is
                sanitized[key] = value
            elif value is None:
                # Skip None values
                continue
            else:
                # Convert everything else to string
                sanitized[key] = str(value)
        return sanitized

    async def process_text(self, text: str, filename: str = "raw_text") -> Dict[str, Any]:
        """
        Processes text and tags all extracted data with a sourceDocumentId.
        """
        logger.info("Processing text input for file: %s", filename)
        
        # ---- NEW: Metadata Tracking ----
        # 1. Create a unique ID for this specific document record (The "Instruction Book ID")
        timestamp = int(pd.Timestamp.now().timestamp())
        doc_id = f"doc_{filename.replace('.', '_')}_{timestamp}"

        # 2. Create the 'Document' node first so we have a record of this upload
        doc_node = [{
            "id": doc_id,
            "label": "Document",
            "properties": {
                "filename": filename,
                "processed_at": str(pd.Timestamp.now()),
                "pk": "Document"  # Partition key for document metadata
            }
        }]
        # Save the metadata node immediately
        await run_in_threadpool(self.graph_service.add_entities, doc_node)

        # ---- Extraction Pipeline ----
        chunks = chunk_text(text)
        extracted_entities: List[Dict[str, Any]] = []
        extracted_relationships: List[Dict[str, Any]] = []

        # ---- AI extraction (async) ----
        for chunk in chunks:
            result = await extract_entities_and_relationships(chunk)
            extracted_entities.extend(result.get("entities", []))
            extracted_relationships.extend(result.get("relationships", []))

        # ---- Normalize and TAG entities ----
        entities = []
        for ent in extracted_entities:
            # Sanitize properties to ensure only primitives
            sanitized_props = self._sanitize_properties(ent.get("properties", {}))
            
            # NEW: Add the source tag so we know which file this node came from
            sanitized_props["sourceDocumentId"] = doc_id 
            
            entities.append(
                {
                    "id": ent["label"].lower().replace(" ", "_"),
                    "label": ent["label"],
                    "properties": sanitized_props,
                }
            )

        # ---- Normalize and TAG relationships ----
        relationships = []
        for rel in extracted_relationships:
            # NEW: Tag the relationship properties as well
            rel_props = {
                "confidence": rel.get("confidence"),
                "sourceDocumentId": doc_id
            }
            
            relationships.append(
                {
                    "from": rel["from"].lower().replace(" ", "_"),
                    "to": rel["to"].lower().replace(" ", "_"),
                    "label": rel["type"],
                    "properties": rel_props,
                }
            )

        # ---- Persist graph (blocking → threadpool) ----
        await run_in_threadpool(self.graph_service.add_entities, entities)
        await run_in_threadpool(
            self.graph_service.add_relationships, relationships
        )

        return {
            "entities_added": len(entities),
            "relationships_added": len(relationships),
            "document_id": doc_id
        }

    async def process_file(self, file: UploadFile) -> Dict[str, Any]:
        filename = file.filename
        logger.info("Processing file: %s", filename)

        if filename.lower().endswith(".csv"):
            # Read CSV into DataFrame
            df = pd.read_csv(file.file)
            
            # Convert to narrative text for better LLM extraction
            text = self._csv_to_narrative(df)
            logger.info("Converted CSV to narrative text with %d rows", len(df))
        else:
            # Plain text file
            text = (await file.read()).decode("utf-8")

        # Pass the filename down so we can create the metadata tag
        return await self.process_text(text, filename=filename)


# ---- Singleton ----
document_processor = DocumentProcessor()