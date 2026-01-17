import logging
import csv
import io
import uuid
import re
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from fastapi import UploadFile

from app.utils.chunking import chunk_text
from app.services.openai_extractor import extract_entities_and_relationships
from app.services.graph_service import graph_service
from app.utils.normalizer import normalize_entity_type  # <--- Ensure this is imported

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.graph_service = graph_service

    # --- 1. SCHEMA DETECTION (Keep your smart logic) ---
    def _detect_schema_roles(self, columns: List[str]) -> Dict[str, str]:
        col_map = {c.lower().strip().replace("_", ""): c for c in columns}
        roles = {"subject": columns[0], "timestamp": None, "activity": None}

        for key, original in col_map.items():
            if any(x in key for x in ['case', 'id', 'key', 'customer', 'order', 'ticket']):
                roles["subject"] = original
                break
        
        for key, original in col_map.items():
            if any(x in key for x in ['timestamp', 'date', 'time', 'created']):
                roles["timestamp"] = original
                break

        for key, original in col_map.items():
            if any(x in key for x in ['activity', 'action', 'status', 'event', 'message']):
                roles["activity"] = original
                break
        
        return roles

    def _clean_label(self, text: str) -> str:
        if not text: return "Unknown"
        text = re.sub(r'(_id|_ID|_Id|_key|_KEY|_code|_CODE)$', '', text)
        return text.replace('_', ' ').strip().title()

    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        # (Keep your existing narrative logic here - abbreviated for brevity)
        narratives = []
        columns = df.columns.tolist()
        roles = self._detect_schema_roles(columns)
        subj_col = roles["subject"]
        time_col = roles["timestamp"]
        act_col = roles["activity"]
        subj_label = self._clean_label(subj_col)

        if time_col:
            try:
                df[time_col] = pd.to_datetime(df[time_col])
                df = df.sort_values(by=[subj_col, time_col])
            except:
                pass

        prev_row = None
        for idx, row in df.iterrows():
            if row.isna().all(): continue
            subj_val = str(row[subj_col])
            row_text = []
            row_text.append(f"There is a '{subj_label}' identified as '{subj_val}'.")
            
            for col in columns:
                if col in [subj_col, time_col, act_col]: continue
                val = row[col]
                if pd.notna(val):
                    clean_col = self._clean_label(col)
                    row_text.append(f"The '{subj_label}' '{subj_val}' has a '{clean_col}' of '{val}'.")

            if time_col and act_col and pd.notna(row.get(time_col)) and pd.notna(row.get(act_col)):
                curr_time = row[time_col]
                curr_act = row[act_col]
                row_text.append(f"At {curr_time}, the activity '{curr_act}' occurred.")

                if prev_row is not None and str(prev_row[subj_col]) == subj_val:
                    prev_act = prev_row[act_col]
                    row_text.append(
                        f"SEQUENCE ANALYSIS: The event '{curr_act}' happened immediately after '{prev_act}'. "
                        f"Evaluate if '{prev_act}' logically caused '{curr_act}'."
                    )

            narratives.append(" ".join(row_text))
            prev_row = row

        return "\n\n".join(narratives)

    def _sanitize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}
        for key, value in properties.items():
            if isinstance(value, (list, dict)):
                sanitized[key] = json.dumps(value)
            elif value is None:
                continue
            else:
                sanitized[key] = str(value)
        return sanitized

    # --- 2. CORE PROCESSOR (With Document Tagging) ---
    async def process_text(self, text: str, filename: str = "raw_text") -> Dict[str, Any]:
        logger.info("Processing text input for file: %s", filename)
        
        # A. Create Unique Document ID
        doc_id = filename 

        # B. Chunk & Extract
        chunks = chunk_text(text)
        extracted_entities = []
        extracted_relationships = []

        for chunk in chunks:
            result = await extract_entities_and_relationships(chunk)
            extracted_entities.extend(result.get("entities", []))
            extracted_relationships.extend(result.get("relationships", []))

        # --- NEW: Calculate Counts ---
        node_count = len(extracted_entities)
        edge_count = len(extracted_relationships)

        # C. Create Parent Document Node
        doc_node = {
            "id": doc_id,
            "label": filename,
            "type": "Document",
            "properties": {
                "filename": filename,
                "uploadDate": str(pd.Timestamp.now()),
                "nodeCount": node_count,   # Save Node Count
                "edgeCount": edge_count,   # Save Edge Count
                "status": "processed",
                "normType": "Document"
            }
        }
        
        # D. Normalize & Tag Entities
        final_entities = [doc_node] 
        
        for ent in extracted_entities:
            clean_id = re.sub(r'[^a-zA-Z0-9_-]', '_', ent["label"].lower())
            props = self._sanitize_properties(ent.get("properties", {}))
            
            # TAGGING
            props["documentId"] = doc_id
            
            # --- CRITICAL FIX: FORCE NORMALIZATION ---
            raw_type = ent.get("type", "Concept")
            raw_label = ent.get("label", "")
            
            # This converts "A0001" -> "Account", "Collision" -> "Organization"
            clean_type = normalize_entity_type(raw_type, raw_label) 
            
            props["normType"] = clean_type # Store explicitly for frontend

            final_entities.append({
                "id": clean_id,
                "label": raw_label,
                "type": clean_type,  # Save the CLEAN type as the main type
                "properties": props,
            })

        # E. Normalize & Tag Relationships
        final_relationships = []
        for rel in extracted_relationships:
            from_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', rel["from"].lower())
            to_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', rel["to"].lower())
            
            rel_props = {
                "confidence": str(rel.get("confidence", 1.0)),
                "sourceDocumentId": doc_id
            }
            
            final_relationships.append({
                "from": from_clean,
                "to": to_clean,
                "label": rel["type"],
                "properties": rel_props,
            })

        # F. Persist
        if final_entities:
            await self.graph_service.add_entities(final_entities)
        
        if final_relationships:
            await self.graph_service.add_relationships(final_relationships)

        return {
            "filename": filename,
            "stats": {
                "nodes": len(final_entities),
                "edges": len(final_relationships)
            }
        }

    async def process_file(self, file: UploadFile) -> Dict[str, Any]:
        filename = file.filename
        content_bytes = await file.read()
        
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(io.StringIO(content_bytes.decode("utf-8")))
            text = self._csv_to_narrative(df)
        else:
            text = content_bytes.decode("utf-8")

        return await self.process_text(text, filename=filename)

document_processor = DocumentProcessor()