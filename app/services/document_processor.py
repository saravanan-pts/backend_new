from typing import Dict, Any, List, Optional, Tuple
from fastapi import UploadFile
import pandas as pd
import json
import logging
import re
from app.utils.chunking import chunk_text
from app.services.openai_extractor import extract_entities_and_relationships
from app.services.graph_service import graph_service

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.graph_service = graph_service

    # --- 1. HYBRID SCHEMA DETECTION (Heuristic + Robust Fallback) ---
    def _detect_schema_roles(self, columns: List[str]) -> Dict[str, str]:
        """
        Hybrid Logic:
        1. Try Heuristics (Regex) to find Subject, Time, Activity.
        2. In a real 'AI Hybrid' version, if this returns None, 
           we would call an LLM here with the headers to ask for the mapping.
        """
        col_map = {c.lower().strip().replace("_", ""): c for c in columns}
        roles = {"subject": columns[0], "timestamp": None, "activity": None}

        # Heuristic 1: Subject (Case/ID)
        for key, original in col_map.items():
            if any(x in key for x in ['case', 'id', 'key', 'customer', 'order', 'ticket']):
                roles["subject"] = original
                break
        
        # Heuristic 2: Timeline (Timestamp/Date)
        for key, original in col_map.items():
            if any(x in key for x in ['timestamp', 'date', 'time', 'created']):
                roles["timestamp"] = original
                break

        # Heuristic 3: Action (Activity/Event)
        for key, original in col_map.items():
            if any(x in key for x in ['activity', 'action', 'status', 'event', 'message', 'code']):
                roles["activity"] = original
                break
        
        logger.info(f"Schema Roles Detected: {roles}")
        return roles

    def _clean_label(self, text: str) -> str:
        """Sanitizes labels for Entity Clustering (e.g. 'case_id' -> 'Case')"""
        if not text: return "Unknown"
        text = re.sub(r'(_id|_ID|_Id|_key|_KEY|_code|_CODE)$', '', text)
        return text.replace('_', ' ').strip().title()

    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        narratives = []
        columns = df.columns.tolist()
        
        # 1. Detect Schema
        roles = self._detect_schema_roles(columns)
        subj_col = roles["subject"]
        time_col = roles["timestamp"]
        act_col = roles["activity"]
        
        subj_label = self._clean_label(subj_col)

        # 2. Sort for Temporal Logic (Critical for Cause & Effect)
        if time_col:
            try:
                df[time_col] = pd.to_datetime(df[time_col])
                df = df.sort_values(by=[subj_col, time_col])
            except:
                logger.warning("Timestamp sort failed, proceeding with unsorted data.")

        # 3. Context Window for Causal Analysis
        # We track the 'previous row' to generate "A caused B" narratives
        prev_row = None
        
        for idx, row in df.iterrows():
            if row.isna().all(): continue
            
            subj_val = str(row[subj_col])
            row_text = []

            # --- Base Entity ---
            # "There is a Ticket identified as T-100."
            row_text.append(f"There is a '{subj_label}' identified as '{subj_val}'.")

            # --- Attributes (Hybrid Relationship Types) ---
            # The AI will read "The Ticket has a Priority of High" and create (Ticket)-[HAS_PRIORITY]->(High)
            for col in columns:
                if col in [subj_col, time_col, act_col]: continue
                val = row[col]
                if pd.notna(val):
                    clean_col = self._clean_label(col)
                    row_text.append(f"The '{subj_label}' '{subj_val}' has a '{clean_col}' of '{val}'.")

            # --- CAUSE AND EFFECT ENGINE ---
            if time_col and act_col and pd.notna(row.get(time_col)) and pd.notna(row.get(act_col)):
                curr_time = row[time_col]
                curr_act = row[act_col]
                
                # Narrative for current event
                row_text.append(f"At {curr_time}, the activity '{curr_act}' occurred.")

                # CHECK PREVIOUS EVENT (The "Context Window")
                if prev_row is not None and str(prev_row[subj_col]) == subj_val:
                    prev_act = prev_row[act_col]
                    
                    # 4. Engineering the Causal Prompt
                    # We explicitly ask the AI to judge the relationship between Prev and Curr
                    row_text.append(
                        f"SEQUENCE ANALYSIS: The event '{curr_act}' happened immediately after '{prev_act}'. "
                        f"Evaluate if '{prev_act}' logically caused or triggered '{curr_act}' based on operational risk patterns. "
                        "If yes, extract a 'CAUSED_BY' relationship. If it is just sequential, extract 'NEXT'."
                    )

            narratives.append(" ".join(row_text))
            prev_row = row

        return "\n\n".join(narratives)

    def _sanitize_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
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
        logger.info("Processing text input for file: %s", filename)
        
        # 5. ID Sanitization (Ensures DB Compatibility)
        # We clean filenames to be safe keys
        safe_filename = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)
        timestamp = int(pd.Timestamp.now().timestamp())
        doc_id = f"doc_{safe_filename}_{timestamp}"

        # Create Document Node
        doc_node = [{
            "id": doc_id,
            "label": "Document",
            "properties": {
                "filename": filename,
                "processed_at": str(pd.Timestamp.now()),
                "pk": "Document" 
            }
        }]
        await self.graph_service.add_entities(doc_node)

        # Chunking & Extraction
        chunks = chunk_text(text)
        extracted_entities = []
        extracted_relationships = []

        for chunk in chunks:
            # AI reads the "Causal Prompt" we generated above and makes the decision
            result = await extract_entities_and_relationships(chunk)
            extracted_entities.extend(result.get("entities", []))
            extracted_relationships.extend(result.get("relationships", []))

        # Normalize & Tag Entities
        entities = []
        for ent in extracted_entities:
            # 5. ID Sanitization for Graph Nodes
            clean_id = re.sub(r'[^a-zA-Z0-9_-]', '_', ent["label"].lower())
            
            sanitized_props = self._sanitize_properties(ent.get("properties", {}))
            sanitized_props["sourceDocumentId"] = doc_id 
            
            entities.append({
                "id": clean_id,
                "label": ent["label"], # Display Label (Human Readable)
                "properties": sanitized_props,
            })

        # Normalize & Tag Relationships
        relationships = []
        for rel in extracted_relationships:
            # 5. ID Sanitization for Edges
            from_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', rel["from"].lower())
            to_clean = re.sub(r'[^a-zA-Z0-9_-]', '_', rel["to"].lower())
            
            rel_props = {
                "confidence": rel.get("confidence"),
                "sourceDocumentId": doc_id
            }
            
            relationships.append({
                "from": from_clean,
                "to": to_clean,
                "label": rel["type"], # Could be 'CAUSED_BY' or 'NEXT' based on AI decision
                "properties": rel_props,
            })

        # Persist
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
        filename = file.filename
        logger.info("Processing file: %s", filename)

        if filename.lower().endswith(".csv"):
            df = pd.read_csv(file.file)
            text = self._csv_to_narrative(df)
        else:
            content = await file.read()
            text = content.decode("utf-8")

        return await self.process_text(text, filename=filename)

document_processor = DocumentProcessor()