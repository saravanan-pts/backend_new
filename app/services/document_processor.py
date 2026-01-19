import logging
import io
import re
import json
import pandas as pd
from typing import Dict, Any, List
from fastapi import UploadFile

from app.utils.chunking import chunk_text
from app.services.openai_extractor import extract_entities_and_relationships
from app.services.graph_service import graph_service
from app.utils.normalizer import normalize_entity_type

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.graph_service = graph_service

    # --- 1. ROBUST SCHEMA DETECTION ---
    def _detect_schema_roles(self, columns: List[str]) -> Dict[str, str]:
        """
        Scans columns to identify Subject, Timestamp, and Activity fields.
        """
        col_map = {c.lower().strip().replace("_", ""): c for c in columns}
        roles = {"subject": columns[0], "timestamp": None, "activity": None}

        # 1. Detect Subject (Case ID, Customer, etc.)
        for key, original in col_map.items():
            if any(x in key for x in ['case', 'id', 'key', 'customer', 'order', 'ticket', 'policy']):
                roles["subject"] = original
                break
        
        # 2. Detect Timestamp
        for key, original in col_map.items():
            if any(x in key for x in ['timestamp', 'date', 'time', 'created', 'at']):
                roles["timestamp"] = original
                break

        # 3. Detect Activity/Status
        for key, original in col_map.items():
            if any(x in key for x in ['activity', 'action', 'status', 'event', 'message', 'type']):
                roles["activity"] = original
                break
        
        return roles

    def _clean_header(self, text: str) -> str:
        """Cleans CSV headers for narrative generation."""
        if not text: return "Unknown"
        text = re.sub(r'(_id|_ID|_Id|_key|_KEY|_code|_CODE)$', '', text)
        return text.replace('_', ' ').strip().title()

    # --- 2. CRITICAL STANDARDIZATION LOGIC (THE FIX) ---
    def _standardize_label(self, label: str) -> str:
        """
        The Master Cleaning Function.
        Merges 'Activity Sale Closed' -> 'Sale Closed'.
        Merges 'Outcome No Result' -> 'No Result'.
        """
        if not label: return "Unknown"
        clean = label.strip()
        
        # List of prefixes to strip out to ensure merging
        prefixes = ["Activity ", "Outcome ", "Status ", "Event ", "Job ", "Action "]
        
        for prefix in prefixes:
            if clean.lower().startswith(prefix.lower()):
                clean = clean[len(prefix):].strip()

        # Special Exception: Don't strip 'Case' if it's strictly "Case"
        # We want "Case 1", "Case 2", but maybe just "Case" is fine.
        if clean.lower() == "case": 
            return "Case" 
        
        return clean.title()

    def _generate_id(self, label: str) -> str:
        """
        Deterministic ID Generation.
        This guarantees that 'Sale Closed' always results in 'sale_closed',
        whether it comes from a Node label or a Relationship target.
        """
        # 1. First, standardize the label (Remove 'Activity', etc.)
        std_label = self._standardize_label(label)
        
        # 2. Convert to strict ID format (lowercase, underscores)
        clean_id = re.sub(r'[^a-zA-Z0-9_-]', '_', std_label.lower()).strip('_')
        
        return clean_id

    # --- 3. NARRATIVE GENERATION (CSV -> Text) ---
    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        narratives = []
        columns = df.columns.tolist()
        roles = self._detect_schema_roles(columns)
        subj_col = roles["subject"]
        time_col = roles["timestamp"]
        act_col = roles["activity"]
        subj_label = self._clean_header(subj_col)

        # Sort by timestamp if available to detect sequences
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
            
            # Base Context
            row_text.append(f"There is a '{subj_label}' identified as '{subj_val}'.")
            
            # Attribute Description
            for col in columns:
                if col in [subj_col, time_col, act_col]: continue
                val = row[col]
                if pd.notna(val):
                    clean_col = self._clean_header(col)
                    row_text.append(f"The '{subj_label}' '{subj_val}' has a '{clean_col}' of '{val}'.")

            # Activity & Sequence Logic
            if time_col and act_col and pd.notna(row.get(time_col)) and pd.notna(row.get(act_col)):
                curr_time = row[time_col]
                curr_act = row[act_col]
                
                row_text.append(f"At {curr_time}, the event '{curr_act}' occurred.")

                # Sequence detection: If same subject, link previous act to current
                if prev_row is not None and str(prev_row[subj_col]) == subj_val:
                    prev_act = prev_row[act_col]
                    row_text.append(
                        f"SEQUENCE: '{curr_act}' happened after '{prev_act}'."
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

    def _parse_filename(self, filename: str):
        if '.' in filename: base = filename.rsplit('.', 1)[0]
        else: base = filename
        if "_" in base: parts = base.split('_', 1); return parts[0], parts[1]
        return "general", base

    # --- 4. CORE PROCESSING LOGIC ---
    async def process_text(self, text: str, filename: str = "raw_text") -> Dict[str, Any]:
        logger.info("Processing text input for file: %s", filename)
        
        domain, documentId = self._parse_filename(filename)
        chunks = chunk_text(text)
        extracted_entities = []
        extracted_relationships = []

        # 1. Extract Entities via AI
        for chunk in chunks:
            result = await extract_entities_and_relationships(chunk)
            extracted_entities.extend(result.get("entities", []))
            extracted_relationships.extend(result.get("relationships", []))

        node_count = len(extracted_entities)
        edge_count = len(extracted_relationships)

        # 2. Create Document Parent Node
        doc_node = {
            "id": filename, 
            "label": filename,
            "type": "Document",
            "properties": {
                "filename": filename,
                "uploadDate": str(pd.Timestamp.now()),
                "nodeCount": node_count,
                "edgeCount": edge_count,
                "status": "processed",
                "normType": "Document",
                "domain": domain,         
                "documentId": documentId  
            }
        }
        
        final_entities = [doc_node] 
        
        # 3. Process & Standardize Entities
        for ent in extracted_entities:
            raw_type = ent.get("type", "Concept")
            raw_label = ent.get("label", "")
            
            # Step A: Standardize Label (Merge duplicates)
            final_label = self._standardize_label(raw_label)
            
            # Step B: Generate Deterministic ID
            clean_id = self._generate_id(final_label)
            
            # Step C: Normalize Type
            clean_type = normalize_entity_type(raw_type, raw_label)

            props = self._sanitize_properties(ent.get("properties", {}))
            props["documentId"] = documentId 
            props["domain"] = domain
            props["normType"] = clean_type

            final_entities.append({
                "id": clean_id,
                "label": final_label,
                "type": clean_type,
                "properties": props,
            })

        # 4. Process Relationships (Using Deterministic IDs)
        final_relationships = []
        for rel in extracted_relationships:
            # CRITICAL FIX:
            # We must run the exact same cleaning on the Source/Target strings
            # that we ran on the Node Labels. This ensures the IDs match.
            
            # If AI says: from "Activity Sale Closed" -> to "Activity Outbound"
            # _generate_id converts "Activity Sale Closed" -> "sale_closed"
            # _generate_id converts "Activity Outbound" -> "outbound"
            # These match the Node IDs created in Step 3 exactly.
            
            from_id = self._generate_id(rel["from"])
            to_id = self._generate_id(rel["to"])
            
            rel_props = {
                "confidence": str(rel.get("confidence", 1.0)),
                "documentId": documentId,
                "domain": domain
            }
            
            final_relationships.append({
                "from": from_id,
                "to": to_id,
                "label": rel["type"],
                "properties": rel_props,
            })

        # 5. Save to Database
        if final_entities:
            await self.graph_service.add_entities(final_entities)
        
        if final_relationships:
            await self.graph_service.add_relationships(final_relationships)

        return {
            "filename": filename,
            "stats": {"nodes": len(final_entities), "edges": len(final_relationships)}
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