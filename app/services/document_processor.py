import logging
import io
import re
import json
import pandas as pd
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """
    Handles parsing of raw files into text narratives.
    INTEGRATED SCHEMA: Uses the 'Relationship Schema' logic to force graph connectivity.
    """

    # --- 1. THE GOLDEN SCHEMA MAP (From your Excel) ---
    # Maps Column Headers -> (Relationship Verb, Entity Type)
    SCHEMA_MAP = {
        # Sales & Marketing
        "job": ("PROFILED_AS", "Job"),
        "marital": ("CATEGORIZED_BY", "Status"),
        "outcome": ("RESULTED_IN", "Outcome"),
        "education": ("QUALIFIED_AS", "Education"),
        
        # Banking / Accounts
        "account_type": ("CLASSIFIED_AS", "Product"),
        "branch_id": ("PROCESSED_AT", "Branch"),
        "opening_balance": ("INITIALIZED_WITH", "Amount"),
        "customer_id": ("INITIATED_BY", "Customer"),
        
        # Insurance / Claims
        "claim_amount": ("VALUED_AT", "Amount"),
        "policy_type": ("COVERED_UNDER", "Product"),
        "accident_type": ("CATEGORIZED_BY", "Accident"),
        "agent": ("HANDLED_BY", "Person"),
        "renewed": ("RESULTED_IN", "Status"),
        
        # Generic / Common
        "timestamp": ("OCCURRED_ON", "Time"),
        "date": ("OCCURRED_ON", "Time"),
        "activity": ("PERFORMS_ACTIVITY", "Activity"),
        "action": ("PERFORMS_ACTIVITY", "Activity")
    }

    # --- 2. ROBUST SCHEMA DETECTION ---
    def _detect_schema_roles(self, columns: List[str]) -> Dict[str, str]:
        col_map = {c.lower().strip().replace("_", ""): c for c in columns}
        roles = {"subject": columns[0], "timestamp": None, "activity": None}

        # Subject (Case/ID)
        for key, original in col_map.items():
            if any(x in key for x in ['case', 'id', 'key', 'customer', 'ticket', 'policy']):
                roles["subject"] = original
                break
        
        # Timestamp
        for key, original in col_map.items():
            if any(x in key for x in ['timestamp', 'date', 'time', 'created', 'at']):
                roles["timestamp"] = original
                break

        # Activity
        for key, original in col_map.items():
            if any(x in key for x in ['activity', 'action', 'status', 'event', 'message']):
                roles["activity"] = original
                break
        
        return roles

    def _clean_header(self, text: str) -> str:
        if not text: return "Unknown"
        text = re.sub(r'(_id|_ID|_Id|_key|_KEY|_code|_CODE)$', '', text)
        return text.replace('_', ' ').strip().title()

    # --- 3. STANDARDIZATION ---
    def standardize_label(self, label: str) -> str:
        if not label: return "Unknown"
        clean = label.strip()
        prefixes = ["Activity ", "Outcome ", "Status ", "Event ", "Job ", "Action ", "Case "]
        for prefix in prefixes:
            if clean.lower().startswith(prefix.lower()):
                clean = clean[len(prefix):].strip()
        
        # Special case: Don't strip 'Case' if it's the only word
        if clean.lower() == "case": return "Case"
        return clean.title()

    def generate_id(self, label: str) -> str:
        std_label = self.standardize_label(label)
        return re.sub(r'[^a-zA-Z0-9_-]', '_', std_label.lower()).strip('_')

    # --- 4. NARRATIVE GENERATION (SCHEMA DRIVEN) ---
    def _csv_to_narrative(self, df: pd.DataFrame) -> str:
        narratives = []
        columns = df.columns.tolist()
        roles = self._detect_schema_roles(columns)
        subj_col = roles["subject"]
        time_col = roles["timestamp"]
        act_col = roles["activity"]
        subj_label = self._clean_header(subj_col)

        # Sort for sequencing
        if time_col:
            try:
                df[time_col] = pd.to_datetime(df[time_col])
                df = df.sort_values(by=[subj_col, time_col])
            except: pass

        total_rows = len(df)
        logger.info(f"Generating narrative for {total_rows} rows using Schema Map...")
        
        prev_row = None
        
        for i, (idx, row) in enumerate(df.iterrows()):
            if total_rows <= 50 or (i + 1) % 50 == 0:
                logger.info(f"Processing row {i + 1}/{total_rows}...")

            if row.isna().all(): continue
            subj_val = str(row[subj_col])
            row_text = []
            
            # A. Identity
            # "There is a Case identified as Case 101."
            row_text.append(f"Entity: {subj_label} {subj_val}.")
            
            # B. Columns -> Relationships (Using SCHEMA_MAP)
            for col in columns:
                if col in [subj_col, time_col, act_col]: continue
                val = row[col]
                
                if pd.notna(val):
                    clean_col = self._clean_header(col)
                    clean_val = str(val).strip()
                    
                    # --- THE FIX: Lookup Column in Schema Map ---
                    col_key = col.lower().strip()
                    
                    if col_key in self.SCHEMA_MAP:
                        # Found in Schema! Use exact relationship and type.
                        rel_verb, entity_type = self.SCHEMA_MAP[col_key]
                        # Narrative: "Case 1 is PROFILED_AS Job 'Management'."
                        row_text.append(f"RELATIONSHIP: The {subj_label} {subj_val} is {rel_verb} {entity_type} '{clean_val}'.")
                    else:
                        # Fallback for unknown columns
                        row_text.append(f"RELATIONSHIP: The {subj_label} {subj_val} HAS_ATTRIBUTE {clean_col} '{clean_val}'.")

            # C. Activity -> Explicit Sequence
            if time_col and act_col and pd.notna(row.get(time_col)) and pd.notna(row.get(act_col)):
                curr_time = row[time_col]
                curr_act = row[act_col]
                
                # Link Activity to Case
                row_text.append(f"RELATIONSHIP: At {curr_time}, the {subj_label} {subj_val} PERFORMS_ACTIVITY '{curr_act}'.")
                
                # Link Activity to Time
                row_text.append(f"RELATIONSHIP: The Activity '{curr_act}' OCCURRED_ON Time '{curr_time}'.")

                # Sequence Logic
                if prev_row is not None and str(prev_row[subj_col]) == subj_val:
                    prev_act = prev_row[act_col]
                    row_text.append(f"SEQUENCE: The Activity '{curr_act}' happened after '{prev_act}'.")

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
        if "_" in base: 
            parts = base.split('_', 1)
            return parts[0], parts[1]
        return "general", base

    # --- MAIN PROCESSOR ---
    async def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        logger.info(f"DocumentProcessor: Parsing file {filename}")
        narrative_text = ""
        domain, doc_name = self._parse_filename(filename)

        if filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_content))
            narrative_text = self._csv_to_narrative(df)
        elif filename.lower().endswith(".txt"):
            narrative_text = file_content.decode("utf-8", errors="ignore")
        else:
            return {"error": "Unsupported file format", "nodes_created": 0}

        if not narrative_text:
            return {"nodes_created": 0, "edges_created": 0, "message": "Empty document"}

        logger.info(f"Generated narrative. Sending to GraphService...")
        from app.services.graph_service import graph_service
        try:
            return await graph_service.process_narrative(narrative_text, filename)
        except AttributeError:
             logger.warning("graph_service.process_narrative not found...")
             raise

    async def process_text(self, text: str) -> Dict[str, Any]:
        from app.services.graph_service import graph_service
        return await graph_service.process_narrative(text, "raw_input.txt")

document_processor = DocumentProcessor()