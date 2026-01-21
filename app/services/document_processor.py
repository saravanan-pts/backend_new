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
    Provides logic for ID generation and Label standardization.
    """

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

    # --- 2. CRITICAL STANDARDIZATION LOGIC ---
    def standardize_label(self, label: str) -> str:
        """
        The Master Cleaning Function.
        Merges 'Activity Sale Closed' -> 'Sale Closed'.
        """
        if not label: return "Unknown"
        clean = label.strip()
        
        prefixes = ["Activity ", "Outcome ", "Status ", "Event ", "Job ", "Action "]
        for prefix in prefixes:
            if clean.lower().startswith(prefix.lower()):
                clean = clean[len(prefix):].strip()

        if clean.lower() == "case": 
            return "Case" 
        
        return clean.title()

    def generate_id(self, label: str) -> str:
        """
        Deterministic ID Generation.
        """
        std_label = self.standardize_label(label)
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

        # --- PROGRESS LOGGING LOGIC ---
        total_rows = len(df)
        logger.info(f"Starting narrative generation for {total_rows} rows...")
        
        prev_row = None
        
        for i, (idx, row) in enumerate(df.iterrows()):
            # Log every row if small file (<50), otherwise log every 50 rows
            if total_rows <= 50 or (i + 1) % 50 == 0:
                logger.info(f"Processing row {i + 1}/{total_rows}...")

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

                # Sequence detection
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
        """
        Extracts Domain and Base Filename.
        Example: "Trading_claims_log.csv" -> ("Trading", "claims_log")
        Example: "claims_log.csv" -> ("general", "claims_log")
        """
        if '.' in filename: base = filename.rsplit('.', 1)[0]
        else: base = filename
        
        if "_" in base: 
            parts = base.split('_', 1)
            return parts[0], parts[1] # Domain, DocumentName
        
        return "general", base

    # --- MAIN PROCESSOR (Returns Stats) ---
    async def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        1. Parses the file into narrative text.
        2. Sends text to GraphService for AI processing.
        3. Returns statistics (nodes/edges count).
        """
        logger.info(f"DocumentProcessor: Parsing file {filename}")
        
        narrative_text = ""
        
        # 1. Extract Domain from Filename
        domain, doc_name = self._parse_filename(filename)

        # 2. Convert File to Text
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_content))
            narrative_text = self._csv_to_narrative(df)
        
        elif filename.lower().endswith(".txt"):
            narrative_text = file_content.decode("utf-8", errors="ignore")
            
        else:
            return {"error": "Unsupported file format", "nodes_created": 0}

        if not narrative_text:
            return {"nodes_created": 0, "edges_created": 0, "message": "Empty document"}

        logger.info(f"Generated narrative of length {len(narrative_text)}. Sending to GraphService (Domain: {domain})...")

        # 3. Lazy Import GraphService
        from app.services.graph_service import graph_service

        # 4. Process the Narrative
        try:
            stats = await graph_service.process_narrative(narrative_text, filename)
            return stats
        except AttributeError:
             logger.warning("graph_service.process_narrative not found, trying process_content...")
             raise

    async def process_text(self, text: str) -> Dict[str, Any]:
        """Handles raw text input from the frontend."""
        from app.services.graph_service import graph_service
        return await graph_service.process_narrative(text, "raw_input.txt")

document_processor = DocumentProcessor()