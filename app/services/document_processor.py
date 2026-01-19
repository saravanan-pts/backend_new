import logging
import io
import re
import json
import pandas as pd
from typing import Dict, Any, List
from fastapi import UploadFile

# REMOVED: from app.services.graph_service import graph_service (Fixes Crash)
# REMOVED: from app.services.openai_extractor ... (Moved to GraphService)

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
    # Made PUBLIC (removed underscore) so GraphService can use it
    def standardize_label(self, label: str) -> str:
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
        if clean.lower() == "case": 
            return "Case" 
        
        return clean.title()

    # Made PUBLIC (removed underscore) so GraphService can use it
    def generate_id(self, label: str) -> str:
        """
        Deterministic ID Generation.
        Guarantees that 'Sale Closed' always results in 'sale_closed'.
        """
        # 1. First, standardize the label
        std_label = self.standardize_label(label)
        
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
        if '.' in filename: base = filename.rsplit('.', 1)[0]
        else: base = filename
        if "_" in base: parts = base.split('_', 1); return parts[0], parts[1]
        return "general", base

    # --- MAIN PARSER (Returns Text Only) ---
    def process_file(self, file_content: bytes, filename: str) -> str:
        """
        Parses the file and returns the narrative text.
        GraphService will handle the Loop, AI call, and Saving.
        """
        logger.info(f"DocumentProcessor: Parsing file {filename}")
        
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_content))
            return self._csv_to_narrative(df)
        
        elif filename.lower().endswith(".txt"):
            return file_content.decode("utf-8", errors="ignore")
            
        else:
            return ""

document_processor = DocumentProcessor()