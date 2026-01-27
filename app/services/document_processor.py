import logging
import io
import re
import json
import pandas as pd
from typing import Dict, Any, List

# If you have the SchemaRegistry, keep this import. If not, you can remove it.
# from app.services.schema_registry import SchemaRegistry 

logger = logging.getLogger(__name__)

class DocumentProcessor:
    """
    Handles file parsing.
    CRITICAL UPDATE: For CSV files, this processor now bypasses narrative generation
    and sends RAW DATA to GraphService to enable the 'Star-Chain' Process Mining logic.
    """

    def _clean_header(self, text: str) -> str:
        if not text: return "Unknown"
        text = re.sub(r'(_id|_ID|_Id|_key|_KEY|_code|_CODE)$', '', text)
        return text.replace('_', ' ').strip().title()

    def standardize_label(self, label: str) -> str:
        if not label: return "Unknown"
        return str(label).strip().title()

    def generate_id(self, label: str) -> str:
        """Generates a clean, deterministic ID."""
        if not label: return "unknown"
        std_label = str(label).strip().lower()
        clean_id = re.sub(r'[^a-z0-9]', '_', std_label)
        return re.sub(r'_+', '_', clean_id).strip('_')

    def _parse_filename(self, filename: str):
        if '.' in filename: base = filename.rsplit('.', 1)[0]
        else: base = filename
        if "_" in base: 
            parts = base.split('_', 1)
            return parts[0], parts[1]
        return "general", base

    # --- MAIN PROCESSOR ---
    async def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        logger.info(f"DocumentProcessor: Processing {filename}")
        
        # 1. DECODE CONTENT
        try:
            text_content = file_content.decode("utf-8")
        except UnicodeDecodeError:
            text_content = file_content.decode("latin-1")

        # 2. ROUTING LOGIC
        from app.services.graph_service import graph_service

        if filename.lower().endswith(".csv"):
            # --- CRITICAL FIX ---
            # Do NOT convert CSV to narrative text sentences.
            # Pass the RAW CSV string directly to GraphService.
            # This enables the "Star-Chain" logic (Row -> Event -> Context).
            logger.info("Detected CSV: Sending raw data to Star-Chain Engine.")
            return await graph_service.process_narrative(text_content, filename)
        
        elif filename.lower().endswith(".txt") or filename.lower().endswith(".md"):
            # For text files, we pass the content as-is (GraphService will use AI)
            logger.info("Detected Text: Sending to AI Engine.")
            return await graph_service.process_narrative(text_content, filename)
            
        else:
            # Fallback for unknown types
            logger.warning(f"Unknown file type: {filename}. Attempting raw text processing.")
            return await graph_service.process_narrative(text_content, filename)

    async def process_text(self, text: str) -> Dict[str, Any]:
        from app.services.graph_service import graph_service
        return await graph_service.process_narrative(text, "raw_input.txt")

# Singleton
document_processor = DocumentProcessor()