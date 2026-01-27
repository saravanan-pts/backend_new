import logging
import uuid
import re
import pandas as pd
from typing import List, Dict, Any, Optional
from io import StringIO

from app.repositories.graph_repository import graph_repository
from app.utils.normalizer import normalize_entity_type
from app.services.document_processor import document_processor
from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

class GraphService:
    """
    FINAL GRAPH ENGINE (Strict Typing Fix)
    - Enforces clean Labels (Line 1).
    - Enforces strict Types (Line 2).
    - Fixes the "Paragraph in ID" bug.
    """

    def __init__(self):
        self.repo = graph_repository
        
        # 15-FILE SCHEMA
        self.RELATIONSHIP_SCHEMA = {
            ("case", "branch"): "PROCESSED_AT",
            ("case", "account_type"): "CLASSIFIED_AS",
            ("case", "product"): "CLASSIFIED_AS",
            ("case", "activity"): "PERFORMS_ACTIVITY",
            ("case", "customer"): "INITIATED_BY",
            ("case", "agent"): "HANDLED_BY",
            ("case", "outcome"): "RESULTED_IN",
            ("customer", "branch"): "BANKING_AT",
            ("customer", "account_type"): "HOLDS_ACCOUNT",
            ("account_number", "loan_amount"): "VALUED_AT",
            ("loan_type", "region"): "RECOVERS_IN",
            ("activity", "outcome"): "VALIDATES_OUTCOME"
        }

    # --- 1. STRICT ID GENERATOR ---
    def _clean_id(self, prefix: str, value: str) -> str:
        """Forces short, readable IDs."""
        clean_val = str(value).strip().lower()
        clean_val = re.sub(r'[^a-z0-9]', '_', clean_val)
        clean_val = re.sub(r'_+', '_', clean_val).strip('_')
        return f"{prefix.lower()}_{clean_val}"

    # --- 2. STRICT TYPE DETECTOR ---
    def _detect_type(self, header: str, value: str) -> str:
        h = header.lower()
        v = str(value).lower()
        if "case" in h or ("id" in h and v.startswith("a0")): return "Case"
        if "customer" in h: return "Customer"
        if "branch" in h: return "Branch"
        if "activity" in h or "action" in h: return "Activity"
        if "timestamp" in h or "date" in h: return "Time"
        if "product" in h or "account_type" in h: return "Product"
        if "amount" in h or "balance" in h: return "Amount"
        if "status" in h or "outcome" in h: return "Status"
        if re.match(r'^b\d+$', v): return "Branch"
        if re.match(r'^c\d+$', v): return "Customer"
        if re.match(r'\d{4}-\d{2}-\d{2}', v): return "Time"
        return "Concept"

    # --- 3. MAIN PROCESSOR ---
    async def process_narrative(self, narrative_text: str, filename: str) -> Dict[str, Any]:
        logger.info(f"Processing: {filename}")
        domain = filename.split('_')[0] if "_" in filename else "general"

        if filename.lower().endswith(".csv") or "," in narrative_text:
            return await self._process_csv_graph(narrative_text, filename, domain)
        
        return await self._process_unstructured_text(narrative_text, filename, domain)

    # --- 4. THE STAR-CHAIN ENGINE (Fixed Types) ---
    async def _process_csv_graph(self, csv_text: str, filename: str, domain: str):
        print(f"--- STAR-CHAIN ENGINE: Processing {filename} ---", flush=True)
        try:
            df = pd.read_csv(StringIO(csv_text))
        except:
            return {"error": "Invalid CSV"}

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        case_col = next((c for c in df.columns if "case" in c or "id" in c), df.columns[0])
        time_col = next((c for c in df.columns if "time" in c or "date" in c), None)
        act_col  = next((c for c in df.columns if "activity" in c or "action" in c), None)

        if time_col:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.sort_values(by=[case_col, time_col])

        all_entities = []
        all_relationships = []
        last_event_map = {} 

        total_rows = len(df)
        for idx, row in df.iterrows():
            if idx % 50 == 0: print(f"Processing row {idx}/{total_rows}...", flush=True)

            # --- A. NODES ---
            
            # 1. CASE NODE
            case_val = str(row[case_col]).strip()
            case_id = self._clean_id("case", case_val)
            all_entities.append({
                "id": case_id, 
                "label": case_val, # Clean Label
                "type": "Case",    # Clean Type
                "properties": {"normType": "Case", "domain": domain}
            })

            # 2. EVENT NODE (The Hub)
            # FIX: Type is strictly "Event". Label is strict "Activity Name".
            event_id = f"evt_{uuid.uuid4().hex[:8]}"
            act_val = str(row[act_col]) if act_col else "Event"
            time_val = str(row.get(time_col, ''))[:10]
            
            all_entities.append({
                "id": event_id, 
                "label": f"{act_val} ({time_val})", # This is Line 1
                "type": "Event",                    # This is Line 2 (The Fix)
                "properties": {
                    "normType": "Event", 
                    "domain": domain, 
                    "timestamp": time_val,
                    "activity_name": act_val
                }
            })

            # --- B. RELATIONSHIPS ---

            # Case -> Event
            all_relationships.append({"from": case_id, "to": event_id, "label": "HAS_EVENT", "properties": {"doc": filename}})

            # Event -> Next Event
            if case_id in last_event_map:
                prev_id = last_event_map[case_id]
                all_relationships.append({"from": prev_id, "to": event_id, "label": "NEXT_STEP", "properties": {"doc": filename, "type": "sequence"}})
            last_event_map[case_id] = event_id

            # Context Nodes
            row_nodes = {}
            for col in df.columns:
                val = str(row[col]).strip()
                if not val or val.lower() == "nan" or col == case_col: continue

                node_type = self._detect_type(col, val)
                node_id = self._clean_id(node_type, val) # Clean ID
                
                all_entities.append({
                    "id": node_id, 
                    "label": val,       # Clean Label
                    "type": node_type,  # Clean Type
                    "properties": {"normType": node_type}
                })
                row_nodes[node_type.lower()] = node_id

                rel = "INVOLVES"
                if node_type == "Activity": rel = "IS_ACTIVITY"
                elif node_type == "Time": rel = "OCCURRED_ON"
                elif node_type == "Branch": rel = "AT_LOCATION"
                
                all_relationships.append({"from": event_id, "to": node_id, "label": rel, "properties": {"doc": filename}})

            # Schema Shortcuts
            for n_type, n_id in row_nodes.items():
                if ("case", n_type) in self.RELATIONSHIP_SCHEMA:
                    rel_name = self.RELATIONSHIP_SCHEMA[("case", n_type)]
                    all_relationships.append({"from": case_id, "to": n_id, "label": rel_name, "properties": {"doc": filename, "type": "schema"}})
                
                for other_type, other_id in row_nodes.items():
                    if (n_type, other_type) in self.RELATIONSHIP_SCHEMA:
                        rel_name = self.RELATIONSHIP_SCHEMA[(n_type, other_type)]
                        all_relationships.append({"from": n_id, "to": other_id, "label": rel_name, "properties": {"doc": filename, "type": "schema"}})

        await self.add_entities(all_entities)
        await self.add_relationships(all_relationships)
        return {"filename": filename, "entities": len(all_entities)}

    # --- UTILS ---
    async def _process_unstructured_text(self, text, filename, domain):
        return {"status": "skipped", "msg": "AI Mode"}

    async def add_entities(self, entities):
        seen = set()
        for e in entities:
            if e["id"] not in seen:
                seen.add(e["id"])
                await self.repo.create_entity(e["id"], e["label"], e.get("properties", {}))

    async def add_relationships(self, relationships):
        for r in relationships:
            await self.repo.create_relationship(r["from"], r["to"], r["label"], r.get("properties", {}))

    async def get_graph(self): return await self.repo.get_graph()
    async def clear_graph(self, scope="all"): return await self.repo.clear_graph(scope)
    async def get_stats(self): return await self.repo.get_stats()
    async def search_nodes(self, q): return await self.repo.search_nodes(q)
    async def get_entities(self, label: Optional[str] = None): return await self.repo.get_entities(label=label)
    async def delete_document_data(self, doc_id: str): return 0

graph_service = GraphService()