import logging
import uuid
import re
import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional
from io import StringIO

from app.config import settings
from app.repositories.graph_repository import graph_repository
from app.services.document_processor import document_processor
from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

class GraphService:
    """
    FINAL GRAPH ENGINE (Strict Isolation + Sequence Logic)
    - Fixes 'Spiderweb': Case IDs cannot be attributes.
    - Features: 
        1. Grouping: Click 'Branch B1' -> See all Cases.
        2. Sequence: Adds 'NEXT_STEP' edges between Activities (Process Mining).
    """

    def __init__(self):
        self.repo = graph_repository
        self.PARTITION_KEY = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")

    # ==========================================
    # 1. HELPER METHODS
    # ==========================================

    def _clean_id(self, prefix: str, value: str) -> str:
        clean_val = str(value).strip()
        safe_val = re.sub(r'[^a-zA-Z0-9]', '_', clean_val)
        return f"{prefix}_{safe_val}"

    def _detect_type(self, header: str, value: str) -> str:
        h = header.lower()
        v = str(value).lower()
        if "customer" in h: return "Customer"
        if "branch" in h: return "Branch"
        if "activity" in h or "action" in h: return "Activity"
        if "time" in h or "date" in h: return "Time"
        if "product" in h or "account_type" in h: return "Product"
        if "balance" in h or "amount" in h: return "Amount"
        
        if re.match(r'^b\d+$', v): return "Branch"
        if re.match(r'^c\d+$', v): return "Customer"
        if re.match(r'\d{4}-\d{2}-\d{2}', v): return "Time"
        return "Attribute"

    # ==========================================
    # 2. CRUD OPERATIONS (✅ ADDED THESE FIXES)
    # ==========================================

    async def add_relationship(self, from_id: str, to_id: str, rel_type: str, properties: Dict[str, Any] = None):
        """Creates a single edge (Used by UI 'Add Edge')."""
        return await self.repo.create_relationship(from_id, to_id, rel_type, properties)

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]):
        """Updates an existing edge."""
        return await self.repo.update_relationship(rel_id, properties)

    async def delete_relationship(self, rel_id: str):
        """Deletes an edge by ID."""
        return await self.repo.delete_relationship(rel_id)

    async def update_entity(self, entity_id: str, properties: Dict[str, Any], partition_key: str = None):
        """Updates an existing node's properties (Fixes UI Edit)."""
        # Ensure Partition Key is passed to repo so it updates the correct node
        if partition_key:
            properties[self.PARTITION_KEY] = partition_key
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str, partition_key: str = None):
        """Deletes a node by ID and Partition Key."""
        return await self.repo.delete_entity(entity_id, partition_key)

    # ==========================================
    # 3. CSV / GRAPH PROCESSING
    # ==========================================

    async def process_narrative(self, narrative_text: str, filename: str) -> Dict[str, Any]:
        logger.info(f"Processing: {filename}")
        domain = filename.split('_')[0] if "_" in filename else "general"

        if filename.lower().endswith(".csv") or "," in narrative_text:
            return await self._process_csv_graph(narrative_text, filename, domain)
        return await self._process_unstructured_text(narrative_text, filename, domain)

    async def _process_csv_graph(self, csv_text: str, filename: str, domain: str):
        print(f"--- PROCESS FLOW ENGINE: Processing {filename} ---", flush=True)
        try:
            df = pd.read_csv(StringIO(csv_text))
        except:
            return {"error": "Invalid CSV"}

        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        case_col = next((c for c in df.columns if "case" in c or "id" in c), df.columns[0])
        time_col = next((c for c in df.columns if "time" in c or "date" in c), None)
        act_col  = next((c for c in df.columns if "activity" in c or "action" in c), None)

        # 1. SORTING IS CRITICAL FOR 'NEXT_STEP' LOGIC
        if time_col:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.sort_values(by=[case_col, time_col])

        # 2. Ban-List to prevent Spiderweb
        all_case_ids_banlist = set(df[case_col].astype(str).str.strip().unique())

        all_entities = []
        all_relationships = []
        created_nodes = set()
        created_edges = set()

        # 3. Sequence Tracker: Stores { case_id: "Activity_Node_ID" }
        case_activity_tracker = {}

        # A. DOCUMENT NODE
        doc_id = filename
        all_entities.append({
            "id": doc_id, 
            "label": filename, 
            "type": "Document",
            "properties": {
                "normType": "Document", 
                "filename": filename, 
                "documentId": filename,
                "status": "processed",
                self.PARTITION_KEY: domain
            }
        })

        total_rows = len(df)
        
        for idx, row in df.iterrows():
            if idx % 50 == 0: print(f"Processing row {idx}/{total_rows}...", flush=True)

            # B. CASE NODE
            case_val = str(row[case_col]).strip()
            case_id = self._clean_id("Case", case_val)
            
            if case_id not in created_nodes:
                all_entities.append({
                    "id": case_id, 
                    "label": case_val, 
                    "type": "Case",      
                    "properties": {
                        "name": case_val, 
                        "normType": "Case", 
                        "domain": domain, 
                        "documentId": filename, 
                        self.PARTITION_KEY: domain 
                    }
                })
                # Link Doc -> Case
                edge_key = f"{doc_id}_{case_id}_CONTAINS"
                if edge_key not in created_edges:
                    all_relationships.append({"from": doc_id, "to": case_id, "label": "CONTAINS", "properties": {"doc": filename}})
                    created_edges.add(edge_key)
                created_nodes.add(case_id)

            # C. TRACK CURRENT ACTIVITY FOR SEQUENCE
            current_activity_id = None

            # D. PROCESS COLUMNS
            for col in df.columns:
                val = str(row[col]).strip()
                if not val or val.lower() == "nan": continue
                if col == case_col: continue # Skip Case Col
                if val in all_case_ids_banlist: continue # BAN-LIST CHECK

                node_type = self._detect_type(col, val)
                if node_type == "Amount": continue

                node_id = self._clean_id(node_type, val)

                # Capture Activity ID for Sequence Logic
                if node_type == "Activity":
                    current_activity_id = node_id

                # Create Context Node
                if node_id not in created_nodes:
                    all_entities.append({
                        "id": node_id, 
                        "label": val, 
                        "type": node_type,  
                        "properties": {
                            "name": val, 
                            "normType": node_type, 
                            "documentId": filename, 
                            self.PARTITION_KEY: domain
                        }
                    })
                    # Link Doc -> Context
                    edge_key = f"{doc_id}_{node_id}_HAS"
                    if edge_key not in created_edges:
                        all_relationships.append({"from": doc_id, "to": node_id, "label": f"HAS_{node_type.upper()}", "properties": {"doc": filename}})
                        created_edges.add(edge_key)
                    created_nodes.add(node_id)

                # Link Case -> Context
                rel_label = "LINKED_TO"
                if node_type == "Branch": rel_label = "MANAGED_BY"
                elif node_type == "Activity": rel_label = "PERFORMS"
                elif node_type == "Product": rel_label = "HAS_PRODUCT"
                elif node_type == "Customer": rel_label = "OWNED_BY"
                elif node_type == "Time": rel_label = "OCCURRED_ON"

                edge_unique_key = f"{case_id}_{node_id}_{rel_label}"
                
                if node_type == "Activity":
                    # Activities need history (allow duplicates)
                    time_val = str(row.get(time_col, ''))[:10]
                    all_relationships.append({
                        "from": case_id, 
                        "to": node_id, 
                        "label": rel_label, 
                        "properties": {"timestamp": time_val, "doc": filename}
                    })
                else:
                    if edge_unique_key not in created_edges:
                        all_relationships.append({
                            "from": case_id, 
                            "to": node_id, 
                            "label": rel_label, 
                            "properties": {"doc": filename}
                        })
                        created_edges.add(edge_unique_key)

            # E. APPLY SEQUENCE LOGIC (NEXT_STEP)
            # If we found an activity in this row, and we know the previous activity for this case:
            if current_activity_id:
                if case_id in case_activity_tracker:
                    previous_activity_id = case_activity_tracker[case_id]
                    
                    # Create Edge: Previous Activity -> Current Activity
                    # This builds the "Process Map" (Cause & Effect)
                    if previous_activity_id != current_activity_id:
                        # We use a global key because we want to see the general flow of the process
                        seq_key = f"{previous_activity_id}_{current_activity_id}_NEXT_STEP"
                        
                        # We add it if not exists (General Flow) OR we can add per case.
                        # Adding per case creates too many edges. Adding once shows the "Standard Process".
                        if seq_key not in created_edges:
                            all_relationships.append({
                                "from": previous_activity_id,
                                "to": current_activity_id,
                                "label": "NEXT_STEP",
                                "properties": {"doc": filename}
                            })
                            created_edges.add(seq_key)

                # Update tracker for the next row
                case_activity_tracker[case_id] = current_activity_id

        await self.add_entities(all_entities)
        await self.add_relationships(all_relationships)
        return {"filename": filename, "entities": len(all_entities)}

    async def _process_unstructured_text(self, text, filename, domain):
        return {"status": "skipped", "msg": "AI Mode not active"}

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
    
    # ✅ CONNECTED TO REPO (Was returning 0 before)
    async def delete_document_data(self, doc_id: str): 
        return await self.repo.delete_document_data(doc_id)

graph_service = GraphService()