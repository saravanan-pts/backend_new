import logging
import uuid
import re
import asyncio
import pandas as pd
from typing import List, Dict, Any, Optional
from io import StringIO

from app.config import settings
from app.repositories.graph_repository import graph_repository
# Note: document_processor import removed from top to avoid circular dependency
from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

# --- OPERATIONAL CATEGORIES (DB SOURCE OF TRUTH) ---
CAUSE_LABELS = ['CAUSE', 'LED_TO', 'CAUSES', 'CAUSED', 'TRIGGERED', 'SOURCE_OF', 'PRECEDED_BY']
EFFECT_LABELS = ['EFFECT', 'RESULTED_IN', 'IMPACTED', 'AFFECTED', 'CONSEQUENCE_OF', 'HAS_EFFECT']
SEQUENCE_LABELS = ['NEXT_STEP', 'FOLLOWED_BY', 'PRECEDES', 'THEN']

class GraphService:
    """
    FINAL GRAPH ENGINE (Active Ingestion & Process Mining)
    - Auto-tags 'riskCategory' (Cause, Effect, or Process) during data load.
    - Implements Star-Chain Sequence Logic with 3 Distinct Edge Types.
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

    def _determine_risk_category(self, label: str) -> str:
        """
        Operational Intelligence:
        Maps edge labels to 'Cause', 'Effect', or 'Process' and SAVES TO DB.
        """
        clean = str(label).upper().strip()
        if clean in CAUSE_LABELS:
            return 'Cause'
        if clean in EFFECT_LABELS:
            return 'Effect'
        if clean in SEQUENCE_LABELS:
            return 'Process'
        return ''

    # ==========================================
    # 2. CRUD OPERATIONS
    # ==========================================

    async def add_relationship(self, from_id: str, to_id: str, rel_type: str, properties: Dict[str, Any] = None):
        """Creates a single edge with auto-risk tagging."""
        if properties is None: properties = {}
        
        # Auto-enrich with Risk Category before saving to DB
        risk_cat = self._determine_risk_category(rel_type)
        if risk_cat:
            properties['riskCategory'] = risk_cat
            
        return await self.repo.create_relationship(from_id, to_id, rel_type, properties)

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]):
        return await self.repo.update_relationship(rel_id, properties)

    async def delete_relationship(self, rel_id: str):
        return await self.repo.delete_relationship(rel_id)

    async def update_entity(self, entity_id: str, properties: Dict[str, Any], partition_key: str = None):
        if partition_key:
            properties[self.PARTITION_KEY] = partition_key
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str, partition_key: str = None):
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
        
        # 1. SORTING (Critical for Process Sequence)
        if time_col:
            df[time_col] = pd.to_datetime(df[time_col])
            df = df.sort_values(by=[case_col, time_col])

        # 2. Ban-List
        all_case_ids_banlist = set(df[case_col].astype(str).str.strip().unique())

        # --- CHANGE 1: Use a Dictionary for Nodes to allow property updates ---
        all_entities_map = {} 
        all_relationships = []
        created_edges = set()

        # 3. Sequence Tracker
        case_activity_tracker = {}
        case_activity_labels = {} # Track labels for human-readable properties

        # A. DOCUMENT NODE
        doc_id = filename
        all_entities_map[doc_id] = {
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
        }

        total_rows = len(df)
        
        for idx, row in df.iterrows():
            if idx % 50 == 0: print(f"Processing row {idx}/{total_rows}...", flush=True)

            # B. CASE NODE
            case_val = str(row[case_col]).strip()
            case_id = self._clean_id("Case", case_val)
            
            if case_id not in all_entities_map:
                all_entities_map[case_id] = {
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
                }
                # Link Doc -> Case
                edge_key = f"{doc_id}_{case_id}_CONTAINS"
                if edge_key not in created_edges:
                    all_relationships.append({"from": doc_id, "to": case_id, "label": "CONTAINS", "properties": {"doc": filename}})
                    created_edges.add(edge_key)

            # C. TRACK CURRENT ACTIVITY
            current_activity_id = None
            current_activity_label = ""

            # D. PROCESS COLUMNS
            for col in df.columns:
                val = str(row[col]).strip()
                if not val or val.lower() == "nan": continue
                if col == case_col: continue 
                if val in all_case_ids_banlist: continue 

                node_type = self._detect_type(col, val)
                if node_type == "Amount": continue

                node_id = self._clean_id(node_type, val)

                if node_type == "Activity":
                    current_activity_id = node_id
                    current_activity_label = val

                # Create Context Node
                if node_id not in all_entities_map:
                    all_entities_map[node_id] = {
                        "id": node_id, 
                        "label": val, 
                        "type": node_type,  
                        "properties": {
                            "name": val, 
                            "normType": node_type, 
                            "documentId": filename, 
                            self.PARTITION_KEY: domain
                        }
                    }
                    # Link Doc -> Context
                    edge_key = f"{doc_id}_{node_id}_HAS"
                    if edge_key not in created_edges:
                        all_relationships.append({"from": doc_id, "to": node_id, "label": f"HAS_{node_type.upper()}", "properties": {"doc": filename}})
                        created_edges.add(edge_key)

                # Link Case -> Context
                rel_label = "LINKED_TO"
                if node_type == "Branch": rel_label = "MANAGED_BY"
                elif node_type == "Activity": rel_label = "PERFORMS"
                elif node_type == "Product": rel_label = "HAS_PRODUCT"
                elif node_type == "Customer": rel_label = "OWNED_BY"
                elif node_type == "Time": rel_label = "OCCURRED_ON"

                edge_unique_key = f"{case_id}_{node_id}_{rel_label}"
                
                if node_type == "Activity":
                    time_val = str(row.get(time_col, ''))[:10]
                    # Activities allow duplicate edges for timestamps
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

            # E. 3 SEPARATE CONCEPTS (NEXT_STEP, CAUSE, EFFECT)
            if current_activity_id:
                if case_id in case_activity_tracker:
                    previous_activity_id = case_activity_tracker[case_id]
                    previous_activity_label = case_activity_labels[case_id]
                    
                    if previous_activity_id != current_activity_id:
                        
                        # 1. NEXT_STEP (Process Flow - Amber)
                        seq_key = f"{previous_activity_id}_{current_activity_id}_NEXT_STEP"
                        if seq_key not in created_edges:
                            all_relationships.append({
                                "from": previous_activity_id, "to": current_activity_id,
                                "label": "NEXT_STEP", "properties": {"doc": filename, "riskCategory": "Process"}
                            })
                            created_edges.add(seq_key)

                        # 2. CAUSE (Root Trigger - Red) - Separate Edge
                        cause_key = f"{previous_activity_id}_{current_activity_id}_CAUSE"
                        if cause_key not in created_edges:
                            all_relationships.append({
                                "from": previous_activity_id, "to": current_activity_id,
                                "label": "CAUSE", "properties": {"doc": filename, "riskCategory": "Cause"}
                            })
                            created_edges.add(cause_key)

                        # 3. EFFECT (Consequence - Green) - Separate Edge
                        effect_key = f"{previous_activity_id}_{current_activity_id}_EFFECT"
                        if effect_key not in created_edges:
                            all_relationships.append({
                                "from": previous_activity_id, "to": current_activity_id,
                                "label": "EFFECT", "properties": {"doc": filename, "riskCategory": "Effect"}
                            })
                            created_edges.add(effect_key)

                        # --- FIX: COMMENTED OUT PROPERTY SETTING ---
                        # This ensures Cause/Effect appear as LINES (Edges) only, not as text properties.
                        
                        # if "cause" not in all_entities_map[current_activity_id]["properties"]:
                        #    all_entities_map[current_activity_id]["properties"]["cause"] = previous_activity_label
                        
                        # if "effect" not in all_entities_map[previous_activity_id]["properties"]:
                        #    all_entities_map[previous_activity_id]["properties"]["effect"] = current_activity_label

                case_activity_tracker[case_id] = current_activity_id
                case_activity_labels[case_id] = current_activity_label

        # Convert map back to list for processing
        all_entities_list = list(all_entities_map.values())

        await self.add_entities(all_entities_list)
        await self.add_relationships(all_relationships)
        return {"filename": filename, "entities": len(all_entities_list)}

    async def _process_unstructured_text(self, text, filename, domain):
        # Placeholder for AI logic
        return {"status": "skipped", "msg": "AI Mode not active"}

    async def add_entities(self, entities):
        seen = set()
        for e in entities:
            # Upsert entity with properties
            await self.repo.create_entity(e["id"], e["label"], e.get("properties", {}))

    async def add_relationships(self, relationships):
        for i, r in enumerate(relationships):
            # --- CRITICAL: THIS SAVES THE CATEGORY TO DB ---
            props = r.get("properties", {})
            risk = self._determine_risk_category(r["label"])
            if risk:
                props['riskCategory'] = risk # <--- Saved to Cosmos DB
            
            await self.repo.create_relationship(r["from"], r["to"], r["label"], props)
            
            # Throttle to prevent 429 errors during massive uploads
            if i % 10 == 0:
                await asyncio.sleep(0.05)

    async def get_graph(self): return await self.repo.get_graph()
    async def clear_graph(self, scope="all"): return await self.repo.clear_graph(scope)
    async def get_stats(self): return await self.repo.get_stats()
    async def search_nodes(self, q): return await self.repo.search_nodes(q)
    async def get_entities(self, label: Optional[str] = None): return await self.repo.get_entities(label=label)
    async def get_relationships_for_entity(self, entity_id: str): return await self.repo.get_relationships_for_entity(entity_id)
    async def delete_document_data(self, doc_id: str): return await self.repo.delete_document_data(doc_id)

graph_service = GraphService()