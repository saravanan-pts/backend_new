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
# from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

# --- OPERATIONAL CATEGORIES (DB SOURCE OF TRUTH) ---
CAUSE_LABELS = ['CAUSE', 'LED_TO', 'CAUSES', 'CAUSED', 'TRIGGERED', 'SOURCE_OF', 'PRECEDED_BY']
EFFECT_LABELS = ['EFFECT', 'RESULTED_IN', 'IMPACTED', 'AFFECTED', 'CONSEQUENCE_OF', 'HAS_EFFECT']
SEQUENCE_LABELS = ['NEXT_STEP', 'FOLLOWED_BY', 'PRECEDES', 'THEN']

class GraphService:
    """
    FINAL GRAPH ENGINE (Active Ingestion & Process Mining)
    - Auto-tags 'riskCategory' (Cause, Effect, or Process) during data load.
    - Implements Semantic Override Logic (Option B) for workflow exception paths.
    - FIX: Robust Partition Key & Context management for Manual CRUD operations.
    - ADDED: Backend Fetch for Lazy Loading Neighbors.
    - FIX: Parallel sequence bands via unique edge ID injection.
    """

    def __init__(self):
        self.repo = graph_repository
        self.PARTITION_KEY = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")

    # ==========================================
    # 1. HELPER METHODS
    # ==========================================

    def _run_query(self, query: str) -> Any:
        """Helper to safely execute Gremlin queries (Returns SINGLE result)."""
        try:
            client = getattr(self.repo, 'client', None)
            if not client: return None
            
            # Handle different gremlinpython client versions securely
            submit = getattr(client, 'submitAsync', getattr(client, 'submit_async', getattr(client, 'submit', None)))
            if not submit: return None
            
            future = submit(query)
            
            # Resolve the Threading Future (this fixes the 'await' crash)
            result_set = future.result() if hasattr(future, 'result') else future
            
            # Extract the actual data from the ResultSet
            if hasattr(result_set, 'all'):
                results = result_set.all().result()
            else:
                results = result_set
                
            if results and isinstance(results, list):
                return results[0]
            return results
        except Exception as e:
            logger.warning(f"Auto-Discovery Query Failed: {e}")
            return None

    def _run_query_list(self, query: str) -> List[Any]:
        """
        [NEW] Helper to safely execute Gremlin queries (Returns LIST of results).
        Required for get_neighbors and bulk fetches.
        """
        try:
            client = getattr(self.repo, 'client', None)
            if not client: return []
            
            submit = getattr(client, 'submitAsync', getattr(client, 'submit_async', getattr(client, 'submit', None)))
            if not submit: return []
            
            future = submit(query)
            result_set = future.result() if hasattr(future, 'result') else future
            
            if hasattr(result_set, 'all'):
                results = result_set.all().result()
            else:
                results = result_set
                
            return results if isinstance(results, list) else []
        except Exception as e:
            logger.warning(f"List Query Failed: {e}")
            return []

    def _is_uuid(self, val: Any) -> bool:
        """Properly checks if a string is a random UUID without using length limits."""
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False

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

    def _derive_domain(self, filename: str) -> str:
        """
        Helper: Extracts the Partition Key (Domain) from the filename.
        Logic: 'car-insurance_log.csv' -> 'car-insurance'
        Used to ensure manual nodes get the correct PK.
        """
        if not filename: return "general"
        base = filename.rsplit('.', 1)[0] # Remove extension
        return base.split('_')[0] if "_" in base else base

    # ==========================================
    # 2. READ OPERATIONS (NEW & EXISTING)
    # ==========================================

    async def get_neighbors(self, node_id: str) -> Dict[str, Any]:
        """
        [NEW] Fetches the specific node, its connecting edges, and direct neighbors.
        Used for 'Lazy Loading' (Backend Fetch) in the UI when expanding a node.
        """
        print(f"--- [FETCH NEIGHBORS] Node ID: {node_id} ---", flush=True)
        try:
            # 1. Fetch Nodes (Central Node + Neighbors)
            # Uses 'elementMap()' to get full properties of the nodes
            nodes_query = f"g.V('{node_id}').union(identity(), both()).dedup().elementMap()"
            nodes_data = self._run_query_list(nodes_query)

            # 2. Fetch Edges (Connecting the central node)
            edges_query = f"g.V('{node_id}').bothE().elementMap()"
            edges_data = self._run_query_list(edges_query)

            return {
                "nodes": nodes_data,
                "edges": edges_data
            }
        except Exception as e:
            logger.error(f"Error fetching neighbors for {node_id}: {str(e)}")
            return {"nodes": [], "edges": []}

    async def get_graph(self): return await self.repo.get_graph()
    async def clear_graph(self, scope="all"): return await self.repo.clear_graph(scope)
    async def get_stats(self): return await self.repo.get_stats()
    async def search_nodes(self, q): return await self.repo.search_nodes(q)
    async def get_entities(self, label: Optional[str] = None): return await self.repo.get_entities(label=label)
    async def get_relationships_for_entity(self, entity_id: str): return await self.repo.get_relationships_for_entity(entity_id)
    async def delete_document_data(self, doc_id: str): return await self.repo.delete_document_data(doc_id)

    # ==========================================
    # 3. CRUD OPERATIONS (FIXED FOR PK & UI)
    # ==========================================

    async def add_relationship(self, from_id: str, to_id: str, rel_type: str, properties: Dict[str, Any] = None):
        """Creates a single edge with auto-risk tagging and Context Inheritance."""
        if properties is None: properties = {}
        
        risk_cat = self._determine_risk_category(rel_type)
        if risk_cat:
            properties['riskCategory'] = risk_cat

        # FIX: Ensure manual edges appear in the UI's Document View!
        if "doc" not in properties and "documentId" not in properties:
            query = f"g.V('{from_id}').project('doc', 'pk').by(coalesce(values('documentId'), constant(''))).by(coalesce(values('{self.PARTITION_KEY}'), constant('')))"
            node_data = self._run_query(query)
            
            if node_data and isinstance(node_data, dict):
                if node_data.get('doc'):
                    properties['documentId'] = str(node_data['doc'])
                    properties['doc'] = str(node_data['doc']) 
                if node_data.get('pk'):
                    properties[self.PARTITION_KEY] = str(node_data['pk'])
                    properties['domain'] = str(node_data['pk'])
            
        print(f"--- [EXECUTING ADD EDGE] Source: {from_id} | Target: {to_id} | Final Props: {properties} ---", flush=True)
        return await self.repo.create_relationship(from_id, to_id, rel_type, properties)

    async def update_relationship(self, rel_id: str, properties: Dict[str, Any]):
        return await self.repo.update_relationship(rel_id, properties)

    async def upgrade_relationship(self, rel_id: str, new_type: str, new_props: Dict[str, Any] = None):
        """
        Safely changes an Edge's Type (Label) dynamically.
        """
        if new_props is None: new_props = {}
        query = f"g.E('{rel_id}').project('sid', 'tid', 'props').by(outV().id()).by(inV().id()).by(valueMap())"
        edge_data = self._run_query(query)

        if not edge_data or not isinstance(edge_data, dict): 
            return {"error": "Relationship not found"}

        from_id, to_id = edge_data.get('sid'), edge_data.get('tid')
        risk_cat = self._determine_risk_category(new_type)
        if risk_cat: new_props['riskCategory'] = risk_cat
        
        new_rel = await self.repo.create_relationship(from_id, to_id, new_type, new_props)
        await self.repo.delete_relationship(rel_id)
        return {"status": "success", "msg": f"Upgraded edge to {new_type}"}

    async def delete_relationship(self, rel_id: str):
        return await self.repo.delete_relationship(rel_id)

    async def update_entity(self, entity_id: str, properties: Dict[str, Any], partition_key: str = None):
        """
        Updates node properties cleanly without corrupting Types or losing PKs.
        """
        print(f"--- [UPDATE REQUEST RECEIVED] Node ID: {entity_id} | Provided PK/Doc: {partition_key} ---", flush=True)
        
        true_pk = partition_key
        doc_id = properties.get("documentId")
        
        # 1. Strip bad UUID Partition Keys sent by the frontend
        if self.PARTITION_KEY in properties:
            val = str(properties[self.PARTITION_KEY])
            if self._is_uuid(val) or val == entity_id:
                del properties[self.PARTITION_KEY]

        # 2. STRIP THE API ROUTER FALLBACK
        if true_pk == entity_id:
            true_pk = None

        # 3. Extract domain if the frontend sent the full filename (.csv)
        if true_pk and str(true_pk).endswith(".csv"):
            true_pk = self._derive_domain(str(true_pk))

        # Clear it if it's a random UUID
        if true_pk and self._is_uuid(true_pk):
            true_pk = None

        # 4. AUTO-DISCOVER the true PK if we don't have it using the sync helper
        if not true_pk:
            val = self._run_query(f"g.V('{entity_id}').values('{self.PARTITION_KEY}')")
            if val:
                true_pk = str(val)
                print(f"--- [AUTO-DISCOVERY] Found PK '{true_pk}' for updating node '{entity_id}' ---", flush=True)

        # Re-inject verified context
        if true_pk: properties[self.PARTITION_KEY] = true_pk
        if doc_id: properties["documentId"] = doc_id

        # 5. FIX: Stop overwriting Concept 'Type' with the display 'Name'
        if "type" in properties:
            clean_type = str(properties["type"]).strip().title()
            properties["normType"] = clean_type
            properties["type"] = clean_type
            properties["entityType"] = clean_type

        print(f"--- [EXECUTING UPDATE] Node ID: {entity_id} | Final PK: {true_pk} ---", flush=True)
        return await self.repo.update_entity(entity_id, properties)

    async def delete_entity(self, entity_id: str, partition_key: str = None):
        """
        Deletes a node securely using precise Partition Key targeting.
        """
        print(f"--- [DELETE REQUEST RECEIVED] Node ID: {entity_id} | Provided PK/Doc: {partition_key} ---", flush=True)
        
        true_pk = partition_key

        # 1. STRIP THE API ROUTER FALLBACK
        if true_pk == entity_id:
            true_pk = None

        # 2. Strip UUIDs sent by mistake
        if true_pk and self._is_uuid(true_pk):
            true_pk = None

        # 3. Extract domain if a filename (.csv) was passed
        if true_pk and str(true_pk).endswith(".csv"):
            true_pk = self._derive_domain(str(true_pk))

        # 4. Auto-Discover the specific node's PK using the sync helper
        if not true_pk:
            val = self._run_query(f"g.V('{entity_id}').values('{self.PARTITION_KEY}')")
            if val:
                true_pk = str(val)
                print(f"--- [AUTO-DISCOVERY] Found PK '{true_pk}' for deleting node '{entity_id}' ---", flush=True)

        print(f"--- [EXECUTING DELETE] Node ID: {entity_id} | Final PK: {true_pk} ---", flush=True)
        return await self.repo.delete_entity(entity_id, true_pk)

    async def add_entities(self, entities):
        """
        Creates nodes. Handles both Bulk Load (CSV) and Manual Creation (UI).
        """
        for e in entities:
            raw_label = e.get("label", "Concept")
            props = e.get("properties", {})

            # --- 1. CONTEXT INHERITANCE ---
            doc_id = props.get("documentId") or e.get("documentId")
            
            target_pk = "general" # Default fallback
            
            if doc_id and str(doc_id).endswith(".csv"):
                target_pk = self._derive_domain(doc_id)
                props["documentId"] = doc_id
                props["domain"] = target_pk
            elif "domain" in props:
                target_pk = props["domain"]
            elif self.PARTITION_KEY in props:
                val = str(props[self.PARTITION_KEY])
                if not self._is_uuid(val): target_pk = val

            # FORCE the Partition Key
            props[self.PARTITION_KEY] = target_pk

            # --- 2. CLEAN ID & TYPE ---
            if "normType" in props:
                node_type = props["normType"]
            else:
                node_type = str(raw_label).strip().title()
                props["normType"] = node_type
                props["type"] = node_type
                props["entityType"] = node_type

            # Name Display
            node_name = props.get("name", raw_label)
            props["name"] = node_name
            
            # Generate Deterministic ID (e.g. 'Person_Janani')
            clean_id = self._clean_id(node_type, node_name)
            
            # --- 3. SAVE ---
            await self.repo.create_entity(clean_id, raw_label, props)

    async def add_relationships(self, relationships):
        for i, r in enumerate(relationships):
            # --- CRITICAL: THIS SAVES THE CATEGORY TO DB ---
            props = r.get("properties", {})
            risk = self._determine_risk_category(r["label"])
            if risk:
                props['riskCategory'] = risk # <--- Saved to Cosmos DB
            
            # --- FIX: INJECT UNIQUE EDGE ID FOR PARALLEL BANDS ---
            edge_id = r.get("id")
            if edge_id:
                props["edge_id"] = edge_id
            
            await self.repo.create_relationship(r["from"], r["to"], r["label"], props)
            
            # Throttle to prevent 429 errors during massive uploads
            if i % 10 == 0:
                await asyncio.sleep(0.05)

    # ==========================================
    # 3.5 AI RISK INGESTION AGENT
    # ==========================================
    async def _ai_ingestion_analysis(self, activity_label: str) -> Dict[str, str]:
        text = str(activity_label).lower()
        insights = {"riskLevel": "Low", "isCause": "False", "isEffect": "False", "aiSummary": "Standard operational step."}

        if any(word in text for word in ['fail', 'error', 'timeout', 'reject', 'denied', 'fraud', 'divergent', 'anomaly', 'breach']):
            insights["riskLevel"] = "High"
            insights["isCause"] = "True"
            insights["riskCategory"] = "Cause"
            insights["aiSummary"] = f"AI Risk Flag: '{activity_label}' indicates a critical failure or anomaly."
        
        elif any(word in text for word in ['closed', 'block', 'suspend', 'locked', 'rejeitado', 'terminated', 'cleared']):
            insights["riskLevel"] = "Medium"
            insights["isEffect"] = "True"
            insights["riskCategory"] = "Effect"
            insights["aiSummary"] = f"AI Risk Flag: '{activity_label}' is a punitive/terminal state."

        return insights

    # ==========================================
    # 4. CSV / GRAPH PROCESSING (YOUR ORIGINAL LOGIC)
    # ==========================================

    async def process_narrative(self, narrative_text: str, filename: str) -> Dict[str, Any]:
        logger.info(f"Processing: {filename}")
        
        # Use helper to get domain (matches the logic in _derive_domain)
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
                
                # --- FIX: Removed the line that skipped 'Amount' ---
                # if node_type == "Amount": continue 

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
                # --- FIX: Added logic to link Amount/Balance ---
                elif node_type == "Amount": rel_label = "HAS_BALANCE"

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

            # E. SEQUENCE LOGIC: OPTION B (SEMANTIC OVERRIDE)
            if current_activity_id:
                if case_id in case_activity_tracker:
                    previous_activity_id = case_activity_tracker[case_id]
                    previous_activity_label = case_activity_labels[case_id]
                    
                    if previous_activity_id != current_activity_id:
                        
                        # AI Intelligence check for the current activity transition
                        ai_insights = await self._ai_ingestion_analysis(current_activity_label)
                        
                        # Determine the Semantic Label (Override defaults if anomaly detected)
                        seq_label = "NEXT_STEP"
                        risk_cat = "Process"
                        
                        if ai_insights["isCause"] == "True":
                            seq_label = "CAUSES"
                            risk_cat = "Cause"
                        elif ai_insights["isEffect"] == "True":
                            seq_label = "RESULTED_IN"
                            risk_cat = "Effect"

                        # Draw a SINGLE sequence edge (No parallel lines for the exact same step) 
                        # using dedupe=False logic (appending _idx) so we get thick visual bands!
                        seq_key = f"{previous_activity_id}_{current_activity_id}_{seq_label}_{idx}"
                        if seq_key not in created_edges:
                            all_relationships.append({
                                "id": seq_key, # <--- UNIQUE ID INJECTED FOR PARALLEL BANDS
                                "from": previous_activity_id, "to": current_activity_id,
                                "label": seq_label, 
                                "properties": {"doc": filename, "riskCategory": risk_cat, "case_ref": case_val}
                            })
                            created_edges.add(seq_key)

                        # 4. NODE PROPERTIES (Data for DB only, Filtered in UI)
                        if "cause" not in all_entities_map[current_activity_id]["properties"]:
                            all_entities_map[current_activity_id]["properties"]["cause"] = previous_activity_label
                        
                        if "effect" not in all_entities_map[previous_activity_id]["properties"]:
                            all_entities_map[previous_activity_id]["properties"]["effect"] = current_activity_label

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

graph_service = GraphService()