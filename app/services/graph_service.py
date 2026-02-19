import logging
import uuid
import re
import asyncio
import pandas as pd
import json # Added for RCA JSON parsing
from typing import List, Dict, Any, Optional
from io import StringIO
from openai import AsyncAzureOpenAI # Added for RCA Agent

from app.config import settings
from app.repositories.graph_repository import graph_repository
# Note: document_processor import removed from top to avoid circular dependency
# from app.services.openai_extractor import extract_entities_and_relationships

logger = logging.getLogger(__name__)

# --- OPERATIONAL CATEGORIES (DB SOURCE OF TRUTH) ---
# ADDED 'NEXT' and 'RESULTS_IN' to match the new shorter edge labels
CAUSE_LABELS = ['CAUSE', 'LED_TO', 'CAUSES', 'CAUSED', 'TRIGGERED', 'SOURCE_OF', 'PRECEDED_BY']
EFFECT_LABELS = ['EFFECT', 'RESULTED_IN', 'RESULTS_IN', 'IMPACTED', 'AFFECTED', 'CONSEQUENCE_OF', 'HAS_EFFECT']
SEQUENCE_LABELS = ['NEXT', 'NEXT_STEP', 'FOLLOWED_BY', 'PRECEDES', 'THEN']

class GraphService:
    """
    FINAL GRAPH ENGINE (Active Ingestion & Process Mining)
    - Auto-tags 'riskCategory' (Cause, Effect, or Process) during data load.
    - Implements Semantic Override Logic (Option B) for workflow exception paths.
    - FIX: Robust Partition Key & Context management for Manual CRUD operations.
    - FIX: Async Gremlin queries to prevent WebSocket drops (Event loop unblocked).
    - FIX: "Star Model" Hierarchical Edge Generation for instant 1-Hop case context.
    - ADDED: Enterprise RCA Agent for automated Root Cause/Effect persistence.
    """

    def __init__(self):
        self.repo = graph_repository
        self.PARTITION_KEY = getattr(settings, "COSMOS_GREMLIN_PARTITION_KEY", "pk")

    # ==========================================
    # 1. HELPER METHODS
    # ==========================================

    async def _run_query(self, query: str) -> Any:
        """Helper to safely execute Gremlin queries (Returns SINGLE result)."""
        try:
            client = getattr(self.repo, 'client', None)
            if not client: return None
            
            # Handle different gremlinpython client versions securely
            submit = getattr(client, 'submitAsync', getattr(client, 'submit_async', getattr(client, 'submit', None)))
            if not submit: return None
            
            future = submit(query)
            
            # PROPER ASYNC AWAIT: Prevents blocking the event loop and dropping the WebSocket
            result_set = await asyncio.wrap_future(future) if hasattr(future, 'add_done_callback') else future
            
            # Extract the actual data from the ResultSet
            if hasattr(result_set, 'all'):
                results_future = result_set.all()
                results = await asyncio.wrap_future(results_future) if hasattr(results_future, 'add_done_callback') else results_future.result()
            else:
                results = result_set
                
            if results and isinstance(results, list):
                return results[0]
            return results
        except Exception as e:
            logger.warning(f"Auto-Discovery Query Failed: {e}")
            return None

    async def _run_query_list(self, query: str) -> List[Any]:
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
            # PROPER ASYNC AWAIT
            result_set = await asyncio.wrap_future(future) if hasattr(future, 'add_done_callback') else future
            
            if hasattr(result_set, 'all'):
                results_future = result_set.all()
                results = await asyncio.wrap_future(results_future) if hasattr(results_future, 'add_done_callback') else results_future.result()
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
        """
        [UPDATED] Semantic type detection based on Enterprise Data Schema.
        Maps raw CSV column headers to definitive Knowledge Graph Node Types.
        """
        h = header.lower()
        v = str(value).lower()
        
        # 1. Core / Identifiers
        if "customer" in h: return "Customer"
        if "vendor" in h: return "Vendor"
        if "branch" in h: return "Branch"
        if "activity" in h or "action" in h: return "Activity"
        if "time" in h or "date" in h: return "Time"
        
        # 2. Geography (Moved up to prevent 'policy_state' -> 'Policy')
        if "state" in h: return "State"
        if "region" in h: return "Region"
        
        # 3. Account & Financials
        if "account" in h:
            if "type" in h: return "AccountType"
            return "Account"
        if "product" in h: return "Product"
        if "balance" in h or "amount" in h or "inr" in h or "$" in h: 
            if "claim" in h: return "ClaimAmount"
            if "loan" in h: return "LoanAmount"
            if "premium" in h: return "PremiumAmount"
            return "Amount"
        if "loan_type" in h: return "LoanType"
        if "deductible" in h: return "Deductible"
        if "premium" in h: return "PremiumAmount"
        if "customer_lifetime_value" in h or "clv" in h: return "CustomerLifetimeValue"
        
        # 4. Demographics & Profiling
        if "job" in h: return "Job"
        if "marital" in h: return "MaritalStatus"
        if "age" in h or "sex" in h or "gender" in h: return "Demographics"
        if "driverrating" in h or "experience" in h: return "DriverProfile"
        
        # 5. Operations & Claims
        if "agent" in h or "repnumber" in h: return "Agent"
        if "outcome" in h: return "Outcome"
        if "channel" in h: return "Channel"
        if "nps" in h: return "NPS"
        if "claim_type" in h: return "ClaimType"
        if "file_name" in h or "document" in h: return "Document"
        if "pages" in h: return "PageCount"
        if "status" in h: return "Status"
        if "policy" in h: 
            if "type" in h: return "PolicyType"
            return "Policy"
            
        # 6. Risk, Fraud & Incidents
        if "fraud" in h: return "FraudFlag"
        if "risk" in h: return "RiskLevel"
        if "accident" in h or "incident" in h:
            if "type" in h: return "IncidentType"
            if "severity" in h: return "IncidentSeverity"
            if "previous" in h: return "IncidentHistory"
            return "Incident"
        if "fault" in h: return "Fault"
        if "authorities" in h or "police" in h: return "Authority"
        if "witness" in h: return "Witness"
        
        # 7. Vehicles & Telematics
        if "vehicle" in h or "auto" in h or "car" in h:
            if "class" in h: return "VehicleClass"
            if "make" in h: return "VehicleMake"
            if "model" in h: return "VehicleModel"
            if "year" in h or "age" in h: return "VehicleAge"
            if "size" in h: return "VehicleSize"
            return "Vehicle"
        if "device" in h: return "Device"
        if "sensor" in h: return "SensorValue"
        if "alarm" in h: return "AlarmClass"
        
        # 8. Regex fallbacks for coded values
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
    # 2. READ OPERATIONS (FIXED FOR COSMOS DB COMPATIBILITY)
    # ==========================================

    async def get_neighbors(self, node_id: str) -> Dict[str, Any]:
        """
        Fetches the specific node, its connecting edges, and direct neighbors.
        REWRITTEN: Avoids elementMap() because Cosmos DB doesn't support it.
        Uses project() and valueMap(true) for full compatibility.
        """
        print(f"--- [FETCH NEIGHBORS] Node ID: {node_id} ---", flush=True)
        try:
            # 1. Fetch Nodes (Central Node + Neighbors) using valueMap(true) for Cosmos support
            nodes_query = f"g.V('{node_id}').union(identity(), both()).dedup().valueMap(true)"
            nodes_data = await self._run_query_list(nodes_query)

            # 2. Fetch Edges using project() for Cosmos DB support
            edges_query = (
                f"g.V('{node_id}').bothE().dedup()"
                f".project('id', 'label', 'inV', 'outV', 'properties')"
                f".by(id).by(label).by(inV().id()).by(outV().id()).by(valueMap())"
            )
            edges_data = await self._run_query_list(edges_query)

            # 3. Format the data to match what the frontend expects
            formatted_nodes = []
            for n in nodes_data:
                # Tinkerpop/Cosmos returns T.id and T.label as enums, we must stringify them
                n_id = str(n.get('id', n.get('T.id', '')))
                n_label = str(n.get('label', n.get('T.label', '')))
                
                # Clean up properties (Cosmos returns properties as lists, e.g., {'name': ['John']})
                props = {}
                for k, v in n.items():
                    if k not in ['id', 'label', 'T.id', 'T.label']:
                        props[k] = v[0] if isinstance(v, list) else v
                
                formatted_nodes.append({
                    "id": n_id,
                    "label": n_label,
                    "properties": props
                })

            formatted_edges = []
            for e in edges_data:
                formatted_edges.append({
                    "id": str(e.get('id')),
                    "label": str(e.get('label')),
                    "from": str(e.get('outV')),
                    "to": str(e.get('inV')),
                    "properties": e.get('properties', {})
                })

            return {
                "nodes": formatted_nodes,
                "edges": formatted_edges
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
            node_data = await self._run_query(query)
            
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
        edge_data = await self._run_query(query)

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

    async def update_entity(self, entity_id: str, payload: Dict[str, Any], partition_key: str = None):
        """
        Updates node properties cleanly without corrupting Types or losing PKs.
        """
        print(f"--- [UPDATE REQUEST RECEIVED] Node ID: {entity_id} | Provided PK/Doc: {partition_key} ---", flush=True)
        
        true_pk = partition_key
        
        # --- THE FIX: FLATTEN THE PAYLOAD ---
        # The UI sends data wrapped like: { label: "...", type: "...", properties: {...} }
        # We must extract and merge these so the graph repository updates everything properly.
        inner_props = payload.get("properties", {})
        doc_id = inner_props.get("documentId") or payload.get("documentId")

        # 1. Strip bad UUID Partition Keys sent by the frontend
        for target_dict in [payload, inner_props]:
            if self.PARTITION_KEY in target_dict:
                val = str(target_dict[self.PARTITION_KEY])
                if self._is_uuid(val) or val == entity_id:
                    del target_dict[self.PARTITION_KEY]

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
            val = await self._run_query(f"g.V('{entity_id}').values('{self.PARTITION_KEY}')")
            if val:
                true_pk = str(val)
                print(f"--- [AUTO-DISCOVERY] Found PK '{true_pk}' for updating node '{entity_id}' ---", flush=True)

        # 5. ASSEMBLE FINAL PROPERTIES FOR THE DATABASE
        final_props = {**inner_props}

        # Re-inject verified context
        if true_pk: final_props[self.PARTITION_KEY] = true_pk
        if doc_id: final_props["documentId"] = doc_id

        # Safely enforce the new Type and Label globally
        if "label" in payload:
            final_props["name"] = payload["label"]
            final_props["label"] = payload["label"]

        node_type = payload.get("type") or inner_props.get("type")
        if node_type:
            clean_type = str(node_type).strip().title()
            final_props["normType"] = clean_type
            final_props["type"] = clean_type
            final_props["entityType"] = clean_type

        print(f"--- [EXECUTING UPDATE] Node ID: {entity_id} | Final PK: {true_pk} | Properties: {list(final_props.keys())} ---", flush=True)
        return await self.repo.update_entity(entity_id, final_props)

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
            val = await self._run_query(f"g.V('{entity_id}').values('{self.PARTITION_KEY}')")
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

        # EXPANDED TRIPWIRES for Claims, Complaints, Telematics, SIU, and Call Centers
        high_risk_keywords = [
            'fail', 'error', 'timeout', 'reject', 'denied', 'fraud', 'divergent', 
            'anomaly', 'breach', 'claim', 'loss', 'complaint', 'damage', 'theft', 
            'collision', 'declined', 'no_result', 'failure'
        ]

        if any(word in text for word in high_risk_keywords):
            insights["riskLevel"] = "High"
            insights["isCause"] = "True"
            insights["riskCategory"] = "Cause"
            insights["aiSummary"] = f"AI Risk Flag: '{activity_label}' indicates a critical failure, claim, or anomaly."
        
        elif any(word in text for word in ['closed', 'block', 'suspend', 'locked', 'rejeitado', 'terminated', 'cleared', 'resolution']):
            insights["riskLevel"] = "Medium"
            insights["isEffect"] = "True"
            insights["riskCategory"] = "Effect"
            insights["aiSummary"] = f"AI Risk Flag: '{activity_label}' is a punitive, resolution, or terminal state."

        return insights

    # ==========================================
    # 3.6 ENTERPRISE RCA AGENT (BACKGROUND TASK)
    # ==========================================
    async def _run_post_ingestion_rca(self, case_id: str, domain: str, filename: str):
        """
        Runs in the background after ingestion. 
        Analyzes the timeline, extracts 1 Cause and 1 Effect, and saves them to the DB.
        """
        logger.info(f"Starting Background RCA for Case: {case_id}")
        
        try:
            # 1. Fetch the newly ingested timeline
            neighbors = await self.get_neighbors(case_id)
            timeline_events = []
            connected_nodes_map = {n['id']: n for n in neighbors.get('nodes', [])}
            
            for edge in neighbors.get('edges', []):
                target_id = edge['to'] if edge['from'] == case_id else edge['from']
                target_node = connected_nodes_map.get(target_id, {})
                target_name = target_node.get('properties', {}).get('name', target_id)
                target_type = target_node.get('label', 'Unknown')
                rel_label = edge.get('label', 'LINKED_TO')
                timestamp = edge.get('properties', {}).get('timestamp', 'Unknown')
                
                if timestamp != 'Unknown':
                    timeline_events.append({"date": timestamp, "desc": f"[{timestamp}] {rel_label} -> {target_name} ({target_type})"})
            
            if not timeline_events:
                return

            timeline_events.sort(key=lambda x: x["date"])
            timeline_text = "\n".join([e["desc"] for e in timeline_events])

            # 2. Call OpenAI for Root Cause Analysis
            ai_client = AsyncAzureOpenAI(
                api_key=settings.AZURE_OPENAI_API_KEY,
                api_version=settings.AZURE_OPENAI_API_VERSION,
                azure_endpoint=settings.AZURE_OPENAI_ENDPOINT
            )

            prompt = f"""
            You are an automated Root Cause Analysis Agent for the {domain.upper()} sector.
            Analyze this case timeline:
            {timeline_text}

            Identify any process anomalies (e.g., Activation immediately followed by Closure).
            Extract exactly ONE Root Cause and ONE Business Effect. 
            Generate a short, downloadable client report explaining WHY this happened in the context of the {domain.upper()} sector.
            
            Return ONLY valid JSON in this exact format:
            {{
                "root_cause_name": "Short 3-word cause",
                "effect_name": "Short 3-word effect",
                "client_report": "A 2-sentence explanation of why this happened and the financial/risk impact."
            }}
            """

            response = await ai_client.chat.completions.create(
                model=settings.AZURE_OPENAI_DEPLOYMENT_NAME,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                response_format={"type": "json_object"}
            )
            
            # Use safe parse
            content = response.choices[0].message.content
            rca_data = json.loads(content)
            
            # 3. Create explicit Nodes in the Database for Cause and Effect
            cause_node_id = self._clean_id("RootCause", f"{case_id}_{rca_data['root_cause_name']}")
            effect_node_id = self._clean_id("BusinessEffect", f"{case_id}_{rca_data['effect_name']}")

            # Save Root Cause Node
            await self.repo.create_entity(cause_node_id, "RootCause", {
                "name": rca_data['root_cause_name'],
                "normType": "RootCause",
                "documentId": filename,
                self.PARTITION_KEY: domain
            })
            await self.repo.create_relationship(case_id, cause_node_id, "HAS_ROOT_CAUSE", {"doc": filename})

            # Save Business Effect Node
            await self.repo.create_entity(effect_node_id, "BusinessEffect", {
                "name": rca_data['effect_name'],
                "normType": "BusinessEffect",
                "documentId": filename,
                self.PARTITION_KEY: domain
            })
            await self.repo.create_relationship(case_id, effect_node_id, "HAS_BUSINESS_EFFECT", {"doc": filename})

            # Save the full downloadable report directly onto the Case node properties
            await self.repo.update_entity(case_id, {"rca_report": rca_data['client_report']}, domain)
            
            logger.info(f"Successfully saved RCA for {case_id} to Database.")

        except Exception as e:
            logger.error(f"Background RCA Failed for {case_id}: {e}")

    # ==========================================
    # 4. CSV / GRAPH PROCESSING
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

        all_entities_map = {} 
        all_relationships = []
        created_edges = set()

        # 3. Sequence Tracker
        case_activity_tracker = {}
        case_activity_labels = {} 

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
            row_context_nodes = [] 

            # D. PROCESS COLUMNS (Nodes Only First)
            for col in df.columns:
                val = str(row[col]).strip()
                if not val or val.lower() == "nan": continue
                if col == case_col: continue 
                if val in all_case_ids_banlist: continue 

                node_type = self._detect_type(col, val)
                
                # --- NEW UX FIX: We no longer create generic 'Time' nodes ---
                if node_type == "Time": continue
                
                node_id = self._clean_id(node_type, val)

                if node_type == "Activity":
                    current_activity_id = node_id
                    current_activity_label = val

                row_context_nodes.append({"id": node_id, "type": node_type, "val": val})

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

            # E. CREATE HIERARCHICAL EDGES (The Star Model)
            for ctx in row_context_nodes:
                ctx_id = ctx["id"]
                ctx_type = ctx["type"]
                time_val = str(row.get(time_col, ''))[:19] if time_col else ''

                # 1. LINK CASE -> ACTIVITY (with timestamp)
                if ctx_type == "Activity":
                    edge_unique_key = f"{case_id}_{ctx_id}_PERFORMS_{time_val}"
                    if edge_unique_key not in created_edges:
                        all_relationships.append({
                            "id": edge_unique_key,
                            "from": case_id, 
                            "to": ctx_id, 
                            "label": "PERFORMS", 
                            "properties": {"timestamp": time_val, "doc": filename}
                        })
                        created_edges.add(edge_unique_key)

                # 2. LINK CASE -> CONTEXT (Semantic Edges)
                else:
                    rel_label = "LINKED_TO" # Fallback
                    
                    # Core & People
                    if ctx_type == "Customer": rel_label = "OWNED_BY"
                    elif ctx_type == "Agent": rel_label = "ASSIGNED_TO"
                    elif ctx_type in ["Demographics", "MaritalStatus", "Job", "DriverProfile"]: rel_label = "HAS_PROFILE"
                    
                    # Geography
                    elif ctx_type in ["State", "Region", "Location", "Branch"]: rel_label = "LOCATED_IN"
                    
                    # Vehicles & Assets
                    elif ctx_type in ["Vehicle", "VehicleMake", "VehicleModel", "VehicleAge", "VehicleClass", "VehicleSize"]: rel_label = "HAS_VEHICLE"
                    
                    # Financials
                    elif ctx_type in ["Amount", "ClaimAmount", "PremiumAmount", "LoanAmount", "Deductible", "FinancialValue"]: rel_label = "HAS_AMOUNT"
                    elif ctx_type in ["Product", "Policy", "PolicyType", "Account", "AccountType", "LoanType"]: rel_label = "HAS_POLICY"
                    
                    # Risk & Incidents
                    elif ctx_type in ["Incident", "IncidentType", "IncidentSeverity"]: rel_label = "INVOLVED_IN"
                    elif ctx_type in ["FraudFlag", "RiskLevel", "Fault"]: rel_label = "HAS_RISK_FLAG"
                    
                    # Meta
                    elif ctx_type in ["Status", "Outcome"]: rel_label = "HAS_STATUS"
                    elif ctx_type == "Channel": rel_label = "VIA_CHANNEL"
                    
                    # Injecting time_val into the key ensures overlapping events fan out
                    edge_unique_key = f"{case_id}_{ctx_id}_{rel_label}_{time_val}"
                    
                    if edge_unique_key not in created_edges:
                        all_relationships.append({
                            "id": edge_unique_key,
                            "from": case_id, 
                            "to": ctx_id, 
                            "label": rel_label, 
                            "properties": {"timestamp": time_val, "doc": filename}
                        })
                        created_edges.add(edge_unique_key)


            # F. SEQUENCE LOGIC: OPTION B (SEMANTIC OVERRIDE)
            if current_activity_id:
                if case_id in case_activity_tracker:
                    previous_activity_id = case_activity_tracker[case_id]
                    previous_activity_label = case_activity_labels[case_id]
                    
                    if previous_activity_id != current_activity_id:
                        
                        # AI Intelligence check for the current activity transition
                        ai_insights = await self._ai_ingestion_analysis(current_activity_label)
                        
                        # Determine the Semantic Label (Shortened!)
                        seq_label = "NEXT"
                        risk_cat = "Process"
                        
                        if ai_insights["isCause"] == "True":
                            seq_label = "CAUSES"
                            risk_cat = "Cause"
                        elif ai_insights["isEffect"] == "True":
                            seq_label = "RESULTS_IN" # Shortened
                            risk_cat = "Effect"

                        # Draw a SINGLE sequence edge (No parallel lines for the exact same step) 
                        # using dedupe=False logic (appending _idx) so we get thick visual bands!
                        seq_key = f"{previous_activity_id}_{current_activity_id}_{seq_label}_{idx}"
                        if seq_key not in created_edges:
                            time_val = str(row.get(time_col, ''))[:19] if time_col else ''
                            all_relationships.append({
                                "id": seq_key, 
                                "from": previous_activity_id, "to": current_activity_id,
                                "label": seq_label, 
                                "properties": {"doc": filename, "riskCategory": risk_cat, "case_ref": case_val, "timestamp": time_val}
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
        
        # --- NEW: TRIGGER BACKGROUND RCA AGENT ---
        # Identify anomalous cases based on generated relationships
        anomalous_cases = set()
        for r in all_relationships:
            if r["label"] in ["CAUSES", "RESULTS_IN"]:
                case_ref = r.get("properties", {}).get("case_ref")
                if case_ref:
                    anomalous_cases.add(self._clean_id("Case", case_ref))

        # Launch background analysis for identified cases
        for a_case in anomalous_cases:
            asyncio.create_task(self._run_post_ingestion_rca(a_case, domain, filename))
        # -----------------------------------------

        return {"filename": filename, "entities": len(all_entities_list)}

    async def _process_unstructured_text(self, text, filename, domain):
        # Placeholder for AI logic
        return {"status": "skipped", "msg": "AI Mode not active"}

graph_service = GraphService()