import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import logging
from openai import AsyncAzureOpenAI
from app.services.graph_service import graph_service

# Define Router
router = APIRouter(prefix="/api/graph", tags=["Analysis"])
logger = logging.getLogger(__name__)

# --- REQUEST MODEL ---
class AnalyzeRequest(BaseModel):
    nodeId: str 

# --- CONFIGURATION ---
AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

# --- RISK DEFINITIONS (The Operational Truth) ---
CAUSE_LABELS = ['LED_TO', 'CAUSES', 'CAUSED', 'TRIGGERED', 'SOURCE_OF', 'PRECEDED_BY']
EFFECT_LABELS = ['RESULTED_IN', 'EFFECT', 'IMPACTED', 'AFFECTED', 'CONSEQUENCE_OF', 'HAS_EFFECT']

ai_client = None
USE_REAL_AI = False

try:
    if AZURE_ENDPOINT and AZURE_API_KEY and AZURE_DEPLOYMENT:
        logger.info(f"Initializing Azure OpenAI Client (Deployment: {AZURE_DEPLOYMENT})...")
        ai_client = AsyncAzureOpenAI(
            api_key=AZURE_API_KEY,
            api_version=AZURE_API_VERSION,
            azure_endpoint=AZURE_ENDPOINT
        )
        USE_REAL_AI = True
    else:
        logger.warning("Missing Azure OpenAI environment variables. AI Analysis will fall back to Logic mode.")
except Exception as e:
    logger.error(f"AI Client Init Failed: {e}")

@router.post("/analyze")
async def analyze_node(body: AnalyzeRequest) -> Dict[str, Any]:
    node_id = body.nodeId
    logger.info(f"Analyzing node: {node_id}")

    try:
        client = graph_service.repo.client
        if not client:
            raise HTTPException(status_code=503, detail="Database not connected")

        # ---------------------------------------------------------
        # STEP 1: ENRICH GRAPH (Active Learning)
        # We update the DB immediately so the edges permanently define Cause/Effect
        # ---------------------------------------------------------
        await enrich_relationships(client, node_id)

        # ---------------------------------------------------------
        # STEP 2: FETCH NODE DETAILS
        # ---------------------------------------------------------
        target_query = f"g.V('{node_id}').project('props', 'label').by(valueMap()).by(label)"
        target_result = await execute_gremlin(client, target_query)
        
        if not target_result:
            logger.error(f"Node {node_id} not found in database.")
            return {"summary": f"Node '{node_id}' could not be analyzed directly."}
            
        target_node = target_result[0]
        node_label = target_node.get('label', 'Unknown')
        node_props = format_properties(target_node.get('props', {}))
        node_name = node_props.get('name') or node_id

        # ---------------------------------------------------------
        # STEP 3: RISK SCANNING
        # ---------------------------------------------------------
        risk_context = {}
        
        # A. Closures (High Priority)
        try:
            closure_query = f"g.V('{node_id}').both().hasLabel('Account Closed').count()" 
            risk_context['closure_count'] = await execute_gremlin_scalar(client, closure_query)
        except:
            risk_context['closure_count'] = 0
        
        # B. Complaints (High Priority)
        try:
            complaint_query = f"g.V('{node_id}').both().hasLabel('Complaint').count()"
            risk_context['complaint_count'] = await execute_gremlin_scalar(client, complaint_query)
        except:
            risk_context['complaint_count'] = 0

        # ---------------------------------------------------------
        # STEP 4: GATHER CONTEXT (Using new DB categories if available)
        # ---------------------------------------------------------
        # We project the 'riskCategory' property we just wrote to the DB
        
        # Upstream (Causes)
        cause_query = f"g.V('{node_id}').inE().project('rel', 'cat', 'source').by(label).by(coalesce(values('riskCategory'), constant(''))).by(outV().label()).limit(10)"
        causes_result = await execute_gremlin(client, cause_query)
        
        # Downstream (Effects)
        effect_query = f"g.V('{node_id}').outE().project('rel', 'cat', 'target').by(label).by(coalesce(values('riskCategory'), constant(''))).by(inV().label()).limit(10)"
        effects_result = await execute_gremlin(client, effect_query)

        # Format context for AI
        causes_text = []
        for c in causes_result:
            cat_str = f"[{c['cat']}] " if c['cat'] else ""
            causes_text.append(f"- {cat_str}{c['source']} ({c['rel']})")

        effects_text = []
        for e in effects_result:
            cat_str = f"[{e['cat']}] " if e['cat'] else ""
            effects_text.append(f"- ({e['rel']}) -> {cat_str}{e['target']}")

        # ---------------------------------------------------------
        # STEP 5: PREPARE AI PROMPT
        # ---------------------------------------------------------
        system_prompt = "You are a Senior Risk Analyst for a Car Insurance company. Analyze the provided graph entity to identify operational risks and root causes."
        
        user_prompt = f"""
        ENTITY REPORT:
        - Name: {node_name} (ID: {node_id})
        - Type: {node_label}
        - Properties: {node_props}

        RISK INDICATORS (Database Scan):
        - Related Closures/Rejections: {risk_context['closure_count']}
        - Related Complaints/Disputes: {risk_context['complaint_count']}

        CONTEXT (Operational Flow):
        - Incoming (Potential Causes): {chr(10).join(causes_text) if causes_text else "(None - Start Node)"}
        - Outgoing (Consequences): {chr(10).join(effects_text) if effects_text else "(None - End Node)"}

        INSTRUCTIONS:
        Write a concise operational analysis in 3 sections:
        1. **Identity**: What is this entity?
        2. **Risk Assessment**: 
           - Mark **HIGH PRIORITY** if closures/complaints > 0.
           - Mark **Stable** otherwise.
        3. **Next Steps**: Actionable recommendation (Monitor, Audit, Contact).
        """

        # ---------------------------------------------------------
        # STEP 6: GENERATE SUMMARY
        # ---------------------------------------------------------
        summary = ""
        if USE_REAL_AI and ai_client:
            try:
                logger.info("Sending prompt to Azure OpenAI...")
                response = await ai_client.chat.completions.create(
                    model=AZURE_DEPLOYMENT,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=300,
                    temperature=0.3
                )
                summary = response.choices[0].message.content
            except Exception as ai_e:
                logger.error(f"Azure AI Call Failed: {ai_e}. Reverting to logic.")
                summary = generate_logic_summary(node_label, risk_context, causes_text, effects_text)
        else:
            summary = generate_logic_summary(node_label, risk_context, causes_text, effects_text)

        return {"summary": summary}

    except Exception as e:
        logger.error(f"Analysis Failed: {e}")
        return {"summary": f"Analysis unavailable: {str(e)}"}

# --- HELPERS ---

async def enrich_relationships(client, node_id: str):
    """
    Updates the database edges connected to this node with explicit 'riskCategory' properties.
    This ensures the 'Cause' and 'Effect' logic is persisted in the DB, not just the frontend.
    """
    try:
        # 1. Tag Incoming CAUSES (e.g. LED_TO, CAUSED) -> riskCategory: Cause
        for label in CAUSE_LABELS:
            # FIX: Removed .iterate() - It caused the GraphCompileException
            query = f"g.V('{node_id}').inE('{label}').property('riskCategory', 'Cause')"
            
            # FIX: We must wait for the result (.result()) so the DB is updated 
            # BEFORE the next step reads it.
            try:
                client.submit_async(query).result() 
            except Exception:
                pass # Ignore if edge doesn't exist

        # 2. Tag Outgoing EFFECTS (e.g. RESULTED_IN) -> riskCategory: Effect
        for label in EFFECT_LABELS:
            # FIX: Removed .iterate()
            query = f"g.V('{node_id}').outE('{label}').property('riskCategory', 'Effect')"
            
            try:
                client.submit_async(query).result()
            except Exception:
                pass 
            
        logger.info(f"Enriched relationships for node {node_id}")
    except Exception as e:
        logger.warning(f"Failed to enrich relationships: {e}")

async def execute_gremlin(client, query):
    try:
        future = client.submit_async(query)
        if hasattr(future, 'result'): return future.result().all().result()
        else: return await future
    except Exception as e:
        logger.error(f"Query Failed: {e}")
        return []

async def execute_gremlin_scalar(client, query):
    try:
        res = await execute_gremlin(client, query)
        if res and isinstance(res, list): return res[0]
        return 0
    except:
        return 0

def format_properties(props):
    clean = {}
    for k, v in props.items():
        if isinstance(v, list) and len(v) > 0: clean[k] = v[0]
        else: clean[k] = v
    return clean

def generate_logic_summary(label, risks, causes, effects):
    closures = risks.get('closure_count', 0)
    complaints = risks.get('complaint_count', 0)
    
    summary = f"**Identity:** This entity is a **{label}** identified in the graph. "
    
    if closures > 0 or complaints > 0:
        summary += f"\n\n**Risk Assessment:** ⚠️ **HIGH PRIORITY**. System detected {closures} closures and {complaints} complaints linked here. This suggests operational risk."
        action = "Audit immediately."
    else:
        summary += "\n\n**Risk Assessment:** ✅ **Stable**. No immediate risk indicators (closures/complaints) found in database scan."
        action = "Continue monitoring."

    if effects:
        summary += f"\n\n**Next Steps:** {action} Review the downstream outcomes."
    else:
        summary += f"\n\n**Next Steps:** {action} Process ends here."
        
    return summary