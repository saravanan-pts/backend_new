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
        logger.warning("Missing Azure OpenAI env vars. Analysis will fall back to Logic mode.")
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
        # STEP 1: FETCH CORE NODE DETAILS
        # ---------------------------------------------------------
        target_query = f"g.V('{node_id}').project('props', 'label').by(valueMap()).by(label)"
        target_result = await execute_gremlin(client, target_query)
        
        if not target_result:
            return {"summary": f"Node '{node_id}' could not be located for analysis."}
            
        target_node = target_result[0]
        node_label = target_node.get('label', 'Unknown')
        node_props = format_properties(target_node.get('props', {}))
        node_name = node_props.get('name') or node_id
        node_risk = node_props.get('riskLevel', 'Unknown')

        # ---------------------------------------------------------
        # STEP 2: DYNAMIC CONTEXT GATHERING (The "GraphRAG" Approach)
        # Instead of hardcoding labels, we look for Semantic Risk Categories
        # and gather Global Context Hubs (Demographics, Geography, Assets)
        # ---------------------------------------------------------
        
        # A. Find Upstream Causes (What led to this?)
        cause_query = f"g.V('{node_id}').inE().has('riskCategory', 'Cause').outV().project('label', 'name').by(label).by(coalesce(values('name'), constant('Unknown'))).limit(5)"
        causes = await execute_gremlin(client, cause_query)

        # B. Find Downstream Effects (What did this result in?)
        effect_query = f"g.V('{node_id}').outE().has('riskCategory', 'Effect').inV().project('label', 'name').by(label).by(coalesce(values('name'), constant('Unknown'))).limit(5)"
        effects = await execute_gremlin(client, effect_query)

        # C. Find Global Context (Who/What/Where is involved?)
        # We look for static hubs attached to this node or its parent case
        context_query = f"""
        g.V('{node_id}').union(
            outE().hasLabel(within('PROFILED_AS','CATEGORIZED_BY','MANAGED_BY','BANKING_AT','COVERS_ASSET','LOCATED_IN')).inV(),
            inE().hasLabel(within('PROFILED_AS','CATEGORIZED_BY','MANAGED_BY','BANKING_AT','COVERS_ASSET','LOCATED_IN')).outV(),
            in().hasLabel('Case').out() // Look up to the parent case and see what else it connects to
        ).dedup().project('label', 'name').by(label).by(coalesce(values('name'), constant('Unknown'))).limit(10)
        """
        global_context = await execute_gremlin(client, context_query)

        # Format context for the LLM
        causes_text = [f"- {c['label']}: {c['name']}" for c in causes] if causes else ["- No immediate upstream anomalies detected."]
        effects_text = [f"- {e['label']}: {e['name']}" for e in effects] if effects else ["- No critical downstream consequences logged yet."]
        context_text = [f"- {ctx['label']}: {ctx['name']}" for ctx in global_context] if global_context else ["- Standard operational node (No extended demographic/asset context found)."]

        # ---------------------------------------------------------
        # STEP 3: ADVANCED AI PROMPTING
        # ---------------------------------------------------------
        system_prompt = """You are an elite Enterprise Process Mining AI. Your job is to analyze operational graph data and provide highly actionable business intelligence. 
        You must look beyond the obvious and identify systemic correlations between failures and demographics/products."""
        
        user_prompt = f"""
        ANALYZE THE FOLLOWING GRAPH NODE:
        
        [CORE IDENTITY]
        - Name: {node_name}
        - Type: {node_label}
        - Internal Risk Flag: {node_risk}
        - Properties: {node_props}

        [CAUSAL CHAIN]
        - Upstream Anomalies/Causes leading to this:
        {chr(10).join(causes_text)}
        
        - Downstream Consequences/Effects of this:
        {chr(10).join(effects_text)}

        [GLOBAL CONTEXT (Demographics, Geography, Products involved)]
        {chr(10).join(context_text)}

        INSTRUCTIONS:
        Write a concise, high-impact operational analysis formatted in Markdown. Use the following structure:
        
        ### Identity & Context
        Briefly explain what this entity is and its role in the overall workflow (e.g., "This is a Sale Failure associated with a Blue-Collar demographic").

        ### Risk & Root Cause Hypothesis
        Analyze the Causal Chain. If there are causes/effects, explain the likely sequence of failure. If the Internal Risk Flag is High/Medium, explain why based on the context. Are certain demographics or products correlating with this failure?

        ### Recommended Action
        Provide 1-2 bullet points of highly specific, actionable advice for operations, engineering, or management to resolve this pattern.
        """

        # ---------------------------------------------------------
        # STEP 4: GENERATE SUMMARY
        # ---------------------------------------------------------
        summary = ""
        if USE_REAL_AI and ai_client:
            try:
                logger.info("Sending advanced GraphRAG prompt to Azure OpenAI...")
                response = await ai_client.chat.completions.create(
                    model=AZURE_DEPLOYMENT,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=450,
                    temperature=0.2 # Lower temperature for highly analytical/logical responses
                )
                summary = response.choices[0].message.content
            except Exception as ai_e:
                logger.error(f"Azure AI Call Failed: {ai_e}. Reverting to logic.")
                summary = generate_logic_summary(node_name, node_label, causes, effects, global_context)
        else:
            summary = generate_logic_summary(node_name, node_label, causes, effects, global_context)

        return {"summary": summary}

    except Exception as e:
        logger.error(f"Analysis Endpoint Failed: {e}")
        return {"summary": f"Analysis unavailable due to server error."}

# --- GREMLIN HELPERS ---
async def execute_gremlin(client, query):
    try:
        future = client.submit_async(query)
        if hasattr(future, 'result'): return future.result().all().result()
        else: return await future
    except Exception as e:
        logger.error(f"Query Failed: {e}")
        return []

def format_properties(props):
    clean = {}
    for k, v in props.items():
        if isinstance(v, list) and len(v) > 0: clean[k] = v[0]
        else: clean[k] = v
    return clean

# --- FALLBACK LOGIC (If OpenAI is down/unconfigured) ---
def generate_logic_summary(name, label, causes, effects, context):
    summary = f"### Identity & Context\nThis entity is identified as **{name}** (Type: {label}). "
    
    if context:
        ctx_names = [f"**{c['name']}** ({c['label']})" for c in context[:3]]
        summary += f"It is contextually linked to {', '.join(ctx_names)}.\n\n"
    else:
        summary += "\n\n"

    summary += "### Risk Assessment\n"
    if causes or effects:
        summary += "Systemic workflow anomalies detected.\n"
        if causes:
            summary += f"- **Root Cause Alert:** Preceded by {causes[0]['name']}.\n"
        if effects:
            summary += f"- **Consequence Alert:** Resulted in {effects[0]['name']}.\n"
    else:
        summary += "No immediate structural anomalies (Causes/Effects) detected in the direct neighborhood.\n"

    summary += "\n### Recommended Action\n"
    if causes or effects:
        summary += "- **Audit Workflow:** Investigate the transition steps identified above to resolve the bottleneck.\n"
    else:
        summary += "- **Monitor:** Standard operational node. Proceed normally.\n"
        
    return summary