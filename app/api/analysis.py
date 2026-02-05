import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import logging
from openai import AsyncAzureOpenAI
from app.services.graph_service import graph_service

# Define Router
router = APIRouter(prefix="/api/graph", tags=["Analysis"])
logger = logging.getLogger(__name__)

# --- SIMPLIFIED REQUEST MODEL ---
# Matches exactly: { "nodeId": "val" }
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

        # 1. FETCH THE NODE
        target_query = f"g.V('{node_id}')"
        target_result = await execute_gremlin(client, target_query)
        
        if not target_result:
            logger.error(f"Node {node_id} not found in database.")
            # Start a fallback search if direct ID lookup fails
            return {"summary": f"Node '{node_id}' could not be analyzed directly. It may have special characters or be missing."}
            
        target_node = target_result[0]
        target_props = format_properties(target_node)
        node_name = target_props.get('name') or target_node.get('id')

        # 2. FIND CAUSES (Incoming)
        cause_query = f"g.V('{node_id}').inE().project('rel', 'source').by(label).by(outV().values('id'))"
        causes_result = await execute_gremlin(client, cause_query)
        causes_text = [f"- {c['source']} ({c['rel']})" for c in causes_result]

        # 3. FIND EFFECTS (Outgoing)
        effect_query = f"g.V('{node_id}').outE().project('rel', 'target').by(label).by(inV().values('id'))"
        effects_result = await execute_gremlin(client, effect_query)
        effects_text = [f"- ({e['rel']}) -> {e['target']}" for e in effects_result]

        # 4. PREPARE PROMPT
        context = f"""
        You are a process analyst analyzing a Knowledge Graph.
        
        SUBJECT NODE: {node_name} (ID: {node_id})
        TYPE: {target_node.get('label', 'Unknown')}
        PROPERTIES: {target_props}
        
        INCOMING LINKS (CAUSES/PREDECESSORS):
        {chr(10).join(causes_text) if causes_text else "(None - This is a start node)"}
        
        OUTGOING LINKS (EFFECTS/SUCCESSORS):
        {chr(10).join(effects_text) if effects_text else "(None - This is an end node)"}
        
        TASK:
        Write a concise 2-3 sentence summary explaining the role of this node in the process. 
        Explain what leads to it and what happens next. Do not list IDs unless necessary.
        """

        # 5. GENERATE SUMMARY
        summary = ""
        
        if USE_REAL_AI and ai_client:
            try:
                logger.info("Sending prompt to Azure OpenAI...")
                response = await ai_client.chat.completions.create(
                    model=AZURE_DEPLOYMENT, 
                    messages=[
                        {"role": "system", "content": "You are a helpful AI assistant summarizing graph data."},
                        {"role": "user", "content": context}
                    ],
                    max_tokens=200,
                    temperature=0.3
                )
                summary = response.choices[0].message.content
            except Exception as ai_e:
                logger.error(f"Azure AI Call Failed: {ai_e}. Reverting to logic.")
                summary = generate_logic_summary(node_name, causes_text, effects_text)
        else:
            summary = generate_logic_summary(node_name, causes_text, effects_text)

        return {"summary": summary}

    except Exception as e:
        logger.error(f"Analysis Failed: {e}")
        # Return a polite error instead of crashing to 500
        return {"summary": f"Analysis unavailable: {str(e)}"}

# --- Helpers ---

def generate_logic_summary(name, causes, effects):
    summary = f"**{name}** is a key node in the graph. "
    if causes:
        summary += f"It is triggered by {len(causes)} upstream events. "
    else:
        summary += "It appears to be a starting point. "
    if effects:
        summary += f"It leads to {len(effects)} downstream outcomes."
    else:
        summary += "It represents a terminal state."
    return summary

async def execute_gremlin(client, query):
    try:
        future = client.submit_async(query)
        if hasattr(future, 'result'): return future.result().all().result()
        else: return await future
    except Exception as e:
        logger.error(f"Query Failed: {e}")
        return []

def format_properties(node_data):
    props = {}
    raw = node_data.get('properties', {})
    for k, v in raw.items():
        if isinstance(v, list) and len(v) > 0: props[k] = v[0].get('value')
        else: props[k] = v
    return props