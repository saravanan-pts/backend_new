import os
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
import logging
from openai import AsyncAzureOpenAI
from app.services.graph_service import graph_service
# Note: If your Gremlin driver doesn't support TextP, we handle the fallback in the query logic below.
# from gremlin_python.process.traversal import TextP 

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

        # 1. FETCH THE TARGET NODE
        target_query = f"g.V('{node_id}').project('props', 'label').by(valueMap()).by(label)"
        target_result = await execute_gremlin(client, target_query)
        
        if not target_result:
            logger.error(f"Node {node_id} not found in database.")
            return {"summary": f"Node '{node_id}' could not be analyzed directly."}
            
        target_node = target_result[0]
        node_label = target_node.get('label', 'Unknown')
        node_props = format_properties(target_node.get('props', {}))
        node_name = node_props.get('name') or node_id

        # 2. RISK SCANNING (The Backend Logic)
        # We query the database to find specific risk indicators connected to this node.
        risk_context = {}
        
        # A. Closures/Rejections (High Priority Risk)
        # Scan outgoing/incoming nodes for labels containing "Closed" or "Rejected"
        closure_query = f"g.V('{node_id}').both().has('label', TextP.containing('Closed')).count()"
        
        # Fallback query if TextP isn't available in your driver version:
        # closure_query = f"g.V('{node_id}').out().hasLabel('Account Closed').count()"
        
        try:
            risk_context['closure_count'] = await execute_gremlin_scalar(client, closure_query)
        except:
            # If the complex query fails, assume 0 for safety
            risk_context['closure_count'] = 0
        
        # B. Complaints/Disputes (High Priority Risk)
        complaint_query = f"g.V('{node_id}').both().has('label', TextP.containing('Complaint')).count()"
        try:
            risk_context['complaint_count'] = await execute_gremlin_scalar(client, complaint_query)
        except:
            risk_context['complaint_count'] = 0

        # 3. GATHER CONTEXT (Causes & Effects)
        # Upstream (Causes)
        cause_query = f"g.V('{node_id}').inE().project('rel', 'source').by(label).by(outV().label()).limit(10)"
        causes_result = await execute_gremlin(client, cause_query)
        causes_text = [f"- {c['source']} ({c['rel']})" for c in causes_result]

        # Downstream (Effects/Next Steps)
        effect_query = f"g.V('{node_id}').outE().project('rel', 'target').by(label).by(inV().label()).limit(10)"
        effects_result = await execute_gremlin(client, effect_query)
        effects_text = [f"- ({e['rel']}) -> {e['target']}" for e in effects_result]

        # 4. PREPARE AI PROMPT
        # We explicitly tell the AI to act as a Car Insurance Analyst and use the Risk Data we found.
        system_prompt = "You are a Senior Risk Analyst for a Car Insurance company. Your goal is to analyze graph entities to identify operational risks, root causes, and necessary actions."
        
        user_prompt = f"""
        ENTITY REPORT:
        - Name: {node_name} (ID: {node_id})
        - Type: {node_label}
        - Properties: {node_props}

        RISK INDICATORS (Database Scan):
        - Related Closures/Rejections: {risk_context['closure_count']}
        - Related Complaints/Disputes: {risk_context['complaint_count']}

        CONTEXT:
        - Incoming Events (Causes): {chr(10).join(causes_text) if causes_text else "(None - Start Node)"}
        - Outgoing Events (Effects): {chr(10).join(effects_text) if effects_text else "(None - End Node)"}

        INSTRUCTIONS:
        Write a normalized analysis in exactly these 3 sections:
        1. **Identity**: Briefly explain what this entity is (e.g., "A Branch managing 50 accounts" or "A Policy Case").
        2. **Risk Assessment**: 
           - If Closures > 0 or Complaints > 0: Mark as **HIGH PRIORITY**. Explain the specific risk (churn/dissatisfaction).
           - Otherwise: Mark as **Stable**.
        3. **Next Steps**: Recommend a specific action based on the risk (e.g., "Audit Branch", "Contact Customer", "Monitor").
        """

        # 5. GENERATE SUMMARY
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

async def execute_gremlin(client, query):
    try:
        future = client.submit_async(query)
        if hasattr(future, 'result'): return future.result().all().result()
        else: return await future
    except Exception as e:
        logger.error(f"Query Failed: {e}")
        return []

async def execute_gremlin_scalar(client, query):
    """Executes a query designed to return a single number (like .count())."""
    try:
        res = await execute_gremlin(client, query)
        if res and isinstance(res, list): return res[0]
        return 0
    except:
        return 0

def format_properties(props):
    # Cleans up Gremlin's {'key': ['val']} format to {'key': 'val'}
    clean = {}
    for k, v in props.items():
        if isinstance(v, list) and len(v) > 0: clean[k] = v[0]
        else: clean[k] = v
    return clean

def generate_logic_summary(label, risks, causes, effects):
    # Fallback Logic (Normalized Format) used if AI is down
    closures = risks.get('closure_count', 0)
    complaints = risks.get('complaint_count', 0)
    
    # 1. Identity
    summary = f"**Identity:** This entity is a **{label}** identified in the graph. "
    
    # 2. Risk Assessment
    if closures > 0 or complaints > 0:
        summary += f"\n\n**Risk Assessment:** ⚠️ **HIGH PRIORITY**. System detected {closures} closures and {complaints} complaints linked here. This suggests operational risk."
        action = "Audit immediately."
    else:
        summary += "\n\n**Risk Assessment:** ✅ **Stable**. No immediate risk indicators (closures/complaints) found in database scan."
        action = "Continue monitoring."

    # 3. Next Steps
    if effects:
        summary += f"\n\n**Next Steps:** {action} Review the {len(effects)} downstream outcomes."
    else:
        summary += f"\n\n**Next Steps:** {action} Process ends here."
        
    return summary