import os
import logging
import json
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
from openai import AsyncAzureOpenAI

# Use the robust, async-safe graph_service to prevent WebSocket crashes
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
        # ---------------------------------------------------------
        # STEP 1: FETCH CORE NODE DETAILS
        # ---------------------------------------------------------
        target_query = f"g.V('{node_id}').project('props', 'label').by(valueMap()).by(label)"
        target_result = await graph_service._run_query_list(target_query)
        
        if not target_result:
            return {"summary": f"Node '{node_id}' could not be located for analysis."}
            
        target_node = target_result[0]
        node_label = target_node.get('label', 'Unknown')
        node_props = format_properties(target_node.get('props', {}))
        node_name = node_props.get('name') or node_id
        node_risk = node_props.get('riskLevel', 'Unknown')

        # ---------------------------------------------------------
        # STEP 2: DYNAMIC CONTEXT GATHERING (Enterprise GraphRAG)
        # ---------------------------------------------------------
        
        # A. Fetch 1-Hop Timeline (The Star Model Edges)
        neighbors = await graph_service.get_neighbors(node_id)
        
        timeline_events = []
        connected_nodes_map = {n['id']: n for n in neighbors.get('nodes', [])}
        
        for edge in neighbors.get('edges', []):
            target_id = edge['to'] if edge['from'] == node_id else edge['from']
            target_node = connected_nodes_map.get(target_id, {})
            
            target_name = target_node.get('properties', {}).get('name', target_id)
            target_type = target_node.get('label', 'Unknown')
            rel_label = edge.get('label', 'LINKED_TO')
            timestamp = edge.get('properties', {}).get('timestamp', 'Unknown Date')
            
            timeline_events.append({
                "date": timestamp,
                "desc": f"[{timestamp}] {rel_label} -> {target_name} ({target_type})"
            })
            
        # Sort chronologically
        timeline_events.sort(key=lambda x: x["date"])
        timeline_text = "\n".join([e["desc"] for e in timeline_events]) if timeline_events else "No historical interactions found."

        # B. Fetch Cause & Effect Chain (Risk Analysis)
        risk_query = f"""
        g.V('{node_id}').union(identity(), out('PERFORMS')).bothE().has('riskCategory', within('Cause', 'Effect')).dedup().project('edge_label', 'source_name', 'target_name').by(label).by(outV().coalesce(values('name'), id())).by(inV().coalesce(values('name'), id()))
        """
        risk_results = await graph_service._run_query_list(risk_query)
        
        risk_chain_text = ""
        if risk_results:
            chain_events = []
            for r in risk_results:
                chain_events.append(f"- {r['source_name']} [{r['edge_label']}] -> {r['target_name']}")
            risk_chain_text = "\n".join(chain_events)
        else:
            risk_chain_text = "- No critical Cause/Effect anomalies detected in this entity's immediate workflow."

        # C. Fetch Network Statistics (Comparative DB Analysis)
        stats_query = f"g.V('{node_id}').both().label().groupCount()"
        stats_result = await graph_service._run_query_list(stats_query)
        network_stats = str(stats_result[0]) if stats_result else "No broader network stats available."

        # ---------------------------------------------------------
        # STEP 3: ELITE ENTERPRISE PROMPTING
        # ---------------------------------------------------------
        system_prompt = """You are an elite Enterprise Process Mining & Risk Analyst AI. 
        Your objective is to analyze Knowledge Graph data to deliver high-impact, C-level business intelligence.
        You must align your analysis with core organizational missions: Operational Excellence, Customer Retention, and Risk Mitigation.
        
        CRITICAL RULES:
        1. Connect the dots: If multiple events share the EXACT SAME timestamp, they represent a single unified action.
        2. Identify anomalies: Pay close attention to the CAUSE and EFFECT chain. If an activity caused an anomaly, highlight it immediately.
        3. Be decisive: Provide solutions that balance operational efficiency with customer satisfaction.
        """
        
        user_prompt = f"""
        PERFORM A DEEP-DIVE ANALYSIS ON THE FOLLOWING ENTITY:
        
        [CORE IDENTITY]
        - Entity Name: {node_name}
        - Entity Type: {node_label}
        - Current Risk Flag: {node_risk}

        [CHRONOLOGICAL TIMELINE (INTERACTION HISTORY)]
        {timeline_text}

        [CAUSE & EFFECT RISK CHAIN]
        {risk_chain_text}

        [GLOBAL NETWORK STATISTICS (COMPARATIVE DB DATA)]
        This entity is connected to the following types of nodes across the database:
        {network_stats}

        INSTRUCTIONS:
        Write a highly professional, visually clean, and EXTREMELY CONCISE Markdown report. 
        Clients do not have time to read long paragraphs. You MUST use short bullet points, bold keywords, and punchy sentences.
        Use EXACTLY these four sections:
        
        ### The Bottom Line
        Maximum 2 sentences summarizing the entity's business value and its primary risk/bottleneck.
        
        ### Timeline at a Glance
        Maximum 4 short bullet points mapping the journey. Highlight products, amounts, and locations clearly.
        
        ### Key Risks & Anomalies
        2 to 3 short bullet points highlighting specific friction points. You MUST mention any triggers found in the CAUSE & EFFECT RISK CHAIN provided above.
        
        ### Action Plan
        2 punchy, highly specific business recommendations to fix the root cause or prevent future revenue leakage.
        """

        # ---------------------------------------------------------
        # STEP 4: GENERATE SUMMARY
        # ---------------------------------------------------------
        summary = ""
        if USE_REAL_AI and ai_client:
            try:
                logger.info("Sending Enterprise GraphRAG prompt to Azure OpenAI...")
                response = await ai_client.chat.completions.create(
                    model=AZURE_DEPLOYMENT,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=650,
                    temperature=0.3 
                )
                summary = response.choices[0].message.content
            except Exception as ai_e:
                logger.error(f"Azure AI Call Failed: {ai_e}. Reverting to logic.")
                summary = generate_logic_summary(node_name, node_label, timeline_events, network_stats, risk_chain_text)
        else:
            summary = generate_logic_summary(node_name, node_label, timeline_events, network_stats, risk_chain_text)

        return {"summary": summary}

    except Exception as e:
        logger.error(f"Analysis Endpoint Failed: {e}")
        return {"summary": f"Analysis unavailable due to server error. Details: {str(e)}"}

# --- NEW ENDPOINT: EXPORT RCA REPORTS ---
@router.get("/export-rca")
async def export_rca_reports():
    """
    Retrieves pre-computed Root Cause Analysis reports for all flagged Cases.
    This does NOT trigger AI costs; it reads results already saved in the DB.
    """
    query = """
    g.V().hasLabel('Case').has('rca_report')
      .project('Case_ID', 'Root_Cause', 'Business_Effect', 'AI_Analysis_Report')
      .by(coalesce(values('name'), id()))
      .by(coalesce(out('HAS_ROOT_CAUSE').values('name'), constant('N/A')))
      .by(coalesce(out('HAS_BUSINESS_EFFECT').values('name'), constant('N/A')))
      .by('rca_report')
    """
    try:
        results = await graph_service._run_query_list(query)
        return {"data": results}
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve database reports.")

# --- HELPERS ---
def format_properties(props):
    clean = {}
    for k, v in props.items():
        if isinstance(v, list) and len(v) > 0: clean[k] = v[0]
        else: clean[k] = v
    return clean

# --- FALLBACK LOGIC (Emoji-free) ---
def generate_logic_summary(name, label, timeline, stats, risk_chain):
    summary = f"### The Bottom Line\nThis entity is identified as **{name}** (Type: {label}). It serves as a standard operational node within the network.\n\n"
    
    summary += "### Timeline at a Glance\n"
    if timeline:
        for event in timeline[:5]: 
            summary += f"- {event['desc']}\n"
        if len(timeline) > 5:
            summary += f"- ...and {len(timeline)-5} more interactions.\n"
    else:
        summary += "- No historical interactions found in the graph.\n"

    summary += f"\n### Key Risks & Anomalies\n"
    if "No critical" not in risk_chain:
        summary += f"{risk_chain}\n"
    else:
        summary += "- No immediate causal anomalies detected.\n"
    summary += f"- DB Connectivity: {stats}\n"

    summary += "\n### Action Plan\n"
    summary += "- Audit Workflow: Investigate the timeline and anomalies above to ensure compliance.\n"
    summary += "- Data Enrichment: Integrate broader demographic data to enable full LLM analysis.\n"
        
    return summary