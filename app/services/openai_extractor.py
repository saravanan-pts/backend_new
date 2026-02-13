import json
import logging
from typing import Dict, Any, List
from openai import AsyncAzureOpenAI
from app.config import settings
from app.utils.json_sanitizer import clean_llm_json, try_parse_llm_json, validate_extraction_result

logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
AZURE_OPENAI_ENDPOINT = settings.AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_KEY = settings.AZURE_OPENAI_API_KEY
AZURE_OPENAI_DEPLOYMENT = settings.AZURE_OPENAI_DEPLOYMENT_NAME
AZURE_OPENAI_API_VERSION = settings.AZURE_OPENAI_API_VERSION

if not all([AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT]):
    logger.warning("Azure OpenAI not fully configured.")

client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)

def _post_process_entity(ent: Dict[str, Any]) -> Dict[str, Any]:
    label = str(ent.get("label", "")).strip()
    raw_type = str(ent.get("type", "Concept")).strip()
    
    # Prefix Cleaning
    if label.lower().startswith(raw_type.lower() + " "):
        clean_label = label[len(raw_type):].strip()
        if len(clean_label) > 1:
            ent["label"] = clean_label.title()

    if ent["label"].lower() in ["unknown", "none", "n/a", "null"]:
        ent["label"] = "Unknown"
    return ent

async def extract_entities_and_relationships(text: str) -> Dict[str, Any]:
    logger.info(f"OpenAI extractor: processing chunk of length {len(text)}")
    
    # UPGRADED PROMPT: Now trained on your new Enterprise Process Mining Ontology
    system_prompt = """
    You are an expert Graph Database Architect. Extract entities and relationships from the text.

    ### 1. RELATIONSHIP ENFORCEMENT
    You must extract process mining and business relationships using this strict Enterprise Ontology.
    
    **Demographics & Accounts:**
    - PROFILED_AS (e.g. Case -> Job)
    - CATEGORIZED_BY (e.g. Case -> Marital Status / Alarm Class)
    - MANAGED_BY (e.g. Case -> Branch)
    - BANKING_AT (e.g. Customer -> Branch)
    - HOLDS_ACCOUNT (e.g. Customer -> Account Type)
    
    **Financials:**
    - VALUED_AT (e.g. Case -> Claim Amount)
    - RECURRING_COST (e.g. Case -> Premium)
    - INITIALIZED_WITH (e.g. Case -> Opening Balance)
    
    **Process Flow (Critical):**
    - PERFORMS_ACTIVITY (e.g. Case -> Outbound Call Started)
    - NEXT_STEP (e.g. Activity 1 -> Activity 2)
    - CAUSES (Use if Activity 1 leads to an anomaly/failure in Activity 2)
    - RESULTED_IN (Use if Activity 1 leads to a terminal state like 'Closed' or 'Rejected')
    - TIME_STAMPED_ACTION (e.g. Activity -> Timestamp)

    ### 2. ENTITY TYPES
    Classify nodes strictly into these types:
    - Case, Customer, Branch, Job, Marital, Outcome, Activity, Time, Product, Amount, Agent.

    ### 3. OUTPUT JSON FORMAT
    {
      "entities": [{"label": "Entity Name", "type": "Entity Type"}],
      "relationships": [{"from": "Source Label", "to": "Target Label", "type": "RELATION_NAME"}]
    }
    """

    user_prompt = f"Extract graph data from this text:\n\n{text}"

    try:
        response = await client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=4096,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()
        
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            cleaned = clean_llm_json(content)
            data = try_parse_llm_json(cleaned)

        validated = validate_extraction_result(data)
        
        # Post-process
        final_entities = [_post_process_entity(e) for e in validated.get("entities", [])]
        relationships = validated.get("relationships", [])
        
        logger.info(f"✅ Extracted: {len(final_entities)} entities, {len(relationships)} relationships")
        return {"entities": final_entities, "relationships": relationships}

    except Exception as e:
        logger.exception(f"❌ OpenAI Extraction Error: {e}")
        return {"entities": [], "relationships": []}