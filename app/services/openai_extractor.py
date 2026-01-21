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
    
    system_prompt = """
    You are an expert Graph Database Architect. Extract entities and relationships from the text.

    ### 1. RELATIONSHIP ENFORCEMENT
    The text explicitly contains uppercase relationship verbs (e.g., "is PROFILED_AS"). 
    **You MUST use these exact relationship names.**
    
    - If text says: "Case 1 is PROFILED_AS Job 'Management'"
      -> Create Relation: {from: "Case 1", to: "Management", type: "PROFILED_AS"}
    
    - If text says: "Case 1 PERFORMS_ACTIVITY 'Call'"
      -> Create Relation: {from: "Case 1", to: "Call", type: "PERFORMS_ACTIVITY"}

    - If text says: "Activity 'Call' OCCURRED_ON Time '12:00'"
      -> Create Relation: {from: "Call", to: "12:00", type: "OCCURRED_ON"}

    ### 2. ENTITY TYPES
    - **Case**: Numeric IDs.
    - **Activity**: "Call Started", "Sale Closed", "Email".
    - **Job**: "Management", "Technician".
    - **Status**: "Single", "Married", "No_Result".
    - **Product**: "Savings", "Loan".
    - **Branch**: Location names.

    ### 3. OUTPUT JSON
    {
      "entities": [{"label": "Entity Name", "type": "Entity Type"}],
      "relationships": [{"from": "Source", "to": "Target", "type": "RELATION_NAME"}]
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