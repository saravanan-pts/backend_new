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
    """
    Safety Layer: Fixes 'Job Mechanic' -> 'Mechanic' if the AI fails.
    """
    label = str(ent.get("label", "")).strip()
    raw_type = str(ent.get("type", "Concept")).strip()
    
    # 1. REMOVE PREFIXES (The Fix for your issue)
    # If Type is "Job" and Label is "Job Mechanic", strip "Job"
    if label.lower().startswith(raw_type.lower() + " "):
        clean_label = label[len(raw_type):].strip()
        # Only apply if we didn't strip everything away (e.g., don't strip "Job" from "Job")
        if len(clean_label) > 1:
            ent["label"] = clean_label
            # Also Title Case it for consistency
            ent["label"] = ent["label"].title()

    # 2. HANDLE "UNKNOWN" or "NONE"
    if ent["label"].lower() in ["unknown", "none", "n/a", "null"]:
        ent["label"] = "Unknown"

    return ent

async def extract_entities_and_relationships(text: str) -> Dict[str, Any]:
    logger.info(f"OpenAI extractor: processing text of length {len(text)}")
    
    # --- PROMPT: The Brain ---
    system_prompt = """
    You are an expert Knowledge Graph Architect. Extract structured data from the text.

    ### 1. SCHEMA CONSISTENCY RULES (CRITICAL)
    The text often follows the pattern: "The **[Category]** is '**[Value]**'".
    - **Rule A:** Use the **[Category]** as the **Entity Type**.
    - **Rule B:** Use the **[Value]** as the **Entity Label**.
    - **Rule C (NO REPETITION):** Do NOT include the Category in the Label.
      - **Bad:** Type="Job", Label="Job Mechanic"
      - **Good:** Type="Job", Label="Mechanic"

    ### 2. CLASSIFICATION RULES
    If the text is generic, classify into these Standard Types:
    - **Person**: Names, Customers, Agents.
    - **Job**: Roles like "Admin", "Technician", "Nurse", "Cleaner".
    - **Organization**: Companies, Agencies.
    - **Event**: Actions like "Call Started", "Sale Closed".
    - **Location**: Cities, States.
    - **Time**: Dates.
    - **Case**: Case IDs.
    - **Status**: "Married", "Single", "Active".

    ### 3. OUTPUT FORMAT
    Return ONLY valid JSON:
    {
      "entities": [{"label": "Name", "type": "Type", "properties": {"description": "brief"}}],
      "relationships": [{"from": "Entity1", "to": "Entity2", "type": "RELATION", "confidence": 1.0}]
    }
    """

    user_prompt = f"Extract knowledge graph data from this text:\n\n{text}"

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
        
        # --- PARSING ---
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            cleaned = clean_llm_json(content)
            data = try_parse_llm_json(cleaned)

        validated = validate_extraction_result(data)
        
        # --- POST-PROCESSING (The Safety Net) ---
        raw_entities = validated.get("entities", [])
        final_entities = []
        
        for ent in raw_entities:
            # Apply the cleaning logic to every single entity
            clean_ent = _post_process_entity(ent)
            final_entities.append(clean_ent)

        relationships = validated.get("relationships", [])
        
        logger.info(f"✅ Extracted: {len(final_entities)} entities, {len(relationships)} relationships")
        return {"entities": final_entities, "relationships": relationships}

    except Exception as e:
        logger.exception(f"❌ OpenAI Extraction Error: {e}")
        return {"entities": [], "relationships": []}