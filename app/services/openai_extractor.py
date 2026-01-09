import json
import logging
from typing import Dict, Any
from openai import AsyncAzureOpenAI
from app.config import settings
from app.utils.json_sanitizer import clean_llm_json, try_parse_llm_json, validate_extraction_result

logger = logging.getLogger(__name__)

# --- Load Azure OpenAI config ---
AZURE_OPENAI_ENDPOINT = settings.AZURE_OPENAI_ENDPOINT
AZURE_OPENAI_API_KEY = settings.AZURE_OPENAI_API_KEY
AZURE_OPENAI_DEPLOYMENT = settings.AZURE_OPENAI_DEPLOYMENT_NAME
AZURE_OPENAI_API_VERSION = settings.AZURE_OPENAI_API_VERSION

if not all([AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY, AZURE_OPENAI_DEPLOYMENT]):
    logger.warning(
        "Azure OpenAI not fully configured. "
        "openai_extractor may fail without proper credentials."
    )

# --- Initialize Azure OpenAI client (ASYNC) ---
client = AsyncAzureOpenAI(
    api_key=AZURE_OPENAI_API_KEY,
    api_version=AZURE_OPENAI_API_VERSION,
    azure_endpoint=AZURE_OPENAI_ENDPOINT
)


async def extract_entities_and_relationships(text: str) -> Dict[str, Any]:
    """
    Extract entities and relationships from text using Azure OpenAI.
    Returns:
    {
        "entities": [{"label": "name", "properties": {...}}],
        "relationships": [{"from": "entity1", "to": "entity2", "type": "REL_TYPE", "confidence": 0.9}]
    }
    """
    logger.info(
        "OpenAI extractor: processing text of length %d",
        len(text),
    )
    
    # Concise prompt optimized for token efficiency
    system_prompt = """Extract entities and relationships as JSON.

{
  "entities": [{"label": "Name", "properties": {"type": "Type", "description": "brief"}}],
  "relationships": [{"from": "Entity1", "to": "Entity2", "type": "RELATION", "confidence": 0.9}]
}

Rules:
- Match "from"/"to" to entity labels exactly
- UPPER_CASE relationship types (WORKS_AT, USES, MANAGES, etc.)
- Keep descriptions under 10 words
- Return only valid JSON"""

    user_prompt = f"""Extract all entities and their relationships:

{text}"""

    try:
        response = await client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=4096,  # Increased for longer responses
            response_format={"type": "json_object"}
        )
        
        content = response.choices[0].message.content.strip()
        finish_reason = response.choices[0].finish_reason
        
        # Warn if truncated
        if finish_reason == "length":
            logger.warning("⚠️ Response truncated - consider smaller chunks")
        
        logger.debug(f"Response: {len(content)} chars, finish_reason={finish_reason}")
        
        # Parse JSON
        try:
            data = json.loads(content)
            logger.info("✅ Parsed JSON successfully")
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse failed: {e}")
            
            # Show error context
            if hasattr(e, 'pos'):
                start = max(0, e.pos - 100)
                end = min(len(content), e.pos + 100)
                logger.debug(f"Error near: ...{content[start:end]}...")
            
            # Fallback parsing
            cleaned = clean_llm_json(content)
            data = try_parse_llm_json(cleaned)
            
            if data == {"entities": [], "relationships": []}:
                logger.error("❌ All parsing strategies failed")
                return {"entities": [], "relationships": []}
            
            logger.info("✅ Parsed after cleaning")
        
        # Validate structure
        validated = validate_extraction_result(data)
        
        entities = validated.get("entities", [])
        relationships = validated.get("relationships", [])
        
        logger.info(f"✅ Extracted: {len(entities)} entities, {len(relationships)} relationships")
        
        if entities:
            logger.debug(f"Entity sample: {entities[0]}")
        if relationships:
            logger.debug(f"Relationship sample: {relationships[0]}")
        else:
            logger.info("ℹ️ No relationships found in this chunk")
        
        return {"entities": entities, "relationships": relationships}
        
    except Exception as exc:
        logger.exception(f"❌ Azure OpenAI error: {exc}")
        return {"entities": [], "relationships": []}