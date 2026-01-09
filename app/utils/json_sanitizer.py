import re
import json
import logging

logger = logging.getLogger(__name__)


def clean_llm_json(raw: str) -> str:
    """
    Clean common JSON formatting issues from LLM responses.
    """
    # Remove markdown code blocks
    raw = re.sub(r"```json\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```\s*", "", raw)
    raw = raw.strip()
    
    # Remove trailing commas before closing braces/brackets
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    
    # Quote unquoted keys
    raw = re.sub(r'(\{|,)\s*([A-Za-z0-9_]+)\s*:', r'\1 "\2":', raw)
    
    # Fix common escape issues
    raw = raw.replace('\\n', ' ')
    raw = raw.replace('\\t', ' ')
    
    # Remove control characters
    raw = re.sub(r'[\x00-\x1F\x7F]', '', raw)
    
    return raw


def fix_unterminated_strings(json_str: str) -> str:
    """
    Attempt to fix unterminated strings in JSON by finding the last occurrence
    of a quote and ensuring strings are properly closed.
    """
    try:
        # Find all string boundaries
        in_string = False
        escape_next = False
        fixed_chars = []
        
        for i, char in enumerate(json_str):
            if escape_next:
                fixed_chars.append(char)
                escape_next = False
                continue
                
            if char == '\\':
                escape_next = True
                fixed_chars.append(char)
                continue
                
            if char == '"':
                in_string = not in_string
                fixed_chars.append(char)
            else:
                fixed_chars.append(char)
        
        # If we ended in a string, close it
        if in_string:
            fixed_chars.append('"')
            logger.debug("Fixed unterminated string")
        
        return ''.join(fixed_chars)
    except Exception as e:
        logger.debug(f"String fix failed: {e}")
        return json_str


def extract_json_object(text: str) -> str:
    """
    Extract the first complete JSON object from text.
    Handles nested braces correctly.
    """
    # Find first opening brace
    start = text.find('{')
    if start == -1:
        return text
    
    # Count braces to find matching closing brace
    brace_count = 0
    in_string = False
    escape_next = False
    
    for i in range(start, len(text)):
        char = text[i]
        
        if escape_next:
            escape_next = False
            continue
            
        if char == '\\':
            escape_next = True
            continue
            
        if char == '"' and not escape_next:
            in_string = not in_string
            continue
            
        if not in_string:
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    # Found matching closing brace
                    return text[start:i+1]
    
    # If we get here, couldn't find matching brace
    # Return from start to end
    return text[start:]


def try_parse_llm_json(cleaned: str):
    """
    Attempt to parse JSON with multiple fallback strategies.
    """
    # Strategy 1: Direct parse
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        logger.debug(f"Direct parse failed: {e}")
    
    # Strategy 2: Extract JSON object and parse
    try:
        extracted = extract_json_object(cleaned)
        return json.loads(extracted)
    except json.JSONDecodeError as e:
        logger.debug(f"Extraction parse failed: {e}")
    
    # Strategy 3: Fix unterminated strings
    try:
        fixed = fix_unterminated_strings(cleaned)
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        logger.debug(f"String fix parse failed: {e}")
    
    # Strategy 4: Extract + Fix strings
    try:
        extracted = extract_json_object(cleaned)
        fixed = fix_unterminated_strings(extracted)
        return json.loads(fixed)
    except json.JSONDecodeError as e:
        logger.debug(f"Combined fix parse failed: {e}")
    
    # Strategy 5: Truncate at last valid closing brace
    try:
        # Find the last complete JSON structure
        last_brace = cleaned.rfind('}')
        if last_brace != -1:
            truncated = cleaned[:last_brace+1]
            return json.loads(truncated)
    except json.JSONDecodeError as e:
        logger.debug(f"Truncation parse failed: {e}")
    
    # All strategies failed
    logger.warning("All JSON parse strategies failed, returning empty structure")
    return {"entities": [], "relationships": []}


def validate_extraction_result(data: dict) -> dict:
    """
    Validate and clean the extraction result structure.
    """
    if not isinstance(data, dict):
        return {"entities": [], "relationships": []}
    
    # Ensure entities exist and are a list
    entities = data.get("entities", [])
    if not isinstance(entities, list):
        entities = []
    
    # Ensure relationships exist and are a list
    relationships = data.get("relationships", [])
    if not isinstance(relationships, list):
        relationships = []
    
    # Clean entities
    cleaned_entities = []
    for ent in entities:
        if isinstance(ent, dict) and "label" in ent:
            cleaned_ent = {
                "label": str(ent["label"]).strip(),
                "properties": ent.get("properties", {})
            }
            # Ensure properties is a dict
            if not isinstance(cleaned_ent["properties"], dict):
                cleaned_ent["properties"] = {}
            
            # Only add if label is not empty
            if cleaned_ent["label"]:
                cleaned_entities.append(cleaned_ent)
    
    # Clean relationships
    cleaned_relationships = []
    for rel in relationships:
        if isinstance(rel, dict) and all(k in rel for k in ["from", "to", "type"]):
            from_label = str(rel["from"]).strip()
            to_label = str(rel["to"]).strip()
            rel_type = str(rel["type"]).strip()
            
            # Only add if all required fields are non-empty
            if from_label and to_label and rel_type:
                cleaned_rel = {
                    "from": from_label,
                    "to": to_label,
                    "type": rel_type,
                    "confidence": float(rel.get("confidence", 0.9))
                }
                cleaned_relationships.append(cleaned_rel)
    
    return {
        "entities": cleaned_entities,
        "relationships": cleaned_relationships
    }