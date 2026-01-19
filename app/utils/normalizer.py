import re

def normalize_entity_type(raw_type: str, raw_label: str) -> str:
    """
    Hybrid Normalizer: Trusts the AI's 'raw_type' mostly, 
    but applies strict overrides for critical business logic.
    """
    # Defensive coding: handle None
    t = (raw_type or "Concept").strip()
    l = (raw_label or "").lower().strip()

    # --- 1. COLUMN HEADER MAPPINGS (New) ---
    # Ensures generic CSV headers map to standard business types
    HEADER_MAPPINGS = {
        # Generic -> Specific
        "name": "Person",
        "full name": "Person",
        "customer": "Person",
        "agent": "Person",
        
        # IDs -> Business Objects
        "id": "Case",
        "key": "Case",
        "ticket": "Case",
        "policy number": "Policy",
        
        # Metadata
        "timestamp": "Time",
        "date": "Time",
        "created at": "Time",
        
        # Synonyms
        "job title": "Job",
        "role": "Job",
        "occupation": "Job",
        "company": "Organization",
        "agency": "Organization"
    }

    if t.lower() in HEADER_MAPPINGS:
        return HEADER_MAPPINGS[t.lower()]

    # --- 2. CRITICAL OVERRIDES (Regex Safety Net) ---
    # Matches "Case 123", "Case #1"
    if "case" in l or re.match(r'case\s*#?\d+', l): 
        return "Case"

    # Fix common AI inconsistencies for Branch
    if "branch" in l: 
        return "Branch"

    # --- 3. FALLBACK ---
    # Trust the AI's classification if it isn't generic
    return t.title() if t.lower() != "concept" else "Concept"