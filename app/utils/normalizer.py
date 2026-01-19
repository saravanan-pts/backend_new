import re

def normalize_entity_type(raw_type: str, raw_label: str) -> str:
    """
    Standardizes entity types based on keywords in the type or label.
    """
    t = (raw_type or "").lower().strip()
    l = (raw_label or "").lower().strip()

    # 0. EXPLICIT OVERRIDES
    if t == "job":
        return "Job"

    # --- RULE 0.1: CASE IDs ---
    if "case" in l or re.match(r'case\s*\d+', l):
        return "Case"

    # --- RULE 0.2: MARITAL STATUS ---
    if l in ["single", "married", "divorced", "widowed", "separated"]:
        return "MaritalStatus"

    # --- RULE 0.3: BRANCH (Fix for 'Branch' showing as Concept) ---
    if "branch" in t or "branch" in l:
        return "Branch"

    # 1. EVENTS (Process Flow)
    if any(x in t for x in ["activity", "event", "call", "process", "action"]) or \
       any(x in l for x in ["activated", "closed", "initiated", "received", "started", "ended", "application", "outbound", "inbound"]):
        return "Event"

    # 2. TIME
    if "time" in t or re.match(r"^\d{4}-\d{2}-\d{2}", l):
        return "Time"

    # 3. JOBS / ROLES
    job_titles = [
        "management", "blue-collar", "technician", "admin.", "admin", 
        "services", "retired", "student", "housemaid", "unemployed", 
        "entrepreneur", "self-employed", "job", "mechanic"
    ]
    if "job" in t or l in job_titles or any(job in l for job in job_titles):
        return "Job"

    # 4. ORGANIZATIONS
    if any(x in t for x in ["org", "company"]) or \
       any(x in l for x in ["towing", "repair", "collision", "auto body", "glass", "service", "inc", "ltd", "bank", "insurance"]):
        return "Organization"

    # 6. PEOPLE
    if any(x in t for x in ["person", "director", "agent", "customer", "driver"]) or \
       l.startswith("c0") or "name" in l:
        return "Person"

    # 7. LOCATIONS
    if any(x in t for x in ["loc", "city", "address", "state", "zip", "place"]):
        return "Location"
    
    # 8. ACCOUNTS
    if "account" in t or l.startswith("a0") or any(x in l for x in ["savings", "checking", "deposit"]):
        return "Account"

    # 9. CLAIMS & POLICIES
    if "claim" in t or "policy" in t:
        return "Claim"

    # 10. VEHICLES
    if "vehicle" in t or "car" in t or any(x in l for x in ["honda", "toyota", "bmw", "ford", "vehicle"]):
        return "Vehicle"

    # Fallback
    return raw_type.title() if raw_type and raw_type.lower() != "concept" else "Concept"