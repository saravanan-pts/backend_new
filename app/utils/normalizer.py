import re

def normalize_entity_type(raw_type: str, label: str) -> str:
    """
    Standardizes entity types based on keywords in the type or label.
    Example: "A0001" (Account) -> "Account", "Collision Repair" -> "Organization"
    """
    t = (raw_type or "").lower()
    l = (label or "").lower()

    # 1. EVENTS (Process Flow)
    if any(x in t for x in ["activity", "event", "call", "process"]) or \
       any(x in l for x in ["activated", "closed", "initiated", "received", "started", "ended", "application"]):
        return "Event"

    # 2. TIME
    if "time" in t or re.match(r"^\d{4}-\d{2}-\d{2}", l):
        return "Time"

    # 3. ORGANIZATIONS (Banking & Insurance)
    if any(x in t for x in ["org", "company"]) or \
       any(x in l for x in ["towing", "repair", "mechanic", "collision", "auto body", "glass", "service", "inc", "ltd"]):
        return "Organization"

    # --- NEW RULE: BRANCH ---
    if "branch" in t or "branch" in l:
        return "Branch"

    # 4. PEOPLE
    if any(x in t for x in ["person", "director", "agent", "customer", "driver"]) or \
       l.startswith("c0") or "name" in l:
        return "Person"

    # 5. LOCATIONS
    if any(x in t for x in ["loc", "city", "address", "state", "zip"]):
        return "Location"
    
    # 6. ACCOUNTS
    if "account" in t or l.startswith("a0") or any(x in l for x in ["savings", "checking", "deposit"]):
        return "Account"

    # 7. CLAIMS & POLICIES
    if "claim" in t or "policy" in t:
        return "Claim"

    # 8. VEHICLES
    if "vehicle" in t or "car" in t or any(x in l for x in ["honda", "toyota", "bmw", "ford"]):
        return "Vehicle"

    # Default Fallback (Capitalize first letter if valid, else Concept)
    return raw_type.capitalize() if raw_type else "Concept"