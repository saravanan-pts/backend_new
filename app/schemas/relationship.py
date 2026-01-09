from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime


class RelationshipResponse(BaseModel):
    id: str
    fromEntityId: str
    toEntityId: str
    type: str
    confidence: Optional[float]
    sourceDocumentId: Optional[str]
    properties: Dict[str, Any] = {}
    createdAt: Optional[datetime]

    class Config:
        from_attributes = True
