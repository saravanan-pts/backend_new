from pydantic import BaseModel
from typing import Dict, Any, Optional
from datetime import datetime


class EntityBase(BaseModel):
    label: str
    type: str


class EntityResponse(EntityBase):
    id: str
    properties: Dict[str, Any] = {}
    metadata: Dict[str, Any] = {}
    createdAt: Optional[datetime]

    class Config:
        from_attributes = True
