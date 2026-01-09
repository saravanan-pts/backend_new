from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class DocumentBase(BaseModel):
    filename: str
    fileType: str


class DocumentCreate(DocumentBase):
    content: str


class DocumentResponse(DocumentBase):
    id: str
    processedAt: datetime
    entityCount: int
    relationshipCount: int

    class Config:
        from_attributes = True
