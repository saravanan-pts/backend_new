import logging
from typing import Optional, Dict, Any
from fastapi import APIRouter, UploadFile, File, Form, HTTPException

# Assumes document_processor is an instantiated object exported from this module
from app.services.document_processor import document_processor

router = APIRouter(tags=["Process"])
logger = logging.getLogger(__name__)


@router.post("")
async def process_document(
    file: Optional[UploadFile] = File(None),
    text: Optional[str] = Form(None),
) -> Dict[str, Any]:
    """
    Process a document or raw text and build a knowledge graph.

    Accepts:
    - CSV file
    - TXT file
    - Raw text input

    Returns:
    - count of entities and relationships added
    """

    # ---- Validation ----
    if not file and not text:
        raise HTTPException(
            status_code=400,
            detail="Either file or text input must be provided",
        )

    if file and text:
        raise HTTPException(
            status_code=400,
            detail="Provide only one of file or text, not both",
        )

    try:
        # ---- File processing ----
        if file:
            filename = file.filename.lower()

            if not (filename.endswith(".csv") or filename.endswith(".txt")):
                raise HTTPException(
                    status_code=400,
                    detail="Only CSV and TXT files are supported",
                )

            logger.info("Received file for processing: %s", file.filename)

            # 1. Read file content (This IS async in FastAPI)
            content = await file.read()
            
            # 2. Call the processor (Updated to await because processor is now async)
            # Ensure document_processor.process_file returns a dict with 'entities' and 'relationships' counts
            result = await document_processor.process_file(content, file.filename)

        # ---- Text processing ----
        else:
            logger.info("Received raw text for processing")
            # Updated to await
            result = await document_processor.process_text(text)

        return {
            "success": True,
            "data": result,
            "message": "Document processed successfully. Relationships established."
        }

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception("Failed to process document")
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )