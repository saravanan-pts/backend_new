from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import Optional, Dict, Any
import logging

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

            logger.info("Received file for processing: %s", filename)
            result = await document_processor.process_file(file)

        # ---- Text processing ----
        else:
            logger.info("Received raw text for processing")
            result = await document_processor.process_text(text)

        return {
            "success": True,
            "data": result,
        }

    except HTTPException:
        raise

    except Exception as exc:
        logger.exception("Failed to process document")
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )
