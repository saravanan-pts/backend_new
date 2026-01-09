from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def health_check():
    """
    Health check endpoint.
    Used by load balancers, Docker, Kubernetes, Azure, etc.
    """
    return {"status": "ok"}
