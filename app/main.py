import logging
import nest_asyncio
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from typing import Dict, Any

# Apply nest_asyncio to prevent event loop errors
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Import Routers
from app.api.health import router as health_router
from app.api.process import router as process_router
from app.api.clear import router as clear_router
from app.api.entities import router as entities_router
from app.api.relationships import router as relationships_router
from app.api.graph import router as graph_router
from app.api.documents import router as docs_router

# Import Service
from app.services.graph_service import graph_service

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_app() -> FastAPI:
    app = FastAPI(
        title="Knowledge Graph Backend",
        description="FastAPI backend for document ingestion and graph generation",
        version="1.0.0",
    )

    # ---- CORS ----
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], 
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Register Routers ----
    app.include_router(health_router, prefix="/api/health", tags=["Health"])
    app.include_router(process_router, prefix="/api/process", tags=["Process"])
    
    # This creates /api/clear, but frontend might check /clear
    app.include_router(clear_router, prefix="/api/clear", tags=["Admin"]) 
    
    app.include_router(entities_router, prefix="/entities", tags=["Entities"])
    app.include_router(relationships_router, prefix="/relationships", tags=["Relationships"])
    
    # These routers usually define their own prefixes internally
    app.include_router(graph_router) 
    app.include_router(docs_router)

    # --- CRITICAL FIX 1: Root Health Check ---
    @app.get("/health")
    async def root_health_check():
        return {"status": "ok", "service": "Knowledge Graph Backend"}

    # --- CRITICAL FIX 2: Root Clear Endpoint ---
    # This captures the "POST /clear" request from your frontend
    # FIX: Added default body to prevent 422 Unprocessable Entity
    @app.post("/clear")
    async def root_clear_graph(payload: Dict[str, Any] = Body(default={"scope": "all"})):
        try:
            scope = payload.get("scope", "all")
            logger.info(f"Root /clear called with scope: {scope}")
            result = await graph_service.clear_graph(scope)
            return {"status": "success", "message": f"Graph cleared ({scope})"}
        except Exception as e:
            logger.error(f"Root clear failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)