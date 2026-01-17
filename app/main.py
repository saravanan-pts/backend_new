import nest_asyncio
nest_asyncio.apply()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

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
    app.include_router(clear_router, prefix="/clear", tags=["Admin"])
    app.include_router(entities_router, prefix="/entities", tags=["Entities"])
    app.include_router(relationships_router, prefix="/relationships", tags=["Relationships"])
    app.include_router(graph_router) 
    app.include_router(docs_router)

    # --- CRITICAL FIX: Root Health Check for Frontend ---
    @app.get("/health")
    def root_health_check():
        return {"status": "ok", "service": "Knowledge Graph Backend"}

    return app

app = create_app()