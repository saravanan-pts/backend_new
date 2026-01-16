import nest_asyncio  # <--- FIXED: Patching the loop
nest_asyncio.apply() # <--- FIXED: Applying the patch

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API routers
from app.api.health import router as health_router
from app.api.process import router as process_router
from app.api.clear import router as clear_router
from app.api.entities import router as entities_router
from app.api.relationships import router as relationships_router
# NEW: Import the new routers
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
        allow_origins=["*"],          # tighten in production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Routers ----
    app.include_router(health_router, prefix="/health", tags=["Health"])
    app.include_router(process_router, prefix="/process", tags=["Process"])
    app.include_router(clear_router)
    app.include_router(entities_router, prefix="/entities", tags=["Entities"])
    app.include_router(relationships_router, prefix="/relationships", tags=["Relationships"])
    
    # NEW: Register the Magic Map and Library Logbook controllers
    # The prefixes (/api/graph and /api/documents) are already set inside the files
    app.include_router(graph_router)
    app.include_router(docs_router)

    return app


app = create_app()