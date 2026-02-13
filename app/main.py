import logging
import nest_asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from typing import Dict, Any
from pydantic import BaseModel  # <--- ADDED THIS

# Apply nest_asyncio to prevent event loop errors in some environments
nest_asyncio.apply()

# Load environment variables
load_dotenv()

# Import Service (needed for the root /clear endpoint and neighbors)
from app.services.graph_service import graph_service
from app.repositories.graph_repository import graph_repository

# Import Routers
from app.api import health, process, clear, entities, relationships, graph, documents, search, analysis

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)

# Lifespan context manager for startup/shutdown events
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up... Connecting to Cosmos DB Gremlin API")
    await graph_repository.connect()
    yield
    # Shutdown
    logger.info("Shutting down... Closing connections")
    await graph_repository.close()

def create_app() -> FastAPI:
    app = FastAPI(
        title="Knowledge Graph Backend",
        description="FastAPI backend for document ingestion and graph generation",
        version="2.0.0",
        lifespan=lifespan
    )

    # ---- CORS ----
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "*"], 
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Register Routers ----
    # Health & Processing
    app.include_router(health.router, prefix="/api/health", tags=["Health"])
    app.include_router(process.router, prefix="/api/process", tags=["Process"])
    
    # Admin / Clear
    app.include_router(clear.router, prefix="/api/clear", tags=["Admin"]) 
    
    # Entities & Relationships (CRUD)
    app.include_router(entities.router, prefix="/entities", tags=["Entities"])
    app.include_router(relationships.router, prefix="/relationships", tags=["Relationships"])
    
    # Graph Visualization & Documents
    app.include_router(graph.router) 
    app.include_router(documents.router)

    # Search Router
    app.include_router(search.router) 

    # Analysis Router
    app.include_router(analysis.router)

    # ==========================================
    # NEW ENDPOINT: LAZY LOADING (NEIGHBORS)
    # ==========================================
    class NeighborRequest(BaseModel):
        nodeId: str

    @app.post("/api/graph/neighbors")
    async def get_node_neighbors(request: NeighborRequest):
        """
        Called when a user clicks a node in the UI.
        Fetches the node and its direct connections from Cosmos DB.
        """
        try:
            # Calls the new method we added to GraphService
            data = await graph_service.get_neighbors(request.nodeId)
            return data
        except Exception as e:
            logger.error(f"Error in neighbor fetch: {str(e)}")
            # Return empty structure so UI doesn't crash
            return {"nodes": [], "edges": []}
    # ==========================================

    # --- Root Health Check ---
    @app.get("/health")
    async def root_health_check():
        status = "connected" if graph_repository.client else "disconnected"
        return {"status": "ok", "database": status, "service": "Knowledge Graph Backend"}

    # --- Root Endpoint (Fixes 404 logs on base URL) ---
    @app.get("/")
    async def root():
        return {
            "message": "Knowledge Graph Backend is running",
            "docs_url": "/docs",
            "health_url": "/health"
        }

    # --- Root Clear Endpoint ---
    @app.post("/clear")
    async def root_clear_graph(payload: Dict[str, Any] = Body(default={"scope": "all"})):
        try:
            scope = payload.get("scope", "all")
            logger.info(f"Root /clear called with scope: {scope}")
            count = await graph_repository.clear_graph(scope)
            return {"status": "success", "message": f"Graph cleared ({scope})", "deleted_count": count}
        except Exception as e:
            logger.error(f"Root clear failed: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)