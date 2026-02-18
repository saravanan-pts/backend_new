import logging
import nest_asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from typing import Dict, Any
from pydantic import BaseModel

# Apply nest_asyncio to prevent event loop errors
nest_asyncio.apply()

# Load environment variables
load_dotenv()

from app.services.graph_service import graph_service
from app.repositories.graph_repository import graph_repository
from app.api import health, process, clear, entities, relationships, graph, documents, search, analysis

# ==========================================
# CLEAN LOGGING CONFIGURATION
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)

# 1. Silence noisy Azure/OpenAI HTTP requests
logging.getLogger("httpx").setLevel(logging.WARNING)

# 2. Filter out "/health" spam from the terminal
class HealthCheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(HealthCheckFilter())
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up... Connecting to Cosmos DB Gremlin API")
    await graph_repository.connect()
    yield
    logger.info("Shutting down... Closing connections")
    await graph_repository.close()

def create_app() -> FastAPI:
    app = FastAPI(
        title="Knowledge Graph Backend",
        description="FastAPI backend for document ingestion and graph generation",
        version="2.0.0",
        lifespan=lifespan
    )

    # ---- Explicit Origins for CORS ----
    origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://kg-ui.irmai.io",
        "http://localhost:3111",     
        "http://127.0.0.1:3111",     
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins, 
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Register Routers ----
    app.include_router(health.router, prefix="/api/health", tags=["Health"])
    app.include_router(process.router, prefix="/api/process", tags=["Process"])
    app.include_router(clear.router, prefix="/api/clear", tags=["Admin"]) 
    app.include_router(entities.router, prefix="/entities", tags=["Entities"])
    app.include_router(relationships.router, prefix="/relationships", tags=["Relationships"])
    app.include_router(graph.router) 
    app.include_router(documents.router)
    app.include_router(search.router) 
    app.include_router(analysis.router)

    class NeighborRequest(BaseModel):
        nodeId: str

    @app.post("/api/graph/neighbors")
    async def get_node_neighbors(request: NeighborRequest):
        try:
            data = await graph_service.get_neighbors(request.nodeId)
            return data
        except Exception as e:
            logger.error(f"Error in neighbor fetch: {str(e)}")
            return {"nodes": [], "edges": []}

    @app.get("/health")
    async def root_health_check():
        status = "connected" if graph_repository.client else "disconnected"
        return {"status": "ok", "database": status, "service": "Knowledge Graph Backend"}

    @app.get("/")
    async def root():
        return {
            "message": "Knowledge Graph Backend is running",
            "docs_url": "/docs",
            "health_url": "/health"
        }

    @app.post("/clear")
    async def root_clear_graph(payload: Dict[str, Any] = Body(default={"scope": "all"})):
        try:
            scope = payload.get("scope", "all")
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