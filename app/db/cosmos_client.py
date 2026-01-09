import logging
from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError

from app.config import settings  # <-- new addition

logger = logging.getLogger(__name__)

_gremlin_client = None


def get_gremlin_client() -> client.Client:
    """
    Returns a singleton Gremlin client for Cosmos DB.
    Client is created once per process and reused.
    """

    global _gremlin_client

    if _gremlin_client:
        return _gremlin_client

    # ---- Load configuration from settings ----
    endpoint = settings.COSMOS_GREMLIN_ENDPOINT
    key = settings.COSMOS_GREMLIN_KEY
    database = settings.COSMOS_GREMLIN_DATABASE
    container = settings.COSMOS_GREMLIN_CONTAINER

    # ---- Fail fast if config is missing ----
    if not all([endpoint, key, database, container]):
        raise RuntimeError(
            "Missing Cosmos Gremlin configuration. "
            "Ensure COSMOS_GREMLIN_ENDPOINT, COSMOS_GREMLIN_KEY, "
            "COSMOS_GREMLIN_DATABASE, and COSMOS_GREMLIN_CONTAINER are set."
        )

    # ---- Build resource path ----
    username = f"/dbs/{database}/colls/{container}"

    # ---- Create Gremlin client ----
    try:
        _gremlin_client = client.Client(
            url=endpoint,
            traversal_source="g",
            username=username,
            password=key,
            message_serializer=serializer.GraphSONSerializersV2d0(),
            transport_factory=None,  # default SSL/TLS
        )

        logger.info("Cosmos Gremlin client initialized successfully")
        return _gremlin_client

    except Exception as exc:
        logger.exception("Failed to initialize Cosmos Gremlin client")
        raise exc


def close_gremlin_client():
    """
    Close the Gremlin client cleanly (useful on app shutdown).
    """
    global _gremlin_client

    if _gremlin_client:
        _gremlin_client.close()
        _gremlin_client = None
        logger.info("Cosmos Gremlin client closed")
