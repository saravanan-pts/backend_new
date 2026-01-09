from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, ValidationError


class Settings(BaseSettings):
    # =========================
    # Azure OpenAI
    # =========================

    AZURE_OPENAI_ENDPOINT: str = Field(
        ...,
        description="Azure OpenAI endpoint, e.g. https://your-resource.openai.azure.com"
    )

    AZURE_OPENAI_API_KEY: str = Field(
        ...,
        description="Azure OpenAI API key"
    )

    AZURE_OPENAI_DEPLOYMENT_NAME: str = Field(
        ...,
        description="Azure OpenAI deployment name, e.g. gpt-4.1-mini"
    )

    AZURE_OPENAI_EMBEDDING_MODEL: str = Field(
        ...,
        description="Azure OpenAI embedding model name"
    )

    AZURE_OPENAI_GENERATION_MODEL: str = Field(
        ...,
        description="Azure OpenAI generation model name"
    )

    AZURE_OPENAI_API_VERSION: str = Field(
        ...,
        description="Azure OpenAI API version, e.g. 2024-02-15-preview"
    )

    # =========================
    # Cosmos DB Gremlin API
    # =========================

    COSMOS_GREMLIN_ENDPOINT: str = Field(
        ...,
        description="Cosmos DB Gremlin endpoint, e.g. wss://your-account.gremlin.cosmos.azure.com:443/"
    )

    COSMOS_GREMLIN_DATABASE: str = Field(
        ...,
        description="Cosmos DB Gremlin database name"
    )

    COSMOS_GREMLIN_CONTAINER: str = Field(
        ...,
        description="Cosmos DB Gremlin graph container (collection) name"
    )

    COSMOS_GREMLIN_KEY: str = Field(
        ...,
        description="Cosmos DB Gremlin primary key"
    )

    # =========================
    # Application Environment
    # =========================

    ENV: str = Field(
        default="development",
        description="Application environment: development | staging | production"
    )

    model_config = SettingsConfigDict(
        env_file=".env",              # load from .env file
        env_file_encoding="utf-8",
        extra="ignore",               # ignore extra env vars
        populate_by_name=True,        # allow exact field matching
    )


# =========================
# Fail fast for missing config
# =========================
try:
    settings = Settings()
except ValidationError as exc:
    print("\n❌ Environment configuration error in .env file\n")
    for err in exc.errors():
        print(f"- {err['loc'][0]}: {err['msg']}")
    raise RuntimeError("Missing or invalid environment variables") from exc
