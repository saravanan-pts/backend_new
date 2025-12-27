# Docker Environment Variables Guide

## Important: `.env.local` is NOT copied into the Docker image

For security reasons, environment files are **excluded** from the Docker image (see `.dockerignore`). Environment variables must be provided **at runtime** when running the container.

## How Environment Variables Work in Docker

### ✅ Correct Approach: Pass at Runtime

Environment variables are provided when you **run** the container, not baked into the image.

### Methods to Provide Environment Variables

#### 1. Using `--env-file` (Recommended for local testing)

```bash
docker run -p 3000:3000 --env-file .env.local irmai-kg-v2-surrealdb:latest
```

#### 2. Using `docker-compose.yml` (Recommended for local development)

The `docker-compose.yml` is configured to automatically load `.env.local`:

```bash
docker-compose up
```

#### 3. Using individual `-e` flags

```bash
docker run -p 3000:3000 \
  -e SURREALDB_URL=ws://localhost:8000/rpc \
  -e SURREALDB_USER=root \
  -e SURREALDB_PASS=root \
  -e AZURE_OPENAI_ENDPOINT=your-endpoint \
  -e AZURE_OPENAI_API_KEY=your-key \
  irmai-kg-v2-surrealdb:latest
```

#### 4. In GitLab CI / Production

Use GitLab CI/CD variables or Azure Container Apps environment variables:

```yaml
# GitLab CI
script:
  - docker run \
    -e SURREALDB_URL=$SURREALDB_URL \
    -e SURREALDB_USER=$SURREALDB_USER \
    -e SURREALDB_PASS=$SURREALDB_PASS \
    -e AZURE_OPENAI_ENDPOINT=$AZURE_OPENAI_ENDPOINT \
    -e AZURE_OPENAI_API_KEY=$AZURE_OPENAI_API_KEY \
    your-image:tag
```

## Why Not Copy `.env.local` into the Image?

1. **Security**: Secrets shouldn't be baked into images
2. **Flexibility**: Same image can be used with different environments
3. **Best Practice**: Follows Docker security guidelines
4. **Compliance**: Easier to audit and rotate secrets

## Required Environment Variables

Make sure your `.env.local` contains:

```bash
# SurrealDB Configuration
SURREALDB_URL=ws://localhost:8000/rpc
SURREALDB_USER=root
SURREALDB_PASS=root
SURREALDB_NS=test
SURREALDB_DB=test

# Azure OpenAI Configuration
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT_NAME=your-deployment-name
```

## Testing

To verify environment variables are loaded:

```bash
# Check env vars in running container
docker exec irmai-kg-v2-test env | grep SURREALDB
docker exec irmai-kg-v2-test env | grep AZURE
```

## Troubleshooting

### Container can't connect to SurrealDB

1. Check if `.env.local` exists and has correct values
2. Verify you're using `--env-file .env.local` when running
3. Check if SurrealDB is accessible from the container (use service name in docker-compose)

### Missing environment variables

The app will fail at runtime if required env vars are missing. Always use `--env-file` or set them explicitly.

