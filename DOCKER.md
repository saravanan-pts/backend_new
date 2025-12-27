# Docker Build and Run Guide

This guide explains how to build and run the application using Docker locally.

## Prerequisites

- Docker Desktop installed and running
- `.env.local` file with required environment variables

## Quick Start

### Option 1: Using the build script

```bash
./docker-build.sh
```

This will:
1. Build the Docker image
2. Optionally run the container

### Option 2: Using docker-compose

```bash
# Build and run
docker-compose up --build

# Run in detached mode
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Option 3: Manual Docker commands

```bash
# Build the image
docker build -t irmai-kg-v2-surrealdb:latest .

# Run the container
docker run -p 3000:3000 --env-file .env.local irmai-kg-v2-surrealdb:latest
```

## Environment Variables

Make sure your `.env.local` file contains all required environment variables:

- `SURREALDB_URL` - SurrealDB connection URL
- `SURREALDB_USER` - SurrealDB username
- `SURREALDB_PASS` - SurrealDB password
- `AZURE_OPENAI_ENDPOINT` - Azure OpenAI endpoint
- `AZURE_OPENAI_API_KEY` - Azure OpenAI API key

## Troubleshooting

### Build fails with TypeScript errors

Make sure you've fixed all TypeScript errors locally first:
```bash
npm run build
```

### Port already in use

Change the port mapping:
```bash
docker run -p 3001:3000 --env-file .env.local irmai-kg-v2-surrealdb:latest
```

### Environment variables not loading

Ensure `.env.local` exists and contains all required variables. You can also pass them directly:
```bash
docker run -p 3000:3000 \
  -e SURREALDB_URL=ws://localhost:8000/rpc \
  -e SURREALDB_USER=root \
  -e SURREALDB_PASS=root \
  irmai-kg-v2-surrealdb:latest
```

## Differences from GitLab CI

The local Dockerfile uses:
- **Node 20** instead of Node 18 (fixes Azure package warnings)
- **Standalone output** mode for optimized production builds

If you need to match GitLab CI exactly, change the base image in Dockerfile to `node:18-alpine`.

