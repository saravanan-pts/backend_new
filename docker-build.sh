#!/bin/bash

# Docker build script for local testing
# This script builds and optionally runs the Docker image locally

set -e

IMAGE_NAME="irmai-kg-v2-surrealdb"
IMAGE_TAG="${1:-latest}"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed"
    echo "Please install Docker Desktop from https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker daemon is not running"
    echo ""
    echo "Please start Docker Desktop:"
    echo "  1. Open Docker Desktop application"
    echo "  2. Wait for it to fully start (whale icon in menu bar should be steady)"
    echo "  3. Run this script again"
    echo ""
    exit 1
fi

echo "=========================================="
echo "🚀 Building Docker image"
echo "=========================================="
echo "📋 Configuration:"
echo "   Image: ${IMAGE_NAME}"
echo "   Tag: ${IMAGE_TAG}"
echo "   Full image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""

# Build the image
echo "🏗️  Building Docker image..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Build successful!"
    echo ""
    echo "📦 Image created: ${IMAGE_NAME}:${IMAGE_TAG}"
    echo ""
    # Check if port 3000 is available
    PORT=3000
    if lsof -Pi :${PORT} -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        echo "⚠️  Port ${PORT} is already in use"
        echo ""
        echo "Options:"
        echo "  1. Use a different port (e.g., 3001)"
        echo "  2. Stop the process using port ${PORT}"
        echo ""
        read -p "Enter port number to use (or press Enter to skip running): " PORT_INPUT
        if [ -z "$PORT_INPUT" ]; then
            echo "Skipping container run. You can run it later with:"
            echo "  docker run -p 3000:3000 --env-file .env.local ${IMAGE_NAME}:${IMAGE_TAG}"
            exit 0
        fi
        PORT=$PORT_INPUT
    fi
    
    echo ""
    echo "To run the container:"
    echo "  docker run -p ${PORT}:3000 --env-file .env.local ${IMAGE_NAME}:${IMAGE_TAG}"
    echo ""
    echo "Or use docker-compose:"
    echo "  docker-compose up"
    echo ""
    
    # Ask if user wants to run it
    read -p "Do you want to run the container now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🚀 Starting container on port ${PORT}..."
        docker run -p ${PORT}:3000 --env-file .env.local "${IMAGE_NAME}:${IMAGE_TAG}"
    fi
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi

