#!/bin/bash

# Script to help update SurrealDB token in .env.local

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ENV_FILE=".env.local"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SurrealDB Token Updater${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if .env.local exists
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${RED}❌ Error: $ENV_FILE not found${NC}"
    echo "Please create it first by copying .env.example"
    exit 1
fi

echo -e "${YELLOW}⚠️  Important:${NC}"
echo "1. Go to https://surrealdb.com/cloud"
echo "2. Navigate to your instance → Authentication → Tokens"
echo "3. Generate a new token with full permissions"
echo "4. Copy the token"
echo ""

# Prompt for new token
read -p "$(echo -e ${BLUE}Enter your new SurrealDB JWT token: ${NC})" new_token

if [ -z "$new_token" ]; then
    echo -e "${RED}❌ Error: Token cannot be empty${NC}"
    exit 1
fi

# Validate token format (JWT tokens start with 'eyJ')
if [[ ! $new_token =~ ^eyJ ]]; then
    echo -e "${YELLOW}⚠️  Warning: Token doesn't look like a JWT (should start with 'eyJ')${NC}"
    read -p "Continue anyway? (y/N): " confirm
    if [[ ! $confirm =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 1
    fi
fi

# Backup .env.local
cp "$ENV_FILE" "${ENV_FILE}.backup"
echo -e "${GREEN}✅ Backup created: ${ENV_FILE}.backup${NC}"

# Update token in .env.local
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s|SURREALDB_TOKEN=.*|SURREALDB_TOKEN=$new_token|" "$ENV_FILE"
else
    # Linux
    sed -i "s|SURREALDB_TOKEN=.*|SURREALDB_TOKEN=$new_token|" "$ENV_FILE"
fi

# Verify the update
if grep -q "SURREALDB_TOKEN=$new_token" "$ENV_FILE"; then
    echo -e "${GREEN}✅ Token updated successfully!${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Restart your development server (Ctrl+C then npm run dev)"
    echo "2. Check the connection indicator in the UI"
    echo "3. Try uploading text or a file to verify"
else
    echo -e "${RED}❌ Error: Failed to update token${NC}"
    echo "Restoring backup..."
    mv "${ENV_FILE}.backup" "$ENV_FILE"
    exit 1
fi

