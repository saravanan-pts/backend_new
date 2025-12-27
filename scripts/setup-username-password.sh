#!/bin/bash

# Script to set up username/password authentication in .env.local

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

ENV_FILE=".env.local"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}SurrealDB Username/Password Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if .env.local exists
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${YELLOW}⚠️  $ENV_FILE not found. Creating from template...${NC}"
    
    # Create basic .env.local file
    cat > "$ENV_FILE" << 'EOF'
# ============================================
# SurrealDB Configuration
# ============================================
NEXT_PUBLIC_SURREALDB_URL=wss://your-instance.surreal.cloud
NEXT_PUBLIC_SURREALDB_NAMESPACE=demo
NEXT_PUBLIC_SURREALDB_DATABASE=surreal_deal_store

# ============================================
# Authentication Options (Choose ONE method)
# ============================================

# Option 1: JWT Token Authentication (Comment out if using username/password)
# SURREALDB_TOKEN=

# Option 2: Username/Password Authentication
SURREALDB_USERNAME=admin
SURREALDB_PASSWORD=admin

# ============================================
# Azure OpenAI Configuration
# ============================================
AZURE_OPENAI_ENDPOINT=
AZURE_OPENAI_API_KEY=
AZURE_OPENAI_DEPLOYMENT_NAME=
EOF
    echo -e "${GREEN}✅ Created $ENV_FILE${NC}"
fi

# Backup .env.local
cp "$ENV_FILE" "${ENV_FILE}.backup"
echo -e "${GREEN}✅ Backup created: ${ENV_FILE}.backup${NC}"
echo ""

# Prompt for username
read -p "$(echo -e ${BLUE}Enter SurrealDB username [default: admin]: ${NC})" username
username=${username:-admin}

# Prompt for password
read -sp "$(echo -e ${BLUE}Enter SurrealDB password [default: admin]: ${NC})" password
echo ""
password=${password:-admin}

# Update username
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    if grep -q "^SURREALDB_USERNAME=" "$ENV_FILE"; then
        sed -i '' "s|^SURREALDB_USERNAME=.*|SURREALDB_USERNAME=$username|" "$ENV_FILE"
    else
        echo "SURREALDB_USERNAME=$username" >> "$ENV_FILE"
    fi
    
    # Update password
    if grep -q "^SURREALDB_PASSWORD=" "$ENV_FILE"; then
        sed -i '' "s|^SURREALDB_PASSWORD=.*|SURREALDB_PASSWORD=$password|" "$ENV_FILE"
    else
        echo "SURREALDB_PASSWORD=$password" >> "$ENV_FILE"
    fi
    
    # Comment out token if it exists
    if grep -q "^SURREALDB_TOKEN=" "$ENV_FILE"; then
        sed -i '' "s|^SURREALDB_TOKEN=|# SURREALDB_TOKEN=|" "$ENV_FILE"
    fi
else
    # Linux
    if grep -q "^SURREALDB_USERNAME=" "$ENV_FILE"; then
        sed -i "s|^SURREALDB_USERNAME=.*|SURREALDB_USERNAME=$username|" "$ENV_FILE"
    else
        echo "SURREALDB_USERNAME=$username" >> "$ENV_FILE"
    fi
    
    # Update password
    if grep -q "^SURREALDB_PASSWORD=" "$ENV_FILE"; then
        sed -i "s|^SURREALDB_PASSWORD=.*|SURREALDB_PASSWORD=$password|" "$ENV_FILE"
    else
        echo "SURREALDB_PASSWORD=$password" >> "$ENV_FILE"
    fi
    
    # Comment out token if it exists
    if grep -q "^SURREALDB_TOKEN=" "$ENV_FILE"; then
        sed -i "s|^SURREALDB_TOKEN=|# SURREALDB_TOKEN=|" "$ENV_FILE"
    fi
fi

echo ""
echo -e "${GREEN}✅ Username and password configured!${NC}"
echo ""
echo -e "${BLUE}Configuration:${NC}"
echo "  Username: $username"
echo "  Password: ${password:0:1}*** (hidden)"
echo ""
echo -e "${YELLOW}⚠️  Security Note:${NC}"
echo "  Default credentials (admin/admin) should be changed in production!"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Restart your development server (Ctrl+C then npm run dev)"
echo "2. Check the connection indicator in the UI (should be green)"
echo "3. Try uploading text or a file to verify"
echo ""

