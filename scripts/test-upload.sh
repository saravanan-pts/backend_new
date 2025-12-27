#!/bin/bash

# Script to test file upload via API

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TEST_FILE="data/data.txt"
API_URL="http://localhost:3000/api/process"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Testing File Upload${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if file exists
if [ ! -f "$TEST_FILE" ]; then
    echo -e "${RED}❌ Test file not found: $TEST_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Test file found: $TEST_FILE${NC}"
echo ""

# Check if server is running
if ! curl -s http://localhost:3000 > /dev/null; then
    echo -e "${YELLOW}⚠️  Server doesn't seem to be running on port 3000${NC}"
    echo -e "${BLUE}Starting server in background...${NC}"
    cd "$(dirname "$0")/.."
    npm run dev > /dev/null 2>&1 &
    SERVER_PID=$!
    echo -e "${BLUE}Waiting for server to start...${NC}"
    sleep 5
fi

echo -e "${BLUE}Uploading file: $TEST_FILE${NC}"
echo ""

# Upload file
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
  -F "file=@$TEST_FILE" \
  "$API_URL")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

echo -e "${BLUE}HTTP Status: $HTTP_CODE${NC}"
echo ""

if [ "$HTTP_CODE" -eq 200 ]; then
    echo -e "${GREEN}✅ Upload successful!${NC}"
    echo ""
    echo -e "${BLUE}Response:${NC}"
    echo "$BODY" | jq '.' 2>/dev/null || echo "$BODY"
else
    echo -e "${RED}❌ Upload failed!${NC}"
    echo ""
    echo -e "${BLUE}Response:${NC}"
    echo "$BODY" | jq '.' 2>/dev/null || echo "$BODY"
    exit 1
fi

