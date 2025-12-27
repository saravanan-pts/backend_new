#!/bin/bash

# ============================================
# Knowledge Graph POC - Master Startup Script
# ============================================
# This script starts the application for testing
# Usage: ./scripts/master.sh [options]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Functions
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if .env.local exists
check_env_file() {
    if [ ! -f ".env.local" ]; then
        print_warning ".env.local file not found!"
        print_info "Creating .env.local from template..."
        
        if [ -f ".env.example" ]; then
            cp .env.example .env.local
            print_success ".env.local created from .env.example"
            print_warning "Please edit .env.local with your credentials"
        else
            print_error ".env.example not found. Please create .env.local manually"
            exit 1
        fi
    else
        print_success ".env.local file exists"
    fi
}

# Check Node.js
check_node() {
    if ! command -v node &> /dev/null; then
        print_error "Node.js is not installed"
        print_info "Please install Node.js 18+ from https://nodejs.org/"
        exit 1
    fi
    
    NODE_VERSION=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VERSION" -lt 18 ]; then
        print_error "Node.js version 18+ required. Current: $(node --version)"
        exit 1
    fi
    
    print_success "Node.js $(node --version) detected"
}

# Check npm
check_npm() {
    if ! command -v npm &> /dev/null; then
        print_error "npm is not installed"
        exit 1
    fi
    
    print_success "npm $(npm --version) detected"
}

# Check dependencies
check_dependencies() {
    if [ ! -d "node_modules" ]; then
        print_warning "node_modules not found. Installing dependencies..."
        npm install
        print_success "Dependencies installed"
    else
        print_success "Dependencies found"
    fi
}

# Check if port is available
check_port() {
    PORT=${1:-3111}
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
        print_warning "Port $PORT is already in use"
        print_info "Attempting to kill process on port $PORT..."
        lsof -ti:$PORT | xargs kill -9 2>/dev/null || true
        sleep 2
        if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
            print_error "Could not free port $PORT. Please free it manually"
            exit 1
        else
            print_success "Port $PORT is now available"
        fi
    else
        print_success "Port $PORT is available"
    fi
}

# Build check
check_build() {
    print_info "Checking if application builds..."
    if npm run build > /dev/null 2>&1; then
        print_success "Application builds successfully"
    else
        print_warning "Build check failed, but continuing..."
        print_info "You may see build errors when starting the dev server"
    fi
}

# Start the application
start_app() {
    print_header "Starting Knowledge Graph POC Application"
    
    # Pre-flight checks
    print_info "Running pre-flight checks..."
    check_node
    check_npm
    check_env_file
    check_dependencies
    check_port $PORT
    
    # Optional build check (commented out for faster startup)
    # check_build
    
    print_header "Starting Development Server"
    print_info "Server will start on: http://localhost:$PORT"
    print_info "Press Ctrl+C to stop the server"
    echo ""
    
    # Start the dev server
    npm run dev
}

# Show help
show_help() {
    echo "Knowledge Graph POC - Master Startup Script"
    echo ""
    echo "Usage: ./scripts/master.sh [options]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help message"
    echo "  -c, --check    Run checks only (don't start server)"
    echo "  -b, --build    Build the application before starting"
    echo "  -p, --port     Specify port (default: 3111)"
    echo ""
    echo "Examples:"
    echo "  ./scripts/master.sh              # Start the app"
    echo "  ./scripts/master.sh --check      # Run checks only"
    echo "  ./scripts/master.sh --build      # Build then start"
    echo "  ./scripts/master.sh --port 3001  # Start on port 3001"
}

# Parse arguments
CHECK_ONLY=false
BUILD_FIRST=false
PORT=3111

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -c|--check)
            CHECK_ONLY=true
            shift
            ;;
        -b|--build)
            BUILD_FIRST=true
            shift
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
if [ "$CHECK_ONLY" = true ]; then
    print_header "Running Pre-flight Checks"
    check_node
    check_npm
    check_env_file
    check_dependencies
    check_port $PORT
    print_success "All checks passed!"
    exit 0
fi

if [ "$BUILD_FIRST" = true ]; then
    print_info "Building application first..."
    npm run build
    print_success "Build completed"
fi

start_app

