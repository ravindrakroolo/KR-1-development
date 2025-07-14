#!/bin/bash

# Enterprise Search Pipeline API Server Startup Script
# This script sets up the environment and starts the FastAPI server

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Enterprise Search Pipeline API Server${NC}"
echo -e "${BLUE}===========================================${NC}"

# Check if we're in the right directory
if [ ! -f "fastapi_file.py" ]; then
    echo -e "${RED}❌ Error: fastapi_file.py not found in current directory${NC}"
    echo -e "${YELLOW}Please run this script from the pipeline directory${NC}"
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo -e "${YELLOW}⚠️  Warning: .env file not found${NC}"
    echo -e "${YELLOW}   Please create a .env file with your configuration${NC}"
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Error: Python 3 is not installed or not in PATH${NC}"
    exit 1
fi

# Check if uvicorn is installed
if ! python3 -c "import uvicorn" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  uvicorn not found. Installing...${NC}"
    pip3 install uvicorn[standard]
fi

# Check if fastapi is installed
if ! python3 -c "import fastapi" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  FastAPI not found. Installing...${NC}"
    pip3 install fastapi
fi

# Set environment variables
export PYTHONPATH="$(pwd):$PYTHONPATH"

echo -e "${GREEN}✅ Environment checks passed${NC}"
echo -e "${BLUE}📁 Working directory: $(pwd)${NC}"
echo -e "${BLUE}🌐 Server will be available at: http://localhost:8000${NC}"
echo -e "${BLUE}📚 API docs will be available at: http://localhost:8000/docs${NC}"
echo -e "${BLUE}🔄 Auto-reload enabled for development${NC}"
echo ""
echo -e "${GREEN}Starting server...${NC}"
echo ""

# Start the server
python3 -c "
import uvicorn
uvicorn.run('fastapi_file:app', host='0.0.0.0', port=8000, reload=True)
" || {
    echo -e "${RED}❌ Failed to start server${NC}"
    echo -e "${YELLOW}Try running: python3 start_api.py${NC}"
    exit 1
}