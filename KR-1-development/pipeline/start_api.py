#!/usr/bin/env python3
"""
Startup script for Enterprise Search Pipeline FastAPI server
This script ensures proper environment setup and starts the server
"""

import os
import sys
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir))

# Set environment variables if not already set
if not os.getenv('PYTHONPATH'):
    os.environ['PYTHONPATH'] = str(current_dir)

def main():
    """Main startup function"""
    try:
        import uvicorn
        
        print("🚀 Starting Enterprise Search Pipeline API Server...")
        print(f"📁 Working directory: {current_dir}")
        print(f"🐍 Python path: {sys.path[0]}")
        print("🌐 Server will be available at: http://localhost:8000")
        print("📚 API docs will be available at: http://localhost:8000/docs")
        print("" + "="*60)
        
        # Start the server
        uvicorn.run(
            "fastapi_file:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            reload_dirs=[str(current_dir)],
            log_level="info"
        )
        
    except ImportError:
        print("❌ Error: uvicorn not installed. Please install it with:")
        print("   pip install uvicorn")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹️  Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()