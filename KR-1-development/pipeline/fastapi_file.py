from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import asyncio
import sys
import os
from datetime import datetime
import traceback

# Load environment variables FIRST
from dotenv import load_dotenv
load_dotenv()  # This loads the .env file

# Import the pipeline from your script
from EnterpriseSearchPipeline import EnterpriseSearchPipeline

# Import the new utils
from utils import EnterpriseSearchUtils

app = FastAPI(
    title="Enterprise Search Pipeline API",
    description="FastAPI wrapper for Enterprise Search Pipeline with multi-account support",
    version="1.0.0"
)

# Pydantic Models for Request Bodies

class IngestRequest(BaseModel):
    services: List[str] = Field(..., description="Services to process")
    account_google_drive: Optional[str] = Field(None, description="Google Drive account ID")
    account_slack: Optional[str] = Field(None, description="Slack account ID")
    account_dropbox: Optional[str] = Field(None, description="Dropbox account ID")
    account_jira: Optional[str] = Field(None, description="Jira account ID")
    account_sharepoint: Optional[str] = Field(None, description="SharePoint account ID")
    account_confluence: Optional[str] = Field(None, description="Confluence account ID")
    external_user_id: str = Field(..., description="External user ID")
    user_email: str = Field(..., description="User email")
    limit: Optional[int] = Field(None, description="Limit number of items per service")
    empty: bool = Field(False, description="Empty collections before ingesting")
    chunkall: bool = Field(False, description="Store all chunks for large documents")

class SearchRequest(BaseModel):
    query: str = Field(..., description="Search query")
    user: str = Field(..., description="User email (required for access control)")
    external_user_id: Optional[str] = Field(None, description="External user ID for filtering")
    account_id: Optional[str] = Field(None, description="Specific account ID to filter results")
    apps: Optional[List[str]] = Field(None, description="Filter by integrations")
    author: Optional[str] = Field(None, description="Filter by author email")
    file_types: Optional[List[str]] = Field(None, description="Filter by file types")
    from_date: Optional[str] = Field(None, description="From date (YYYY-MM-DD)")
    to_date: Optional[str] = Field(None, description="To date (YYYY-MM-DD)")
    sort_by: str = Field("relevance", description="Sort by relevance or date")
    limit: int = Field(10, description="Number of results")
    include_failed_sync: bool = Field(False, description="Include failed sync documents")

class EmptyRequest(BaseModel):
    confirm: bool = Field(..., description="Confirm deletion of all documents")

class FetchInfoRequest(BaseModel):
    external_user_id: str = Field(..., description="External user ID")
    limit: Optional[int] = Field(None, description="Limit number of files")
    all: bool = Field(False, description="Fetch all files (no limit)")

class FetchByAccountRequest(BaseModel):
    account_id: str = Field(..., description="Account ID")
    limit: Optional[int] = Field(None, description="Limit number of files")
    all: bool = Field(False, description="Fetch all files (no limit)")

# NEW PYDANTIC MODELS FOR UTILS.PY ENDPOINTS

class FetchInfoRBACRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    integration_type: Optional[str] = Field(None, description="Optional filter ('google_drive', 'slack', etc.)")
    limit: Optional[int] = Field(None, description="Optional limit on results")

class DynamicSuggestionsRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    partial_query: str = Field(..., description="Partial search query for suggestions (REQUIRED)")
    limit: int = Field(10, description="Number of suggestions to return (default: 10)")

class SyncStatusRBACRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    integration_type: Optional[str] = Field(None, description="Optional integration filter ('google_drive', 'slack', etc.)")

# Global pipeline instance
pipeline = None

# Global utils instance
utils_instance = None

def get_pipeline(empty_collections: bool = False, chunk_all: bool = False):
    """Get or create pipeline instance with error handling"""
    global pipeline
    if pipeline is None or empty_collections or chunk_all:
        try:
            # Check critical environment variables before initializing
            required_vars = ["TOKEN_ASTRA_DB"]
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            
            if missing_vars:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "Missing required environment variables",
                        "missing_variables": missing_vars,
                        "env_file_exists": os.path.exists(".env"),
                        "working_directory": os.getcwd(),
                        "solution": "Check your .env file in the current directory and ensure it contains the required variables"
                    }
                )
            
            pipeline = EnterpriseSearchPipeline(
                empty_collections=empty_collections, 
                chunk_all=chunk_all
            )
        except HTTPException:
            # Re-raise HTTP exceptions
            raise
        except Exception as e:
            # Handle other initialization errors
            raise HTTPException(
                status_code=500, 
                detail={
                    "error": "Failed to initialize pipeline",
                    "details": str(e),
                    "type": type(e).__name__,
                    "env_file_exists": os.path.exists(".env"),
                    "working_directory": os.getcwd()
                }
            )
    return pipeline

def get_utils():
    """Get or create utils instance with error handling"""
    global utils_instance
    if utils_instance is None:
        try:
            utils_instance = EnterpriseSearchUtils()
        except Exception as e:
            raise HTTPException(
                status_code=500, 
                detail={
                    "error": "Failed to initialize EnterpriseSearchUtils",
                    "details": str(e),
                    "type": type(e).__name__
                }
            )
    return utils_instance

# API Endpoints
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Enterprise Search Pipeline API",
        "version": "2.0.0",
        "endpoints": {
            # CORE ENDPOINTS
            "ingest": "POST /ingest - Ingest data from enterprise tools",
            "search": "POST /search - Enhanced search with RBAC enforcement",
            "dashboard": "GET /dashboard - Show analytics dashboard",
            "sync_status": "GET /sync-status - Check sync status of all documents",
            "empty": "POST /empty - Empty existing collections",
            "fetch_info": "POST /fetch-info - Fetch file info for external user ID",
            "fetch_by_account": "POST /fetch-by-account - Fetch files by account ID",
            
            # NEW RBAC ENDPOINTS
            "fetch_info_rbac": "POST /fetch-info-rbac - Fetch files using NEW access control structure",
            "dynamic_suggestions": "POST /dynamic-suggestions - Get dynamic autocomplete suggestions with NEW RBAC",
            "sync_status_rbac": "POST /sync-status-rbac - Get sync status using NEW access control structure",
            
            # NEW USER ANALYTICS ENDPOINTS
            "trending_searches": "POST /trending-searches - Get trending searches with NEW RBAC",
            "suggested_documents": "POST /suggested-documents - Get suggested documents with NEW RBAC", 
            "recent_searches": "POST /recent-searches - Get user's recent searches with NEW RBAC",
            
            # NEW SYSTEM ANALYTICS ENDPOINTS
            "analytics_search": "GET /analytics/search - Get comprehensive search analytics",
            "analytics_dashboard": "GET /analytics/dashboard - Get full analytics dashboard",
            "analytics_trending": "GET /analytics/trending/{limit} - Get trending queries (limit optional)",
            
            # UTILITY ENDPOINTS
            "health": "GET /health - Health check endpoint",
            "debug_env": "GET /debug/env - Debug environment variables"
        },
        "new_rbac_features": {
            "description": "New endpoints using EnterpriseSearchUtils with enhanced RBAC",
            "required_fields": "account_id + external_user_id + user_email (ALL THREE MANDATORY)",
            "structure": "new_access_control_only"
        },
        "analytics_features": {
            "description": "Comprehensive analytics for search behavior and system performance",
            "user_analytics": "Track searches, suggestions, and document access patterns",
            "system_analytics": "Monitor performance, RBAC usage, and trending patterns",
            "real_time_data": "Live analytics with search patterns and user behavior insights"
        },
        "api_documentation": {
            "swagger_ui": "/docs",
            "redoc": "/redoc",
            "openapi_json": "/openapi.json"
        }
    }
    """Root endpoint with API information"""
    return {
        "message": "Enterprise Search Pipeline API",
        "version": "2.0.0",
        "endpoints": {
            "ingest": "POST /ingest - Ingest data from enterprise tools",
            "search": "POST /search - Enhanced search with RBAC enforcement",
            "dashboard": "GET /dashboard - Show analytics dashboard",
            "sync_status": "GET /sync-status - Check sync status of all documents",
            "empty": "POST /empty - Empty existing collections",
            "fetch_info": "POST /fetch-info - Fetch file info for external user ID",
            "fetch_by_account": "POST /fetch-by-account - Fetch files by account ID",
            # NEW RBAC ENDPOINTS
            "fetch_info_rbac": "POST /fetch-info-rbac - Fetch files using NEW access control structure",
            "dynamic_suggestions": "POST /dynamic-suggestions - Get dynamic autocomplete suggestions with NEW RBAC",
            "sync_status_rbac": "POST /sync-status-rbac - Get sync status using NEW access control structure",
            # NEW ANALYTICS ENDPOINTS
            "trending_searches": "POST /trending-searches - Get trending searches with NEW RBAC",
            "suggested_documents": "POST /suggested-documents - Get suggested documents with NEW RBAC",
            "recent_searches": "POST /recent-searches - Get user's recent searches with NEW RBAC"
        },
        "new_rbac_features": {
            "description": "New endpoints using EnterpriseSearchUtils with enhanced RBAC",
            "required_fields": "account_id + external_user_id + user_email (ALL THREE MANDATORY)",
            "structure": "new_access_control_only"
        }
    }

@app.post("/ingest")
async def ingest_data(request: IngestRequest, background_tasks: BackgroundTasks):
    """Ingest data from enterprise tools"""
    try:
        # Validate services
        valid_services = ['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence', 'all']
        for service in request.services:
            if service not in valid_services:
                raise HTTPException(status_code=400, detail=f"Invalid service: {service}")
        
        # Process 'all' service
        services = request.services
        if 'all' in services:
            services = ['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence']
        
        # Build account mapping
        account_mapping = {}
        if request.account_google_drive:
            account_mapping['google_drive'] = request.account_google_drive
        if request.account_slack:
            account_mapping['slack'] = request.account_slack
        if request.account_dropbox:
            account_mapping['dropbox'] = request.account_dropbox
        if request.account_jira:
            account_mapping['jira'] = request.account_jira
        if request.account_sharepoint:
            account_mapping['sharepoint'] = request.account_sharepoint
        if request.account_confluence:
            account_mapping['confluence'] = request.account_confluence
        
        # Get pipeline instance
        pipeline_instance = get_pipeline(request.empty, request.chunkall)
        
        # Run ingestion in background
        def run_ingestion():
            try:
                pipeline_instance.run_enhanced_ingestion(
                    services=services,
                    account_mapping=account_mapping,
                    external_user_id=request.external_user_id,
                    user_email=request.user_email,
                    limit=request.limit,
                    empty_collections=request.empty,
                    chunk_all=request.chunkall
                )
            except Exception as e:
                print(f"Ingestion error: {e}")
                print(traceback.format_exc())
        
        background_tasks.add_task(run_ingestion)
        
        return {
            "status": "success",
            "message": "Ingestion started in background",
            "services": services,
            "external_user_id": request.external_user_id,
            "user_email": request.user_email,
            "account_mapping": account_mapping
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.post("/search")
async def search_data(request: SearchRequest):
    """Enhanced search with RBAC enforcement"""
    # In search endpoint
    start_time = time.time()
    try:
        # Validate apps if provided
        if request.apps:
            valid_apps = ['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence']
            for app in request.apps:
                if app not in valid_apps:
                    raise HTTPException(status_code=400, detail=f"Invalid app: {app}")
        
        # Validate sort_by
        if request.sort_by not in ['relevance', 'date']:
            raise HTTPException(status_code=400, detail="sort_by must be 'relevance' or 'date'")
        
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Run search
        results = pipeline_instance.run_enhanced_search(
            query=request.query,
            user_email=request.user,
            external_user_id=request.external_user_id,
            account_id=request.account_id,
            apps=request.apps,
            author=request.author,
            file_types=request.file_types,
            from_date=request.from_date,
            to_date=request.to_date,
            sort_by=request.sort_by,
            limit=request.limit,
            include_failed_sync=request.include_failed_sync
        )

        response_time = time.time() - start_time
        utils.analytics.track_search_query(
        query=request.query,
        user_email=request.user,
        account_id=request.account_id,
        external_user_id=request.external_user_id,
        results_count=len(results),
        response_time=response_time,
        integration_types=request.apps,
        success=True
    )
        
        return {
            "status": "success",
            "query": request.query,
            "user": request.user,
            "results": results or [],
            "timestamp": datetime.now().isoformat()
        }
        
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/dashboard")
async def get_dashboard():
    """Show analytics dashboard"""
    try:
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Get dashboard data
        dashboard_data = pipeline_instance.display_dashboard()
        
        return {
            "status": "success",
            "dashboard": dashboard_data,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dashboard failed: {str(e)}")

@app.get("/sync-status")
async def get_sync_status():
    """Check sync status of all documents"""
    try:
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Get sync status
        sync_status = pipeline_instance.run_sync_status_check()
        
        return {
            "status": "success",
            "sync_status": sync_status,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync status check failed: {str(e)}")

@app.post("/empty")
async def empty_collections(request: EmptyRequest):
    """Empty existing collections"""
    try:
        if not request.confirm:
            raise HTTPException(status_code=400, detail="confirm must be True to empty collections")
        
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Empty collections
        result = pipeline_instance.run_empty_collections()
        
        return {
            "status": "success",
            "message": "Collections emptied successfully",
            "result": result,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Empty collections failed: {str(e)}")

@app.post("/fetch-info")
async def fetch_info(request: FetchInfoRequest):
    """Fetch file info for external user ID"""
    try:
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Determine limit
        limit = None if request.all else request.limit
        
        # Fetch info
        info = pipeline_instance.run_fetch_info(request.external_user_id, limit)
        
        return {
            "status": "success",
            "external_user_id": request.external_user_id,
            "info": info,
            "limit": limit,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fetch info failed: {str(e)}")

@app.post("/fetch-by-account")
async def fetch_by_account(request: FetchByAccountRequest):
    """Fetch files by account ID"""
    try:
        # Get pipeline instance
        pipeline_instance = get_pipeline()
        
        # Determine limit
        limit = None if request.all else request.limit
        
        # Fetch by account
        info = pipeline_instance.run_fetch_info(request.account_id, limit)
        
        return {
            "status": "success",
            "account_id": request.account_id,
            "info": info,
            "limit": limit,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fetch by account failed: {str(e)}")

# ==================== NEW ENDPOINTS USING UTILS.PY ====================

@app.post("/fetch-info-rbac")
async def fetch_info_rbac(request: FetchInfoRBACRequest):
    """
    Fetch files using NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email (ALL THREE MANDATORY)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call fetch_info from utils
        result = utils.fetch_info(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            integration_type=request.integration_type,
            limit=request.limit
        )
        
        # Return the result from utils
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fetch info RBAC failed: {str(e)}")

@app.get("/analytics/dashboard")
async def analytics_dashboard():
    """Get comprehensive analytics dashboard"""
    try:
        utils = get_utils()
        full_report = utils.analytics.generate_report()
        return {
            "status": "success",
            "analytics": full_report,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics dashboard failed: {str(e)}")

@app.get("/analytics/search")
async def search_analytics():
    """Get search-specific analytics"""
    try:
        utils = get_utils()
        search_analytics = utils.analytics.get_search_analytics_summary()
        return {
            "status": "success",
            "analytics": search_analytics,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search analytics failed: {str(e)}")

@app.get("/analytics/trending/{limit}")
async def trending_queries(limit: int = 10):
    """Get trending search queries"""
    try:
        utils = get_utils()
        trending = utils.analytics.get_trending_queries(limit)
        return {
            "status": "success",
            "trending_queries": trending,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trending queries failed: {str(e)}")

@app.post("/dynamic-suggestions")
async def dynamic_suggestions(request: DynamicSuggestionsRequest):
    """
    Get dynamic autocomplete suggestions with NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email + partial_query (ALL REQUIRED)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        if not request.partial_query or len(request.partial_query.strip()) < 1:
            raise HTTPException(status_code=400, detail="partial_query is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call get_dynamic_suggestions from utils
        result = utils.get_dynamic_suggestions(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            partial_query=request.partial_query,
            limit=request.limit
        )
        
        # Return the result from utils
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Dynamic suggestions failed: {str(e)}")

@app.post("/sync-status-rbac")
async def sync_status_rbac(request: SyncStatusRBACRequest):
    """
    Get sync status using NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email (ALL THREE MANDATORY)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call get_sync_status from utils
        result = utils.get_sync_status(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            integration_type=request.integration_type
        )
        
        # Return the result from utils
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sync status RBAC failed: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "Enterprise Search Pipeline API",
        "utils_integration": "EnterpriseSearchUtils available",
        "rbac_structure": "new_access_control_only"
    }

@app.get("/debug/env")
async def debug_env():
    """Debug endpoint to check environment variables"""
    env_vars = {
        "TOKEN_ASTRA_DB": "✅ Set" if os.getenv("TOKEN_ASTRA_DB") else "❌ Missing",
        "DATABASE_ID": "✅ Set" if os.getenv("DATABASE_ID") else "❌ Missing",
        "ASTRA_DB_ENDPOINT": "✅ Set" if os.getenv("ASTRA_DB_ENDPOINT") else "❌ Missing",
        "working_directory": os.getcwd(),
        "env_file_exists": os.path.exists(".env"),
        "python_path": sys.path[:3],  # First 3 entries
        "utils_available": True
    }
    
    # Check if .env file exists and show first few lines (without revealing secrets)
    if os.path.exists(".env"):
        try:
            with open(".env", "r") as f:
                lines = f.readlines()[:5]  # First 5 lines only
                env_vars["env_file_preview"] = [line.split("=")[0] + "=***" if "=" in line else line.strip() for line in lines]
        except Exception as e:
            env_vars["env_file_error"] = str(e)
    
    # Test utils initialization
    try:
        utils = get_utils()
        env_vars["utils_status"] = "✅ EnterpriseSearchUtils initialized successfully"
    except Exception as e:
        env_vars["utils_status"] = f"❌ EnterpriseSearchUtils failed: {str(e)}"
    
    return env_vars


# NEW PYDANTIC MODELS FOR ANALYTICS ENDPOINTS

class TrendingSearchesRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    limit: int = Field(10, description="Number of trending searches to return (default: 10)")

class SuggestedDocumentsRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    limit: int = Field(10, description="Number of suggested documents to return (default: 10)")

class RecentSearchesRequest(BaseModel):
    account_id: str = Field(..., description="Account ID (REQUIRED - e.g., 'apn_lmhmG6W')")
    external_user_id: str = Field(..., description="External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')")
    user_email: str = Field(..., description="User email (REQUIRED - e.g., 'user@domain.com')")
    limit: int = Field(10, description="Number of recent searches to return (default: 10)")

# ==================== NEW ANALYTICS ENDPOINTS ====================

@app.post("/trending-searches")
async def trending_searches(request: TrendingSearchesRequest):
    """
    Get trending searches with NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email (ALL THREE MANDATORY)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call get_trending_searches from utils
        result = utils.get_trending_searches(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            limit=request.limit
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trending searches failed: {str(e)}")

@app.post("/suggested-documents")
async def suggested_documents(request: SuggestedDocumentsRequest):
    """
    Get suggested documents with NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email (ALL THREE MANDATORY)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call get_suggested_documents from utils
        result = utils.get_suggested_documents(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            limit=request.limit
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Suggested documents failed: {str(e)}")

@app.post("/recent-searches")
async def recent_searches(request: RecentSearchesRequest):
    """
    Get user's recent searches with NEW access control structure ONLY
    Requires: account_id + external_user_id + user_email (ALL THREE MANDATORY)
    """
    try:
        # Validate required fields
        if not request.account_id:
            raise HTTPException(status_code=400, detail="account_id is REQUIRED")
        if not request.external_user_id:
            raise HTTPException(status_code=400, detail="external_user_id is REQUIRED")
        if not request.user_email or '@' not in request.user_email:
            raise HTTPException(status_code=400, detail="valid user_email is REQUIRED")
        
        # Get utils instance
        utils = get_utils()
        
        # Call get_recent_searches from utils
        result = utils.get_recent_searches(
            account_id=request.account_id,
            external_user_id=request.external_user_id,
            user_email=request.user_email,
            limit=request.limit
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recent searches failed: {str(e)}")


# Error handlers
@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """General exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "message": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    import uvicorn
    
    # Ensure dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Environment variables loaded from .env file")
    except ImportError:
        print("⚠️  python-dotenv not installed. Installing...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "python-dotenv"])
        from dotenv import load_dotenv
        load_dotenv()
    
    # Debug environment
    print(f"🔍 TOKEN_ASTRA_DB: {'✅ Set' if os.getenv('TOKEN_ASTRA_DB') else '❌ Missing'}")
    print(f"📁 Working directory: {os.getcwd()}")
    print(f"📄 .env file exists: {os.path.exists('.env')}")
    
    # Test utils import
    try:
        from utils import EnterpriseSearchUtils
        print("✅ EnterpriseSearchUtils imported successfully")
    except Exception as e:
        print(f"❌ Failed to import EnterpriseSearchUtils: {e}")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)