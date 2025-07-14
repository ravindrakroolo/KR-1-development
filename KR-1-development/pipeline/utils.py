# utils.py - NEW ACCESS CONTROL STRUCTURE ONLY
"""
Enterprise Search Utilities - NEW STRUCTURE ONLY (No Legacy Compatibility)
REQUIRED FIELDS: account_id, external_user_id, user_email (ALL THREE MANDATORY)
"""

import os
import sys
import json
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from collections import Counter
from analytics.analytics_tracker import AnalyticsTracker
from connectors.astra_db_connector import AstraDBManager


class EnterpriseSearchUtils:
    """
    NEW ACCESS CONTROL STRUCTURE ONLY - No legacy compatibility
    ALL functions require account_id + external_user_id + user_email
    """
    
    def __init__(self):
        # Initialize analytics and AstraDB
        self.analytics = AnalyticsTracker()
        self.astra_db = AstraDBManager(self.analytics, empty_collections=False, chunk_all=False)
    
    # ==================== FETCH FILES (NEW STRUCTURE ONLY) ====================
    
    def fetch_info(self, account_id: str, external_user_id: str, user_email: str, 
                   integration_type: str = None, limit: int = None) -> Dict:
        """
        Fetch files using NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')  
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            integration_type: Optional filter ('google_drive', 'slack', etc.)
            limit: Optional limit on results
            
        Returns:
            Dict with file information
        """
        try:
            # STRICT validation - ALL THREE params required
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"🔐 NEW STRUCTURE RBAC:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            if integration_type:
                print(f"   Integration Filter: {integration_type}")
            
            # Build NEW STRUCTURE query - straightforward, no legacy
            base_filter = {
                "access_control.account_id": account_id,
                "access_control.external_user_id": external_user_id,
                "access_control.authorized_emails": {"$in": [user_email_normalized]}
            }
            
            # Add integration filter if provided
            if integration_type:
                base_filter["integration_type"] = integration_type
            
            # Execute query
            cursor = self.astra_db.docs_collection.find(
                base_filter,
                sort={"last_sync_updated": -1},
                limit=limit or 0,
                projection={
                    "title": True, "file_id": True, "id": True, "tool": True,
                    "integration_type": True, "sync_status": True, "last_sync_updated": True,
                    "file_url": True, "web_view_link": True, "metadata": True,
                    "access_control": True
                }
            )
            
            files = list(cursor)
            processed_files = []
            
            print(f"   📊 Found {len(files)} files with NEW structure RBAC")
            
            for doc in files:
                # Use actual URLs from database
                file_url = doc.get("file_url", "")
                web_view_link = doc.get("web_view_link", "")
                
                # Fallback URL construction only if no real URLs exist
                if not file_url and not web_view_link:
                    integration = doc.get("integration_type", "unknown")
                    file_id = doc.get("file_id", "")
                    if integration == "google_drive":
                        file_url = f"https://drive.google.com/file/d/{file_id}/view"
                    elif integration == "slack":
                        file_url = f"https://app.slack.com/client/{file_id}"
                    else:
                        file_url = f"https://yourapp.com/docs/{file_id}"
                
                # Get access control info from NEW structure
                access_control = doc.get("access_control", {})
                
                processed_files.append({
                    "database_id": str(doc.get("_id", "")),
                    "file_id": doc.get("file_id", ""),
                    "document_id": doc.get("id", ""),
                    "filename": doc.get("title", "Untitled"),
                    "source_platform": doc.get("tool", "unknown"),
                    "integration_type": doc.get("integration_type", "unknown"),
                    "sync_status": doc.get("sync_status", False),
                    "last_sync_updated": doc.get("last_sync_updated", ""),
                    "file_url": file_url,
                    "web_view_link": web_view_link or file_url,
                    "file_type": doc.get("metadata", {}).get("type", "unknown"),
                    "content_size": doc.get("metadata", {}).get("content_size", 0),
                    "has_content": doc.get("metadata", {}).get("has_content", False),
                    # NEW structure access info
                    "account_id": access_control.get("account_id", ""),
                    "external_user_id": access_control.get("external_user_id", ""),
                    "author_email": access_control.get("created_by", ""),
                    "users_with_access": access_control.get("authorized_emails", []),
                    "total_users_with_access": len(access_control.get("authorized_emails", [])),
                    "permission_level": access_control.get("permission_level", ""),
                    # Verification info
                    "rbac_structure": "new_access_control_only"
                })
            
            return {
                "status": "success",
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email_normalized,
                "integration_type": integration_type,
                "total_files": len(processed_files),
                "files": processed_files,
                "rbac_structure": "new_access_control_only",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email,
                "files": []
            }
    
    # ==================== DYNAMIC SUGGESTIONS (NEW STRUCTURE ONLY) ====================
    
    def get_dynamic_suggestions(self, account_id: str, external_user_id: str, user_email: str,
                               partial_query: str, limit: int = 10) -> Dict:
        """
        Get dynamic autocomplete suggestions with NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            partial_query: Partial search query for suggestions (REQUIRED)
            limit: Number of suggestions to return (default: 10)
            
        Returns:
            Dict with dynamic suggestions from 6 intelligent sources
        """
        try:
            # STRICT validation - ALL THREE params required + partial_query
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            if not partial_query or len(partial_query.strip()) < 1:
                return {"status": "error", "error": "partial_query is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"💡 NEW STRUCTURE DYNAMIC SUGGESTIONS:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            print(f"   Partial Query: '{partial_query}'")
            print(f"   Limit: {limit}")
            
            # Call the enhanced dynamic suggestions from AstraDBManager
            # But filter results by NEW structure access control
            suggestions_result = self.astra_db.get_dynamic_suggestions(
                partial_query=partial_query,
                user_email=user_email_normalized,
                limit=limit
            )
            
            # Filter suggestions to only include documents accessible via NEW structure
            if suggestions_result.get("data"):
                filtered_suggestions = []
                
                for suggestion in suggestions_result["data"]:
                    # For document-type suggestions, verify NEW structure access
                    if suggestion.get("type") in ["document", "document_title", "content_phrase", "content"]:
                        # Check if user has access via NEW structure
                        if self._verify_document_access_new_structure(
                            suggestion.get("document_title", ""),
                            account_id, external_user_id, user_email_normalized
                        ):
                            suggestion["rbac_verified"] = True
                            suggestion["access_method"] = "new_structure_verified"
                            filtered_suggestions.append(suggestion)
                    else:
                        # Non-document suggestions (trending, recent, autocomplete, spell) - include all
                        suggestion["rbac_verified"] = False
                        suggestion["access_method"] = "non_document_suggestion"
                        filtered_suggestions.append(suggestion)
                
                print(f"   📊 Filtered {len(filtered_suggestions)}/{len(suggestions_result['data'])} suggestions via NEW structure RBAC")
                
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "partial_query": partial_query,
                    "suggestions": filtered_suggestions,
                    "total_suggestions": len(filtered_suggestions),
                    "original_total": len(suggestions_result["data"]),
                    "rbac_structure": "new_access_control_only",
                    "suggestion_sources": [
                        "recent_searches", "trending_searches", "document_titles",
                        "matching_documents", "autocomplete_predictions", "spell_corrections"
                    ],
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            else:
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "partial_query": partial_query,
                    "suggestions": [],
                    "total_suggestions": 0,
                    "rbac_structure": "new_access_control_only",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email,
                "partial_query": partial_query,
                "suggestions": []
            }
    
    # ==================== SYNC STATUS (NEW STRUCTURE ONLY) ====================
    
    def get_sync_status(self, account_id: str, external_user_id: str, user_email: str,
                       integration_type: str = None) -> Dict:
        """
        Get sync status using NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            integration_type: Optional integration filter ('google_drive', 'slack', etc.)
            
        Returns:
            Dict with sync status information
        """
        try:
            # STRICT validation - ALL THREE params required
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"🔐 NEW STRUCTURE SYNC STATUS:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            if integration_type:
                print(f"   Integration Filter: {integration_type}")
            
            # Build NEW STRUCTURE base filter - straightforward, no legacy
            base_filter = {
                "access_control.account_id": account_id,
                "access_control.external_user_id": external_user_id,
                "access_control.authorized_emails": {"$in": [user_email_normalized]}
            }
            
            # Add integration filter if provided
            if integration_type:
                base_filter["integration_type"] = integration_type
            
            # Count total documents
            total_docs = self.astra_db.docs_collection.count_documents(base_filter, upper_bound=10000)
            
            # Count synced documents
            synced_filter = {**base_filter, "sync_status": True}
            synced_docs = self.astra_db.docs_collection.count_documents(synced_filter, upper_bound=10000)
            
            # Count failed documents
            failed_filter = {**base_filter, "sync_status": False}
            failed_docs = self.astra_db.docs_collection.count_documents(failed_filter, upper_bound=10000)
            
            print(f"   📊 NEW structure documents: {total_docs} total, {synced_docs} synced, {failed_docs} failed")
            
            # Get recent failures
            recent_failures = list(self.astra_db.docs_collection.find(
                failed_filter,
                sort={"last_sync_updated": -1},
                limit=5,
                projection={
                    "title": True, "sync_error": True, "last_sync_updated": True, 
                    "integration_type": True, "access_control": True
                }
            ))
            
            # Get breakdown by integration (within NEW structure scope)
            integration_breakdown = {}
            cursor = self.astra_db.docs_collection.find(
                base_filter,
                projection={"integration_type": True, "sync_status": True}
            )
            
            for doc in cursor:
                integration = doc.get("integration_type", "unknown")
                if integration not in integration_breakdown:
                    integration_breakdown[integration] = {
                        "total": 0,
                        "synced": 0,
                        "failed": 0
                    }
                
                integration_breakdown[integration]["total"] += 1
                if doc.get("sync_status"):
                    integration_breakdown[integration]["synced"] += 1
                else:
                    integration_breakdown[integration]["failed"] += 1
            
            return {
                "status": "success",
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email_normalized,
                "integration_type": integration_type,
                "total_documents": total_docs,
                "successfully_synced": synced_docs,
                "sync_failed": failed_docs,
                "sync_success_rate": round((synced_docs / max(1, total_docs)) * 100, 2),
                "recent_failures": recent_failures,
                "integration_breakdown": integration_breakdown,
                "rbac_structure": "new_access_control_only",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email
            }

    # ==================== TRENDING SEARCHES (NEW STRUCTURE ONLY) ====================
    
    def get_trending_searches(self, account_id: str, external_user_id: str, user_email: str,
                             limit: int = 10) -> Dict:
        """
        Get trending searches with NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            limit: Number of trending searches to return (default: 10)
            
        Returns:
            Dict with trending searches filtered by NEW access control
        """
        try:
            # STRICT validation - ALL THREE params required
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"📈 NEW STRUCTURE TRENDING SEARCHES:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            print(f"   Limit: {limit}")
            
            # Get trending searches from AstraDB
            trending_result = self.astra_db.get_trending_searches(limit=limit * 2)  # Get more for filtering
            
            # Filter trending searches to only include those the user can access via NEW structure
            if trending_result.get("data"):
                filtered_trending = []
                
                for trend in trending_result["data"]:
                    # If trending item has a document, verify NEW structure access
                    if trend.get("has_document") and trend.get("document_title"):
                        if self._verify_document_access_new_structure(
                            trend.get("document_title", ""),
                            account_id, external_user_id, user_email_normalized
                        ):
                            trend["rbac_verified"] = True
                            trend["access_method"] = "new_structure_verified"
                            filtered_trending.append(trend)
                    else:
                        # Non-document trending searches - include all
                        trend["rbac_verified"] = False
                        trend["access_method"] = "non_document_trending"
                        filtered_trending.append(trend)
                    
                    if len(filtered_trending) >= limit:
                        break
                
                print(f"   📊 Filtered {len(filtered_trending)}/{len(trending_result['data'])} trending searches via NEW structure RBAC")
                
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "trending_searches": filtered_trending,
                    "total_trending": len(filtered_trending),
                    "original_total": len(trending_result["data"]),
                    "rbac_structure": "new_access_control_only",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            else:
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "trending_searches": [],
                    "total_trending": 0,
                    "rbac_structure": "new_access_control_only",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email,
                "trending_searches": []
            }

    # ==================== SUGGESTED DOCUMENTS (NEW STRUCTURE ONLY) ====================
    
    def get_suggested_documents(self, account_id: str, external_user_id: str, user_email: str,
                               limit: int = 10) -> Dict:
        """
        Get suggested documents with NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            limit: Number of suggested documents to return (default: 10)
            
        Returns:
            Dict with suggested documents filtered by NEW access control
        """
        try:
            # STRICT validation - ALL THREE params required
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"💡 NEW STRUCTURE SUGGESTED DOCUMENTS:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            print(f"   Limit: {limit}")
            
            # Get documents user has access to via NEW structure
            base_filter = {
                "access_control.account_id": account_id,
                "access_control.external_user_id": external_user_id,
                "access_control.authorized_emails": {"$in": [user_email_normalized]},
                "sync_status": True
            }
            
            cursor = self.astra_db.docs_collection.find(
                base_filter,
                sort={"last_sync_updated": -1},
                limit=limit,
                projection={
                    "title": True, "file_id": True, "last_sync_updated": True,
                    "integration_type": True, "file_url": True, "web_view_link": True,
                    "metadata": True, "access_control": True
                }
            )
            
            docs = list(cursor)
            suggested_docs = []
            
            print(f"   📊 Found {len(docs)} suggested documents with NEW structure RBAC")
            
            for doc in docs:
                # Use actual URLs from database
                file_url = doc.get("file_url", "")
                web_view_link = doc.get("web_view_link", "")
                
                # Fallback URL construction only if no real URLs exist
                if not file_url and not web_view_link:
                    integration = doc.get("integration_type", "unknown")
                    file_id = doc.get("file_id", "")
                    if integration == "google_drive":
                        file_url = f"https://drive.google.com/file/d/{file_id}/view"
                    elif integration == "slack":
                        file_url = f"https://app.slack.com/client/{file_id}"
                    else:
                        file_url = f"https://yourapp.com/docs/{file_id}"
                
                # Get access control info
                access_control = doc.get("access_control", {})
                integration = doc.get("integration_type", "unknown").upper()
                file_type = doc.get("metadata", {}).get("type", "file")
                
                # Calculate recency
                last_updated = doc.get("last_sync_updated", "")
                if last_updated:
                    try:
                        updated_dt = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                        days_ago = (datetime.now(timezone.utc) - updated_dt).days
                        if days_ago == 0:
                            recency = "today"
                        elif days_ago == 1:
                            recency = "yesterday"
                        elif days_ago < 7:
                            recency = f"{days_ago} days ago"
                        else:
                            recency = f"{days_ago // 7} weeks ago"
                    except:
                        recency = "recently"
                else:
                    recency = "recently"
                
                suggested_docs.append({
                    "title": doc.get("title", "Untitled"),
                    "link": web_view_link or file_url,
                    "reason": f"Recent {file_type} from {integration} (updated {recency})",
                    "integration": integration,
                    "file_type": file_type,
                    "last_updated": last_updated,
                    "author": access_control.get("created_by", "Unknown"),
                    "account_id": access_control.get("account_id", ""),
                    "external_user_id": access_control.get("external_user_id", ""),
                    "rbac_structure": "new_access_control_only"
                })
            
            return {
                "status": "success",
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email_normalized,
                "suggested_documents": suggested_docs,
                "total_suggested": len(suggested_docs),
                "rbac_structure": "new_access_control_only",
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email,
                "suggested_documents": []
            }

    # ==================== RECENT SEARCHES (NEW STRUCTURE ONLY) ====================
    
    def get_recent_searches(self, account_id: str, external_user_id: str, user_email: str,
                           limit: int = 10) -> Dict:
        """
        Get user's recent searches with NEW access control structure ONLY
        
        Args:
            account_id: Account ID (REQUIRED - e.g., 'apn_lmhmG6W')
            external_user_id: External user ID (REQUIRED - e.g., 'anuj-dwivedi-kroolo')
            user_email: User email (REQUIRED - e.g., 'user@domain.com')
            limit: Number of recent searches to return (default: 10)
            
        Returns:
            Dict with user's recent searches with document links
        """
        try:
            # STRICT validation - ALL THREE params required
            if not account_id:
                return {"status": "error", "error": "account_id is REQUIRED"}
            if not external_user_id:
                return {"status": "error", "error": "external_user_id is REQUIRED"}
            if not user_email or '@' not in user_email:
                return {"status": "error", "error": "valid user_email is REQUIRED"}
            
            # Normalize email for consistent querying
            user_email_normalized = self.astra_db.normalize_email(user_email)
            
            print(f"🕒 NEW STRUCTURE RECENT SEARCHES:")
            print(f"   Account ID: {account_id}")
            print(f"   External User ID: {external_user_id}")
            print(f"   User Email: {user_email_normalized}")
            print(f"   Limit: {limit}")
            
            # Get recent searches from analytics (filtered by user email)
            recent_result = self.astra_db.get_recent_searches(user_email=user_email_normalized, limit=limit)
            
            # Enhance recent searches with NEW structure document access verification
            if recent_result.get("data"):
                enhanced_recent = []
                
                for search in recent_result["data"]:
                    # Verify document access via NEW structure if search has document
                    if search.get("has_document") and search.get("document_title"):
                        if self._verify_document_access_new_structure(
                            search.get("document_title", ""),
                            account_id, external_user_id, user_email_normalized
                        ):
                            search["rbac_verified"] = True
                            search["access_method"] = "new_structure_verified"
                        else:
                            search["rbac_verified"] = False
                            search["access_method"] = "no_access_via_new_structure"
                            # Update link to search page instead of document
                            search["link"] = f"https://yourapp.com/search?q={urllib.parse.quote(search.get('query', ''))}"
                    else:
                        search["rbac_verified"] = False
                        search["access_method"] = "search_query_only"
                    
                    enhanced_recent.append(search)
                
                print(f"   📊 Enhanced {len(enhanced_recent)} recent searches with NEW structure RBAC verification")
                
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "recent_searches": enhanced_recent,
                    "total_recent": len(enhanced_recent),
                    "rbac_structure": "new_access_control_only",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            else:
                return {
                    "status": "success",
                    "account_id": account_id,
                    "external_user_id": external_user_id,
                    "user_email": user_email_normalized,
                    "recent_searches": [],
                    "total_recent": 0,
                    "rbac_structure": "new_access_control_only",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            
        except Exception as e:
            return {
                "status": "error", 
                "error": str(e),
                "account_id": account_id,
                "external_user_id": external_user_id,
                "user_email": user_email,
                "recent_searches": []
            }

    def _verify_document_access_new_structure(self, document_title: str, account_id: str, 
                                            external_user_id: str, user_email: str) -> bool:
        """Verify if user has access to a document via NEW structure"""
        try:
            if not document_title:
                return False
            
            # Quick check if document exists and user has access via NEW structure
            doc = self.astra_db.docs_collection.find_one({
                "title": document_title,
                "access_control.account_id": account_id,
                "access_control.external_user_id": external_user_id,
                "access_control.authorized_emails": {"$in": [user_email]}
            })
            
            return doc is not None
            
        except Exception:
            return False


# ==================== DIRECT PYTHON COMMANDS (NEW STRUCTURE) ====================

def fetch_info_command(account_id: str, external_user_id: str, user_email: str, 
                      integration_type: str = None, limit: int = None):
    """
    Direct command to fetch files with NEW structure RBAC
    
    Usage:
    python -c "from utils import fetch_info_command; fetch_info_command('apn_lmhmG6W', 'anuj-dwivedi-kroolo', 'user@domain.com')"
    """
    utils = EnterpriseSearchUtils()
    result = utils.fetch_info(account_id, external_user_id, user_email, integration_type, limit)
    
    print(f"\n📋 NEW STRUCTURE FETCH RESULTS")
    print("=" * 60)
    print(f"🏢 Account ID: {account_id}")
    print(f"👤 External User: {external_user_id}")
    print(f"📧 User Email: {user_email}")
    if integration_type:
        print(f"🔗 Integration: {integration_type.upper()}")
    print(f"📊 Total Files: {result.get('total_files', 0)}")
    print(f"🆕 Structure: {result.get('rbac_structure', 'unknown')}")
    print()
    
    if result.get('status') == 'error':
        print(f"❌ Error: {result['error']}")
        return result
    
    files = result.get('files', [])
    if not files:
        print("📭 No files found with NEW structure RBAC")
        return result
    
    # Display files
    for i, file_data in enumerate(files, 1):
        sync_indicator = "✅" if file_data.get('sync_status') else "⚠️"
        integration = file_data.get('integration_type', 'unknown').upper()
        
        print(f"{i:3d}. {sync_indicator} {file_data.get('filename', 'Untitled')}")
        print(f"     🔗 {integration}")
        print(f"     📧 Author: {file_data.get('author_email', 'Unknown')}")
        print(f"     👥 Access: {file_data.get('total_users_with_access', 0)} users")
        print(f"     🔐 Permission: {file_data.get('permission_level', 'unknown')}")
        print(f"     🆔 Doc ID: {file_data.get('document_id', 'N/A')}")
        print(f"     🌐 URL: {file_data.get('file_url', 'N/A')}")
        print(f"     🆕 Structure: {file_data.get('rbac_structure', 'unknown')}")
        print()
    
    return result


def dynamic_suggestions_command(account_id: str, external_user_id: str, user_email: str,
                              partial_query: str, limit: int = 10):
    """
    Direct command to get dynamic suggestions with NEW structure RBAC
    
    Usage:
    python -c "from utils import dynamic_suggestions_command; dynamic_suggestions_command('apn_lmhmG6W', 'anuj-dwivedi-kroolo', 'user@domain.com', 'proj')"
    """
    utils = EnterpriseSearchUtils()
    result = utils.get_dynamic_suggestions(account_id, external_user_id, user_email, partial_query, limit)
    
    print(f"\n💡 NEW STRUCTURE DYNAMIC SUGGESTIONS")
    print("=" * 70)
    print(f"🏢 Account ID: {account_id}")
    print(f"👤 External User: {external_user_id}")
    print(f"📧 User Email: {user_email}")
    print(f"🔍 Partial Query: '{partial_query}'")
    print(f"📊 Limit: {limit}")
    print(f"🆕 Structure: {result.get('rbac_structure', 'unknown')}")
    print()
    
    if result.get('status') == 'error':
        print(f"❌ Error: {result['error']}")
        return result
    
    suggestions = result.get('suggestions', [])
    if not suggestions:
        print("💭 No suggestions available")
        print(f"💡 Try different keywords or check spelling for '{partial_query}'")
        return result
    
    print(f"🎯 Found {len(suggestions)} suggestions:")
    if result.get('original_total', 0) > len(suggestions):
        print(f"   (Filtered {result['original_total'] - len(suggestions)} via NEW structure RBAC)")
    print()
    
    # Group suggestions by type for better display
    suggestions_by_type = {}
    for suggestion in suggestions:
        suggestion_type = suggestion.get("type", "unknown")
        if suggestion_type not in suggestions_by_type:
            suggestions_by_type[suggestion_type] = []
        suggestions_by_type[suggestion_type].append(suggestion)
    
    # Display suggestions grouped by type
    type_icons = {
        "recent_search": "🕒",
        "trending": "📈",
        "document_title": "📋",
        "document": "📄",
        "content_phrase": "📝",
        "autocomplete": "🔮",
        "spell_correction": "🔤"
    }
    
    type_names = {
        "recent_search": "Your Recent Searches",
        "trending": "Trending Searches",
        "document_title": "Document Titles",
        "document": "Matching Documents",
        "content_phrase": "Content Phrases",
        "autocomplete": "Autocomplete Predictions",
        "spell_correction": "Spell Corrections"
    }
    
    for suggestion_type, type_suggestions in suggestions_by_type.items():
        icon = type_icons.get(suggestion_type, "💡")
        type_name = type_names.get(suggestion_type, suggestion_type.replace("_", " ").title())
        
        print(f"{icon} **{type_name}:**")
        
        for i, suggestion in enumerate(type_suggestions, 1):
            text = suggestion.get("text", "")
            score = suggestion.get("score", 0)
            source = suggestion.get("source", "")
            link = suggestion.get("link", "")
            rbac_verified = suggestion.get("rbac_verified", False)
            
            print(f"   {i}. **{text}** (score: {score})")
            
            # Show document info if available
            if suggestion.get("document_title"):
                print(f"      📄 Document: {suggestion['document_title']}")
                print(f"      🔗 {suggestion.get('integration', 'UNKNOWN')} | {link}")
                print(f"      🔐 RBAC: {'✅ Verified' if rbac_verified else '➖ N/A'}")
            elif suggestion.get("search_count"):
                print(f"      📊 {suggestion['search_count']} searches")
                print(f"      🌐 {link}")
            else:
                print(f"      🌐 {link}")
            
            print(f"      💭 {source}")
            print(f"      🆕 Access: {suggestion.get('access_method', 'unknown')}")
            print()
        
        print()
    
    return result


def sync_status_command(account_id: str, external_user_id: str, user_email: str,
                       integration_type: str = None):
    """
    Direct command to check sync status with NEW structure RBAC
    
    Usage:
    python -c "from utils import sync_status_command; sync_status_command('apn_lmhmG6W', 'anuj-dwivedi-kroolo', 'user@domain.com')"
    """
    utils = EnterpriseSearchUtils()
    result = utils.get_sync_status(account_id, external_user_id, user_email, integration_type)
    
    print(f"\n🔄 NEW STRUCTURE SYNC STATUS")
    print("=" * 60)
    print(f"🏢 Account ID: {account_id}")
    print(f"👤 External User: {external_user_id}")
    print(f"📧 User Email: {user_email}")
    if integration_type:
        print(f"🔗 Integration: {integration_type.upper()}")
    print(f"🆕 Structure: {result.get('rbac_structure', 'unknown')}")
    print()
    
    if result.get('status') == 'error':
        print(f"❌ Error: {result['error']}")
        return result
    
    print(f"📊 NEW Structure Sync Status:")
    print(f"   Total Documents: {result['total_documents']}")
    print(f"   Successfully Synced: {result['successfully_synced']}")
    print(f"   Sync Failed: {result['sync_failed']}")
    print(f"   Success Rate: {result['sync_success_rate']}%")
    print()
    
    # Show breakdown by integration
    breakdown = result.get('integration_breakdown', {})
    if breakdown:
        print(f"🔗 Integration Breakdown:")
        for integration, stats in breakdown.items():
            success_rate = round((stats['synced'] / max(1, stats['total'])) * 100, 1)
            status_icon = "✅" if success_rate >= 90 else "⚠️" if success_rate >= 70 else "❌"
            
            print(f"   {status_icon} {integration.upper()}")
            print(f"      Total: {stats['total']} | Synced: {stats['synced']} | Failed: {stats['failed']} ({success_rate}%)")
        print()
    
    # Show recent failures
    failures = result.get('recent_failures', [])
    if failures:
        print(f"⚠️ Recent Failures:")
        for failure in failures:
            title = failure.get('title', 'Unknown')[:50]
            integration = failure.get('integration_type', 'unknown').upper()
            access_control = failure.get('access_control', {})
            account = access_control.get('account_id', 'unknown')
            print(f"   • {title} ({integration} - {account})")
        print()
    else:
        print(f"✅ No recent failures found")
        print()
    
    return result


# ==================== MAIN ENTRY POINT (NEW STRUCTURE) ====================

if __name__ == "__main__":
    """
    NEW STRUCTURE usage - ALL THREE parameters required:
    
    python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com
    python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive
    python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive 50
    
    python utils.py sync apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com
    python utils.py sync apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive
    
    python utils.py suggestions apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com "proj"
    python utils.py suggestions apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com "meet" 15
    """
    import sys
    
    if len(sys.argv) < 5:  # command + 3 required RBAC params + query for suggestions
        print("❌ NEW STRUCTURE ERROR: All 3 parameters required")
        print()
        print("Usage:")
        print("  python utils.py fetch <account_id> <external_user_id> <user_email> [integration_type] [limit]")
        print("  python utils.py sync <account_id> <external_user_id> <user_email> [integration_type]")
        print("  python utils.py suggestions <account_id> <external_user_id> <user_email> <partial_query> [limit]")
        print()
        print("Examples:")
        print("  python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com")
        print("  python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive")
        print("  python utils.py fetch apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive 50")
        print("  python utils.py sync apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com")
        print("  python utils.py sync apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com google_drive")
        print("  python utils.py suggestions apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com 'proj'")
        print("  python utils.py suggestions apn_lmhmG6W anuj-dwivedi-kroolo user@domain.com 'meet' 15")
        print()
        print("🆕 NEW ACCESS CONTROL STRUCTURE ONLY")
        print("🔐 REQUIRED: account_id + external_user_id + user_email (ALL THREE)")
        sys.exit(1)
    
    command = sys.argv[1]
    account_id = sys.argv[2]
    external_user_id = sys.argv[3]
    user_email = sys.argv[4]
    
    if command == "fetch":
        integration_type = sys.argv[5] if len(sys.argv) > 5 else None
        limit = int(sys.argv[6]) if len(sys.argv) > 6 and sys.argv[6].isdigit() else None
        
        fetch_info_command(account_id, external_user_id, user_email, integration_type, limit)
        
    elif command == "sync":
        integration_type = sys.argv[5] if len(sys.argv) > 5 else None
        
        sync_status_command(account_id, external_user_id, user_email, integration_type)
        
    elif command == "suggestions":
        if len(sys.argv) < 6:
            print("❌ Error: partial_query is required for suggestions command")
            print("Usage: python utils.py suggestions <account_id> <external_user_id> <user_email> <partial_query> [limit]")
            sys.exit(1)
        
        partial_query = sys.argv[5]
        limit = int(sys.argv[6]) if len(sys.argv) > 6 and sys.argv[6].isdigit() else 10
        
        dynamic_suggestions_command(account_id, external_user_id, user_email, partial_query, limit)
        
    else:
        print(f"❌ Unknown command: {command}")
        print("Available commands: fetch, sync, suggestions")
        print("🆕 NEW ACCESS CONTROL STRUCTURE ONLY")
        print("🔐 All commands require: account_id + external_user_id + user_email")
        sys.exit(1)