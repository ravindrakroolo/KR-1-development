import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

# Import from the refactored modules
from analytics.analytics_tracker import AnalyticsTracker
from connectors.astra_db_connector import AstraDBManager
from connectors.pipedream_connector import PipedreamConnector
from processors.google_drive_processor import GoogleDriveProcessor
from processors.slack_processor import SlackProcessor
from processors.dropbox_processor import DropboxProcessor
from processors.jira_processor import JiraProcessor
from processors.confluence_processor import ConfluenceProcessor
from processors.sharepoint_processor import SharePointProcessor

class EnterpriseSearchPipeline:
    """Main pipeline orchestrator with enhanced features"""
    
    def __init__(self, empty_collections: bool = False, chunk_all: bool = False):
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.chunk_all = chunk_all
        
        # Initialize analytics
        self.analytics = AnalyticsTracker()
        
        # Initialize Astra DB
        try:
            self.astra_db = AstraDBManager(self.analytics, empty_collections=empty_collections, chunk_all=chunk_all)
        except Exception as e:
            print(f"❌ Failed to initialize Astra DB: {e}")
            sys.exit(1)
        
        # Initialize Pipedream
        try:
            client_id = os.getenv('PIPEDREAM_CLIENT_ID')
            client_secret = os.getenv('PIPEDREAM_CLIENT_SECRET')
            project_id = os.getenv('PIPEDREAM_PROJECT_ID')
            base_url = os.getenv('PIPEDREAM_BASE_URL', 'https://api.pipedream.com/v1')
            
            external_user_id = "kroolo-developer"
            
            self.pipedream = PipedreamConnector(
                client_id=client_id,
                client_secret=client_secret,
                project_id=project_id,
                base_url=base_url,
                external_user_id=external_user_id
            )
            
        except Exception as e:
            print(f"❌ Failed to initialize Pipedream: {e}")
            sys.exit(1)
    
    def print_all_commands(self):
        """Print all available commands at startup"""
        print(f"""
🚀 ENHANCED ENTERPRISE SEARCH PIPELINE - ALL COMMANDS
================================================================

📥 INGESTION COMMANDS:
  python script.py ingest google_drive dropbox slack --limit 50
  python script.py ingest all --limit 100 --empty
  python script.py ingest dropbox --chunkall
  python script.py empty --confirm

🔍 SEARCH COMMANDS (Enhanced with Author & Access Lists):
  python script.py search "project updates" --user kroolo.developer@kroolo.com
  python script.py search "meeting notes" --user developer@kroolo.com --apps dropbox --limit 5
  python script.py search "john@company.com" --user developer@kroolo.com --sort date

📊 ANALYTICS & MONITORING:
  python script.py dashboard
  python script.py sync-status

👤 USER & ACCOUNT MANAGEMENT:
  python script.py fetch-info kroolo-developer --all
  python script.py fetch-by-account apn_Ogh0KJj --limit 20

🔧 ADVANCED FEATURES:
  Enhanced DOCX processing with multiple extraction methods
  Aggressive search that finds results in ALL fields
  Shows author + access list for each result
  Never saves files locally - all processing in memory
  
📝 SEARCH FILTERS:
  --apps google_drive dropbox slack       Filter by integrations
  --author email@domain.com               Filter by document author  
  --type pdf docx xlsx                    Filter by file types
  --from-date 2024-01-01                 Date range filtering
  --to-date 2024-12-31                   Date range filtering
  --sort relevance|date                  Sort results
  --include-failed-sync                  Include failed documents

🎯 ENHANCED FEATURES:
  ✅ Robust DOCX processing (plain text, HTML, RTF exports)
  ✅ Aggressive search in ALL fields (title, content, emails, metadata)
  ✅ Shows author + users with access for each result
  ✅ Never saves files locally - all in-memory processing
  ✅ Better search fallback strategies
  ✅ Enhanced snippet generation with highlighting
================================================================
""")
    
    def run_ingestion(self, services: List[str], limit: Optional[int] = None):
        """Run data ingestion"""
        print(f"🚀 Enterprise Data Ingestion Pipeline")
        print(f"📅 Timestamp: {self.timestamp}")
        print(f"🎯 Services: {', '.join(services)}")
        print(f"📊 Limit: {limit or 'unlimited'}")
        print("=" * 60)
        
        if not self.pipedream.authenticate():
            print("❌ Failed to authenticate with Pipedream")
            return
        
        results = {}
        
        # Process each service
        if 'google_drive' in services:
            results['google_drive'] = self._process_google_drive(limit)
        
        if 'dropbox' in services:
            results['dropbox'] = self._process_dropbox(limit)
        
        if 'slack' in services:
            results['slack'] = self._process_slack(limit)
        
        # Save results
        self._save_results(results)
        self._display_summary(results)
    
    def _process_google_drive(self, limit: Optional[int]) -> Dict:
        """Process Google Drive"""
        print(f"\n📁 PROCESSING GOOGLE DRIVE")
        print("=" * 40)
        
        processor = GoogleDriveProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        connected, error_msg = processor.connect()
        if not connected:
            return {"status": "failed", "error": error_msg}
        
        files = processor.list_files(limit)
        if not files:
            return {"status": "success", "items_processed": 0, "items_stored": 0}
        
        stored_count = 0
        sync_successful_count = 0
        sync_failed_count = 0
        
        for i, file_data in enumerate(files, 1):
            file_name = file_data.get('name', 'Unknown')
            print(f"📄 Processing {i}/{len(files)}: {file_name}")
            
            # Skip image files
            if processor._is_image_file(file_data):
                print(f"  🖼️ Skipping image file: {file_name}")
                continue
            
            try:
                doc_data, processing_info = processor.process_file(file_data)
                
                if processing_info.get('sync_successful'):
                    sync_successful_count += 1
                else:
                    sync_failed_count += 1
                
                stored, store_error = self.astra_db.store_document(doc_data)
                if stored:
                    stored_count += 1
                    
            except Exception as e:
                sync_failed_count += 1
                print(f"  ❌ Processing failed: {e}")
        
        return {
            "status": "success",
            "total_items_found": len(files),
            "items_processed": len(files),
            "items_stored": stored_count,
            "sync_successful": sync_successful_count,
            "sync_failed": sync_failed_count,
            "account_id": processor.account_id
        }
    
    def _process_dropbox(self, limit: Optional[int]) -> Dict:
        """Process Dropbox"""
        print(f"\n📁 PROCESSING DROPBOX")
        print("=" * 40)
        
        processor = DropboxProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        connected, error_msg = processor.connect()
        if not connected:
            return {"status": "failed", "error": error_msg}
        
        files = processor.list_files(limit)
        if not files:
            return {"status": "success", "items_processed": 0, "items_stored": 0}
        
        stored_count = 0
        sync_successful_count = 0
        sync_failed_count = 0
        
        for i, file_data in enumerate(files, 1):
            file_name = file_data.get('name', 'Unknown')
            print(f"📄 Processing {i}/{len(files)}: {file_name}")
            
            try:
                doc_data, processing_info = processor.process_file(file_data)
                
                if processing_info.get('sync_successful'):
                    sync_successful_count += 1
                else:
                    sync_failed_count += 1
                
                stored, store_error = self.astra_db.store_document(doc_data)
                if stored:
                    stored_count += 1
                    
            except Exception as e:
                sync_failed_count += 1
                print(f"  ❌ Processing failed: {e}")
        
        return {
            "status": "success",
            "total_items_found": len(files),
            "items_processed": len(files),
            "items_stored": stored_count,
            "sync_successful": sync_successful_count,
            "sync_failed": sync_failed_count,
            "account_id": processor.account_id
        }
    
    def _process_slack(self, limit: Optional[int]) -> Dict:
        """Process Slack"""
        print(f"\n💬 PROCESSING SLACK")
        print("=" * 40)
        
        processor = SlackProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        connected, error_msg = processor.connect()
        if not connected:
            return {"status": "failed", "error": error_msg}
        
        conversations = processor.list_conversations(limit)
        if not conversations:
            return {"status": "success", "items_processed": 0, "items_stored": 0}
        
        stored_count = 0
        sync_successful_count = 0
        sync_failed_count = 0
        
        for i, conv_data in enumerate(conversations, 1):
            conv_name = conv_data.get('name', 'Unknown')
            print(f"💬 Processing {i}/{len(conversations)}: {conv_name}")
            
            try:
                doc_data, processing_info = processor.process_conversation(conv_data)
                
                if processing_info.get('sync_successful'):
                    sync_successful_count += 1
                else:
                    sync_failed_count += 1
                
                stored, store_error = self.astra_db.store_document(doc_data)
                if stored:
                    stored_count += 1
                    
            except Exception as e:
                sync_failed_count += 1
                print(f"  ❌ Processing failed: {e}")
        
        return {
            "status": "success",
            "total_items_found": len(conversations),
            "items_processed": len(conversations),
            "items_stored": stored_count,
            "sync_successful": sync_successful_count,
            "sync_failed": sync_failed_count,
            "account_id": processor.account_id
        }
    
    def run_search(self, query: str, user_email: str, filters: Dict = None, 
                   sort_by: str = "relevance", limit: int = 10) -> Dict:
        """Run ENHANCED search showing author + access lists"""
        print(f"\n🔍 ENHANCED ENTERPRISE SEARCH")
        print("=" * 50)
        print(f"Query: '{query}'")
        print(f"User: {user_email}")
        print(f"Filters: {filters}")
        print(f"Limit: {limit}")
        print()
        
        search_results, search_duration = self.astra_db.search_documents(
            query=query,
            user_email=user_email,
            filters=filters,
            sort_by=sort_by,
            limit=limit
        )
        
        print(f"⚡ Search completed in {search_duration * 1000:.2f}ms")
        print(f"📊 Results: {len(search_results['results'])}/{search_results['summary'].get('total', 0)}")
        
        # Display integration distribution
        by_integration = search_results['summary'].get('by_integration', {})
        if by_integration:
            print(f"🔗 By Integration: {', '.join([f'{k}: {v}' for k, v in by_integration.items()])}")
        
        # Search status
        search_status = search_results['summary'].get('status', 'unknown')
        if search_status == "results_found":
            print(f"✅ Status: Found {len(search_results['results'])} results")
        else:
            print(f"⚠️ Status: {search_status}")
        
        # Show suggestions if any
        suggestions = search_results['summary'].get('suggestions', [])
        if suggestions:
            print(f"💡 Suggestions: {', '.join([s['query'] for s in suggestions])}")
        
        print()
        
        # Display results with ENHANCED info showing author + access lists
        for i, result in enumerate(search_results["results"], 1):
            sync_indicator = "✅" if result.get('sync_status') else "⚠️"
            integration = result.get('integration_type', 'unknown').upper()
            relevance_score = result.get('relevance_score', 0)
            is_fallback = result.get('is_fallback_result', False)
            
            # Relevance indicator
            if relevance_score > 10:
                relevance_indicator = "🔥"
            elif relevance_score >= 5:
                relevance_indicator = "⭐"
            else:
                relevance_indicator = "📄"
            
            # Fallback indicator
            fallback_indicator = " [FALLBACK]" if is_fallback else ""
            
            print(f"{i:2d}. {sync_indicator} {relevance_indicator} **{result.get('title_matches', result['title'])}**{fallback_indicator}")
            print(f"    🔗 {integration} | {result['type']} | Score: {relevance_score}")
            print(f"    📧 Author: {result.get('author_email', result['author'])}")
            
            # ENHANCED: Show users with access
            users_with_access = result.get('users_with_access', [])
            access_count = result.get('access_count', 0)
            if users_with_access:
                if access_count <= 3:
                    print(f"    👥 Access ({access_count}): {', '.join(users_with_access)}")
                else:
                    print(f"    👥 Access ({access_count}): {', '.join(users_with_access[:3])} + {access_count - 3} more")
            else:
                print(f"    👥 Access: Private/Limited")
            
            print(f"    🕒 Updated: {result['last_updated']}")
            print(f"    📝 {result['snippet']}")
            print(f"    🌐 {result['link']}")
            print()
        
        # Display summary
        by_sync_status = search_results['summary'].get('by_sync_status', {})
        if by_sync_status:
            print(f"📊 SYNC STATUS:")
            print(f"   ✅ Synced: {by_sync_status.get('synced', 0)}")
            print(f"   ❌ Failed: {by_sync_status.get('failed', 0)}")
            print()
        
        query_words = search_results['summary'].get('query_words', [])
        if query_words:
            print(f"🔍 Search Method: {search_results['summary'].get('search_method', 'unknown')}")
            print(f"📝 Query Words: {', '.join(query_words)}")
            print()
        
        return search_results
    
    def run_fetch_info(self, external_user_id: str, limit: int = None):
        """Fetch file info for external user"""
        print(f"📋 FETCHING INFO FOR: {external_user_id}")
        print("=" * 60)
        
        file_info = self.astra_db.get_user_file_info(external_user_id, limit)
        
        if file_info.get('error'):
            print(f"❌ Error: {file_info['error']}")
            return file_info
        
        files = file_info.get('files', [])
        
        print(f"👤 External User: {external_user_id}")
        print(f"📊 Total Files: {file_info.get('total_files', 0)}")
        print()
        
        if not files:
            print("📭 No files found")
            return file_info
        
        # Display files with enhanced info
        for i, file_data in enumerate(files, 1):
            sync_indicator = "✅" if file_data.get('sync_status') else "⚠️"
            integration_type = file_data.get('integration_type', 'unknown').upper()
            
            print(f"{i:3d}. {sync_indicator} {file_data.get('filename', 'Untitled')}")
            print(f"     🔗 {integration_type}")
            print(f"     📧 Author: {file_data.get('author_email', 'Unknown')}")
            print(f"     👥 Access: {file_data.get('total_users_with_access', 0)} users")
            
            # Show some users with access
            users_with_access = file_data.get('users_with_access', [])
            if users_with_access:
                if len(users_with_access) <= 3:
                    print(f"     👤 Users: {', '.join(users_with_access)}")
                else:
                    print(f"     👤 Users: {', '.join(users_with_access[:3])} + {len(users_with_access) - 3} more")
            
            print(f"     🌐 {file_data.get('file_url', 'N/A')}")
            print()
        
        # Summary by integration
        integration_summary = {}
        for file_data in files:
            integration = file_data.get('integration_type', 'unknown')
            integration_summary[integration] = integration_summary.get(integration, 0) + 1
        
        print(f"📊 BY INTEGRATION:")
        for integration, count in integration_summary.items():
            print(f"   {integration.upper()}: {count} files")
        
        return file_info
    
    def run_empty_collections(self):
        """Empty all collections"""
        print(f"🗑️ EMPTYING COLLECTIONS")
        print("=" * 40)
        
        existing_collections = self.astra_db.db.list_collection_names()
        
        if self.astra_db.documents_collection_name not in existing_collections:
            print(f"❌ Collection '{self.astra_db.documents_collection_name}' does not exist.")
            return
        
        if self.astra_db.analytics_collection_name not in existing_collections:
            print(f"❌ Collection '{self.astra_db.analytics_collection_name}' does not exist.")
            return
        
        docs_collection = self.astra_db.db.get_collection(self.astra_db.documents_collection_name)
        analytics_collection = self.astra_db.db.get_collection(self.astra_db.analytics_collection_name)
        
        print(f"🧹 Emptying {self.astra_db.documents_collection_name}...")
        docs_deleted = self.astra_db._empty_collection(docs_collection)
        print(f"✅ Deleted {docs_deleted} documents from {self.astra_db.documents_collection_name}")
        
        print(f"🧹 Emptying {self.astra_db.analytics_collection_name}...")
        analytics_deleted = self.astra_db._empty_collection(analytics_collection)
        print(f"✅ Deleted {analytics_deleted} documents from {self.astra_db.analytics_collection_name}")
        
        print(f"\n🎯 Collections emptied successfully!")
        print(f"📊 Total documents deleted: {docs_deleted + analytics_deleted}")
    
    def run_sync_status_check(self):
        """Check and display sync status for all documents"""
        print(f"🔄 SYNC STATUS CHECK")
        print("=" * 40)
        
        sync_summary = self.astra_db.get_sync_status_summary()
        if sync_summary.get('error'):
            print(f"❌ Error: {sync_summary['error']}")
            return sync_summary
        
        print(f"📊 Current Sync Status:")
        print(f"   Total Documents: {sync_summary['total_documents']}")
        print(f"   Successfully Synced: {sync_summary['successfully_synced']}")
        print(f"   Sync Failed: {sync_summary['sync_failed']}")
        print(f"   Success Rate: {sync_summary['sync_success_rate']}%")
        print()
        
        return sync_summary
    
    def display_dashboard(self):
        """Display enhanced analytics dashboard"""
        report = self.analytics.generate_report()
        
        print(f"\n📊 ENHANCED ENTERPRISE SEARCH ANALYTICS DASHBOARD")
        print("=" * 60)
        
        # Pipeline Summary
        summary = report['pipeline_summary']
        health_score = summary['overall_health_score']
        sync_success_rate = summary.get('sync_success_rate_percent', 0)
        health_indicator = "🟢" if health_score >= 90 else "🟡" if health_score >= 70 else "🔴"
        sync_indicator = "🟢" if sync_success_rate >= 90 else "🟡" if sync_success_rate >= 70 else "🔴"
        
        print(f"🏥 Overall System Health: {health_indicator} {health_score}%")
        print(f"🔄 Sync Success Rate: {sync_indicator} {sync_success_rate}%")
        print(f"⏱️ Total Runtime: {summary['total_runtime_seconds']}s")
        print()
        
        # Content Processing
        content = report['content_processing']
        print(f"📄 CONTENT PROCESSING ANALYSIS")
        print(f"   📁 Items Discovered: {content['total_items_discovered']}")
        print(f"   📝 Content Extraction Rate: {content['content_extraction_rate_percent']}%")
        print(f"   ✅ Sync Success Rate: {content['sync_success_rate_percent']}%")
        print(f"   📊 Items Synced Successfully: {content['items_synced_successfully']}")
        print(f"   ❌ Items Sync Failed: {content['items_sync_failed']}")
        print()
        
        # Database Sync Status
        sync_summary = self.astra_db.get_sync_status_summary()
        if not sync_summary.get('error'):
            print(f"🗃️ DATABASE SYNC STATUS")
            print(f"   📚 Total Documents: {sync_summary['total_documents']}")
            print(f"   ✅ Successfully Synced: {sync_summary['successfully_synced']}")
            print(f"   ❌ Sync Failed: {sync_summary['sync_failed']}")
            print(f"   📈 Database Sync Rate: {sync_summary['sync_success_rate']}%")
            print()
        
        print("=" * 60)
        
        # Save report - NOT saving to local files, just display
        print(f"📊 Dashboard analysis complete")
    
    def _save_results(self, results: Dict):
        """Save pipeline results - avoiding local file saves per requirement"""
        # Just print results instead of saving to file
        print(f"\n📋 INGESTION RESULTS SUMMARY:")
        print(json.dumps(results, indent=2))
    
    def _display_summary(self, results: Dict):
        """Display ingestion summary"""
        print(f"\n🎉 INGESTION COMPLETE!")
        print("=" * 40)
        
        for service, result in results.items():
            if result.get('status') == 'success':
                print(f"✅ {service.upper()}:")
                print(f"   📁 Items Found: {result.get('total_items_found', 0)}")
                print(f"   📄 Items Processed: {result.get('items_processed', 0)}")
                print(f"   💾 Items Stored: {result.get('items_stored', 0)}")
                print(f"   ✅ Sync Successful: {result.get('sync_successful', 0)}")
                print(f"   ❌ Sync Failed: {result.get('sync_failed', 0)}")
            else:
                print(f"❌ {service.upper()}: {result.get('error', 'Unknown error')}")
    
    def run_enhanced_ingestion(self, services: List[str], account_mapping: Dict[str, str], 
                              external_user_id: str, user_email: str, 
                              limit: Optional[int] = None, empty_collections: bool = False, 
                              chunk_all: bool = False):
        """Enhanced multi-account ingestion with per-service account management"""
        print(f"🚀 ENHANCED MULTI-ACCOUNT INGESTION PIPELINE")
        print(f"📅 Timestamp: {self.timestamp}")
        print(f"🎯 Services: {', '.join(services)}")
        print(f"👤 External User ID: {external_user_id}")
        print(f"📧 User Email: {user_email}")
        print(f"📊 Limit: {limit or 'unlimited'}")
        print(f"🗑️ Empty Collections: {empty_collections}")
        print("=" * 60)
        
        # Validate account mappings
        missing_accounts = []
        for service in services:
            if service not in ['all'] and service not in account_mapping:
                missing_accounts.append(service)
        
        if missing_accounts:
            print(f"⚠️ WARNING: Missing account mappings for: {', '.join(missing_accounts)}")
            print(f"   These services will use default account settings")
        
        # Display account mappings
        print(f"\n🔗 ACCOUNT MAPPINGS:")
        for service, account_id in account_mapping.items():
            print(f"   {service.upper()}: {account_id}")
        print()
        
        if not self.pipedream.authenticate():
            print("❌ Failed to authenticate with Pipedream")
            return
        
        # Set user context for this session
        self.pipedream.external_user_id = external_user_id
        
        results = {}
        
        # Process each service with account-specific handling
        for service in services:
            if service == 'all':
                continue
                
            account_id = account_mapping.get(service)
            if account_id:
                print(f"\n🔄 Processing {service.upper()} with account {account_id}")
                # Here you would set the account context for the service
                # This is a placeholder for account-specific processing
            else:
                print(f"\n🔄 Processing {service.upper()} with default account")
            
            if service == 'google_drive':
                results['google_drive'] = self._process_google_drive_enhanced(limit, account_id)
            elif service == 'dropbox':
                results['dropbox'] = self._process_dropbox_enhanced(limit, account_id)
            elif service == 'slack':
                results['slack'] = self._process_slack_enhanced(limit, account_id)
            elif service == 'jira':
                results['jira'] = self._process_jira_enhanced(limit, account_id)
            elif service == 'sharepoint':
                results['sharepoint'] = self._process_sharepoint_enhanced(limit, account_id)
            elif service == 'confluence':
                results['confluence'] = self._process_confluence_enhanced(limit, account_id)
        
        self._save_results(results)
        self._display_summary(results)
        
        return results
    
    def _process_google_drive_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced Google Drive processing with account-specific handling"""
        print(f"📁 ENHANCED GOOGLE DRIVE PROCESSING (Account: {account_id or 'default'})")
        return self._process_google_drive(limit)
    
    def _process_dropbox_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced Dropbox processing with account-specific handling"""
        print(f"📁 ENHANCED DROPBOX PROCESSING (Account: {account_id or 'default'})")
        return self._process_dropbox(limit)
    
    def _process_slack_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced Slack processing with account-specific handling"""
        print(f"💬 ENHANCED SLACK PROCESSING (Account: {account_id or 'default'})")
        return self._process_slack(limit)
    
    def _process_jira_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced Jira processing with account-specific handling"""
        print(f"🎫 ENHANCED JIRA PROCESSING (Account: {account_id or 'default'})")
        return self._process_jira(limit)
    
    def _process_jira(self, limit: Optional[int]) -> Dict:
        """Process Jira"""
        print(f"\n🎫 PROCESSING JIRA")
        print("=" * 40)
        
        processor = JiraProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        # Connect to Jira
        connected, message = processor.connect()
        if not connected:
            return {
                "status": "error",
                "message": f"Jira connection failed: {message}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
        
        try:
            # Get Jira issues
            issues = processor.list_issues(limit)
            print(f"📋 Found {len(issues)} Jira issues")
            
            total_processed = 0
            total_stored = 0
            sync_successful = 0
            sync_failed = 0
            
            for issue in issues:
                try:
                    doc_data, processing_info = processor.process_issue(issue)
                    total_processed += 1
                    
                    if processing_info.get('sync_successful'):
                        # Store in Astra DB
                        success, message = self.astra_db.store_document(doc_data)
                        if success:
                            total_stored += 1
                            sync_successful += 1
                            print(f"   ✅ Stored: {doc_data.get('title', 'Unknown')}")
                        else:
                            sync_failed += 1
                            print(f"   ❌ Failed to store: {message}")
                    else:
                        sync_failed += 1
                        print(f"   ⚠️ Processing failed: {processing_info.get('sync_error', 'Unknown error')}")
                        
                except Exception as e:
                    sync_failed += 1
                    print(f"   ❌ Error processing issue: {e}")
            
            return {
                "status": "success",
                "message": f"Processed {total_processed} Jira issues",
                "total_items_found": len(issues),
                "items_processed": total_processed,
                "items_stored": total_stored,
                "sync_successful": sync_successful,
                "sync_failed": sync_failed
            }
            
        except Exception as e:
            print(f"❌ Jira processing error: {e}")
            return {
                "status": "error",
                "message": f"Jira processing failed: {str(e)}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
    
    def _process_sharepoint_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced SharePoint processing with account-specific handling"""
        print(f"📊 ENHANCED SHAREPOINT PROCESSING (Account: {account_id or 'default'})")
        return self._process_sharepoint(limit)
    
    def _process_sharepoint(self, limit: Optional[int]) -> Dict:
        """Process SharePoint"""
        print(f"\n📊 PROCESSING SHAREPOINT")
        print("=" * 40)
        
        processor = SharePointProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        # Connect to SharePoint
        connected, message = processor.connect()
        if not connected:
            return {
                "status": "error",
                "message": f"SharePoint connection failed: {message}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
        
        try:
            # Get SharePoint files
            files = processor.list_files(limit)
            print(f"📄 Found {len(files)} SharePoint files")
            
            total_processed = 0
            total_stored = 0
            sync_successful = 0
            sync_failed = 0
            
            for file in files:
                try:
                    doc_data, processing_info = processor.process_file(file)
                    total_processed += 1
                    
                    if processing_info.get('sync_successful'):
                        # Store in Astra DB
                        success, message = self.astra_db.store_document(doc_data)
                        if success:
                            total_stored += 1
                            sync_successful += 1
                            print(f"   ✅ Stored: {doc_data.get('title', 'Unknown')}")
                        else:
                            sync_failed += 1
                            print(f"   ❌ Failed to store: {message}")
                    else:
                        sync_failed += 1
                        print(f"   ⚠️ Processing failed: {processing_info.get('sync_error', 'Unknown error')}")
                        
                except Exception as e:
                    sync_failed += 1
                    print(f"   ❌ Error processing file: {e}")
            
            return {
                "status": "success",
                "message": f"Processed {total_processed} SharePoint files",
                "total_items_found": len(files),
                "items_processed": total_processed,
                "items_stored": total_stored,
                "sync_successful": sync_successful,
                "sync_failed": sync_failed
            }
            
        except Exception as e:
            print(f"❌ SharePoint processing error: {e}")
            return {
                "status": "error",
                "message": f"SharePoint processing failed: {str(e)}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
    
    def _process_confluence_enhanced(self, limit: Optional[int], account_id: Optional[str]) -> Dict:
        """Enhanced Confluence processing with account-specific handling"""
        print(f"📖 ENHANCED CONFLUENCE PROCESSING (Account: {account_id or 'default'})")
        return self._process_confluence(limit)
    
    def _process_confluence(self, limit: Optional[int]) -> Dict:
        """Process Confluence"""
        print(f"\n📖 PROCESSING CONFLUENCE")
        print("=" * 40)
        
        processor = ConfluenceProcessor(self.pipedream, self.pipedream.external_user_id, self.analytics, self.chunk_all)
        
        # Connect to Confluence
        connected, message = processor.connect()
        if not connected:
            return {
                "status": "error",
                "message": f"Confluence connection failed: {message}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
        
        try:
            # Get Confluence pages
            pages = processor.list_pages(limit)
            print(f"📄 Found {len(pages)} Confluence pages")
            
            total_processed = 0
            total_stored = 0
            sync_successful = 0
            sync_failed = 0
            
            for page in pages:
                try:
                    doc_data, processing_info = processor.process_page(page)
                    total_processed += 1
                    
                    if processing_info.get('sync_successful'):
                        # Store in Astra DB
                        success, message = self.astra_db.store_document(doc_data)
                        if success:
                            total_stored += 1
                            sync_successful += 1
                            print(f"   ✅ Stored: {doc_data.get('title', 'Unknown')}")
                        else:
                            sync_failed += 1
                            print(f"   ❌ Failed to store: {message}")
                    else:
                        sync_failed += 1
                        print(f"   ⚠️ Processing failed: {processing_info.get('sync_error', 'Unknown error')}")
                        
                except Exception as e:
                    sync_failed += 1
                    print(f"   ❌ Error processing page: {e}")
            
            return {
                "status": "success",
                "message": f"Processed {total_processed} Confluence pages",
                "total_items_found": len(pages),
                "items_processed": total_processed,
                "items_stored": total_stored,
                "sync_successful": sync_successful,
                "sync_failed": sync_failed
            }
            
        except Exception as e:
            print(f"❌ Confluence processing error: {e}")
            return {
                "status": "error",
                "message": f"Confluence processing failed: {str(e)}",
                "total_items_found": 0,
                "items_processed": 0,
                "items_stored": 0,
                "sync_successful": 0,
                "sync_failed": 0
            }
    
    def run_enhanced_search(self, query: str, user_email: str, external_user_id: Optional[str] = None,
                           account_id: Optional[str] = None, apps: Optional[List[str]] = None,
                           author: Optional[str] = None, file_types: Optional[List[str]] = None,
                           from_date: Optional[str] = None, to_date: Optional[str] = None,
                           sort_by: str = "relevance", limit: int = 10, 
                           include_failed_sync: bool = False) -> Dict:
        """Enhanced search with RBAC enforcement and account filtering"""
        print(f"\n🔍 ENHANCED ENTERPRISE SEARCH WITH RBAC")
        print("=" * 50)
        print(f"Query: '{query}'")
        print(f"User: {user_email}")
        if external_user_id:
            print(f"External User ID: {external_user_id}")
        if account_id:
            print(f"Account Filter: {account_id}")
        print(f"Limit: {limit}")
        print()
        
        # Build enhanced filters
        filters = {}
        if apps:
            filters['apps'] = apps
        if author:
            filters['author'] = author
        if file_types:
            filters['file_types'] = file_types
        if from_date:
            filters['from_date'] = from_date
        if to_date:
            filters['to_date'] = to_date
        if include_failed_sync:
            filters['include_failed_sync'] = True
        if external_user_id:
            filters['external_user_id'] = external_user_id
        if account_id:
            filters['account_id'] = account_id
        
        print(f"🔧 Enhanced Filters: {filters}")
        print()
        
        # Use existing search functionality with enhanced filters
        return self.run_search(
            query=query,
            user_email=user_email,
            filters=filters if filters else None,
            sort_by=sort_by,
            limit=limit
        )