"""
Enterprise Search Pipeline - Enhanced Version with Better Search and DOCX Processing

KEY IMPROVEMENTS:
1. Better DOCX processing via Google Drive API (NO LOCAL FILES)
2. More aggressive search - finds results in ALL fields
3. Shows author + access list for each result
4. Never saves files locally
5. Simple but effective improvements
"""

import os
import sys
import argparse
import json
import re
import hashlib
import time
import urllib.parse
import difflib
from datetime import datetime, timezone, timedelta
from pathlib import Path
import requests
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter

# Astra DB imports
from astrapy import DataAPIClient
from dotenv import load_dotenv

# Import from the refactored modules
from EnterpriseSearchPipeline import EnterpriseSearchPipeline

# Load environment variables
load_dotenv()


def main():
    """Enhanced multi-account pipeline with per-service account management"""
    parser = argparse.ArgumentParser(
        description='Enhanced Enterprise Search Pipeline - Multi-Account Support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Multi-service ingestion with account IDs
  python script.py ingest google_drive slack dropbox jira sharepoint confluence \
    --account-google-drive apn_7rhgvdD \
    --account-slack apn_MGh06ly \
    --account-dropbox apn_86hPVdL \
    --account-jira apn_oOhWBlr \
    --account-sharepoint apn_lmhm86Z \
    --account-confluence apn_V1h7oGa \
    --external-user-id kroolo-developer \
    --user-email developer@kroolo.com \
    --limit 5 --empty

  # Enhanced search with account filtering
  python script.py search "project documents" \
    --user developer@kroolo.com \
    --account-id apn_7rhgvdD \
    --external-user-id kroolo-developer
"""
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # INGESTION COMMAND
    ingest_parser = subparsers.add_parser('ingest', help='Ingest data from enterprise tools')
    ingest_parser.add_argument(
        'services',
        nargs='+',
        choices=['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence', 'all'],
        help='Services to process'
    )
    
    # Account-specific parameters
    ingest_parser.add_argument('--account-google-drive', help='Google Drive account ID (e.g., apn_7rhgvdD)')
    ingest_parser.add_argument('--account-slack', help='Slack account ID (e.g., apn_MGh06ly)')
    ingest_parser.add_argument('--account-dropbox', help='Dropbox account ID (e.g., apn_86hPVdL)')
    ingest_parser.add_argument('--account-jira', help='Jira account ID (e.g., apn_oOhWBlr)')
    ingest_parser.add_argument('--account-sharepoint', help='SharePoint account ID (e.g., apn_lmhm86Z)')
    ingest_parser.add_argument('--account-confluence', help='Confluence account ID (e.g., apn_V1h7oGa)')
    
    # User management
    ingest_parser.add_argument('--external-user-id', required=True, help='External user ID (e.g., kroolo-developer)')
    ingest_parser.add_argument('--user-email', required=True, help='User email (e.g., developer@kroolo.com)')
    
    # Processing options
    ingest_parser.add_argument('--limit', type=int, help='Limit number of items per service')
    ingest_parser.add_argument('--empty', action='store_true', help='Empty collections before ingesting')
    ingest_parser.add_argument('--chunkall', action='store_true', help='Store all chunks for large documents')
    
    # SEARCH COMMAND
    search_parser = subparsers.add_parser('search', help='Enhanced search with RBAC enforcement')
    search_parser.add_argument('query', help='Search query')
    search_parser.add_argument('--user', required=True, help='User email (required for access control)')
    search_parser.add_argument('--external-user-id', help='External user ID for filtering')
    search_parser.add_argument('--account-id', help='Specific account ID to filter results')
    
    # Search filters
    search_parser.add_argument('--apps', nargs='+', 
                              choices=['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence'], 
                              help='Filter by integrations')
    search_parser.add_argument('--author', help='Filter by author email')
    search_parser.add_argument('--type', nargs='+', help='Filter by file types')
    search_parser.add_argument('--from-date', help='From date (YYYY-MM-DD)')
    search_parser.add_argument('--to-date', help='To date (YYYY-MM-DD)')
    search_parser.add_argument('--sort', choices=['relevance', 'date'], default='relevance')
    search_parser.add_argument('--limit', type=int, default=10, help='Number of results')
    search_parser.add_argument('--include-failed-sync', action='store_true', help='Include failed sync documents')
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser('dashboard', help='Show analytics dashboard')
    
    # Sync status command
    sync_parser = subparsers.add_parser('sync-status', help='Check sync status of all documents')
    
    # Empty collections command
    empty_parser = subparsers.add_parser('empty', help='Empty existing collections')
    empty_parser.add_argument('--confirm', action='store_true', required=True, 
                             help='Confirm deletion of all documents')
    
    # Fetch file info command
    fetch_parser = subparsers.add_parser('fetch-info', help='Fetch file info for external user ID')
    fetch_parser.add_argument('external_user_id', help='External user ID (e.g., kroolo-developer)')
    fetch_parser.add_argument('--limit', type=int, help='Limit number of files')
    fetch_parser.add_argument('--all', action='store_true', help='Fetch all files (no limit)')
    
    # Fetch by account command
    fetch_by_account_parser = subparsers.add_parser('fetch-by-account', help='Fetch files by account ID')
    fetch_by_account_parser.add_argument('account_id', help='Account ID (e.g., apn_lmhmG6W)')
    fetch_by_account_parser.add_argument('--limit', type=int, help='Limit number of files')
    fetch_by_account_parser.add_argument('--all', action='store_true', help='Fetch all files (no limit)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize pipeline
    empty_collections = getattr(args, 'empty', False)
    chunk_all = getattr(args, 'chunkall', False)
    pipeline = EnterpriseSearchPipeline(empty_collections=empty_collections, chunk_all=chunk_all)
    
    # Print all commands at startup
    pipeline.print_all_commands()
    
    try:
        if args.command == 'ingest':
            # Process services
            services = []
            if 'all' in args.services:
                services = ['google_drive', 'dropbox', 'slack', 'jira', 'sharepoint', 'confluence']
            else:
                services = args.services
            
            # Build account mapping for enhanced ingestion
            account_mapping = {}
            if hasattr(args, 'account_google_drive') and args.account_google_drive:
                account_mapping['google_drive'] = args.account_google_drive
            if hasattr(args, 'account_slack') and args.account_slack:
                account_mapping['slack'] = args.account_slack
            if hasattr(args, 'account_dropbox') and args.account_dropbox:
                account_mapping['dropbox'] = args.account_dropbox
            if hasattr(args, 'account_jira') and args.account_jira:
                account_mapping['jira'] = args.account_jira
            if hasattr(args, 'account_sharepoint') and args.account_sharepoint:
                account_mapping['sharepoint'] = args.account_sharepoint
            if hasattr(args, 'account_confluence') and args.account_confluence:
                account_mapping['confluence'] = args.account_confluence
                
            # Run enhanced ingestion
            pipeline.run_enhanced_ingestion(
                services=services,
                account_mapping=account_mapping,
                external_user_id=args.external_user_id,
                user_email=args.user_email,
                limit=args.limit,
                empty_collections=args.empty,
                chunk_all=args.chunkall
            )
            
        elif args.command == 'search':
            # Build search filters
            filters = {}
            if args.apps:
                filters['apps'] = args.apps
            if args.author:
                filters['author'] = args.author
            if args.type:
                filters['file_types'] = args.type
            if args.from_date:
                filters['from_date'] = args.from_date
            if args.to_date:
                filters['to_date'] = args.to_date
            if args.include_failed_sync:
                filters['include_failed_sync'] = True
            
            # Run enhanced search
            pipeline.run_enhanced_search(
                query=args.query,
                user_email=args.user,
                external_user_id=getattr(args, 'external_user_id', None),
                account_id=getattr(args, 'account_id', None),
                apps=args.apps,
                author=args.author,
                file_types=args.type,
                from_date=args.from_date,
                to_date=args.to_date,
                sort_by=args.sort,
                limit=args.limit,
                include_failed_sync=args.include_failed_sync
            )
            
        elif args.command == 'dashboard':
            # Show analytics dashboard
            pipeline.display_dashboard()
            
        elif args.command == 'sync-status':
            # Check sync status
            pipeline.run_sync_status_check()
            
        elif args.command == 'empty':
            # Empty collections
            if args.confirm:
                pipeline.run_empty_collections()
            else:
                print("❌ --confirm flag is required to empty collections")
                return
        
        elif args.command == 'fetch-info':
            # Fetch comprehensive file info for external user ID
            limit = None if args.all else args.limit
            pipeline.run_fetch_info(args.external_user_id, limit)
        
        elif args.command == 'fetch-by-account':
            # Fetch by account_id
            limit = None if args.all else args.limit
            pipeline.run_fetch_info(args.account_id, limit)
                
    except KeyboardInterrupt:
        print("\n⏹️ Operation cancelled by user")
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")


if __name__ == "__main__":
    main()