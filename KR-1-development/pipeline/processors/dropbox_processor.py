import time
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class DropboxProcessor:
    """Dropbox connector"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_86hPVdL"
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.chunk_all = chunk_all
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to Dropbox"""
        print(f"🔧 Connecting to Dropbox (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        account = self.pipedream.get_account_by_id(self.account_id)
        if not account:
            return False, f"Dropbox account {self.account_id} not found"
        
        print(f"✅ Connected to Dropbox")
        return True, "Success"
    
    def list_files(self, limit=None) -> List[Dict]:
        """List Dropbox files"""
        print(f"📁 Fetching Dropbox files (limit: {limit or 'unlimited'})")
        
        all_files = []
        cursor = None
        page_count = 0
        max_pages = 50 if limit is None else max(1, (limit // 100) + 1)
        
        while page_count < max_pages:
            page_size = 100 if limit is None else min(100, limit - len(all_files))
            if page_size <= 0:
                break
            
            if cursor:
                api_url = "https://api.dropboxapi.com/2/files/list_folder/continue"
                data = {"cursor": cursor}
                response = self._make_request_with_retry(api_url, f"list_files_continue_page_{page_count + 1}", method='POST', data=data)
            else:
                api_url = "https://api.dropboxapi.com/2/files/list_folder"
                data = {
                    "path": "",
                    "recursive": True,
                    "include_media_info": False,
                    "include_deleted": False,
                    "include_has_explicit_shared_members": True,
                    "include_mounted_folders": True,
                    "limit": page_size
                }
                response = self._make_request_with_retry(api_url, f"list_files_page_{page_count + 1}", method='POST', data=data)
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                files = data.get('entries', [])
                
                # Filter out folders, only keep files
                file_entries = [f for f in files if f.get('.tag') == 'file']
                all_files.extend(file_entries)
                
                print(f"📥 Page {page_count + 1}: {len(file_entries)} files")
                
                cursor = data.get('cursor')
                has_more = data.get('has_more', False)
                page_count += 1
                
                if not has_more or not cursor or (limit and len(all_files) >= limit):
                    break
                    
                time.sleep(0.5)
            else:
                break
        
        if limit and len(all_files) > limit:
            all_files = all_files[:limit]
        
        print(f"✅ Retrieved {len(all_files)} files")
        return all_files
    
    def process_file(self, file_data: Dict) -> Tuple[Dict, Dict]:
        """Process Dropbox file"""
        file_id = file_data.get('id')
        file_name = file_data.get('name', 'Unknown')
        file_path = file_data.get('path_display', '')
        
        processing_info = {
            'has_permissions': False,
            'has_content': False,
            'sync_successful': False,
            'sync_error': None
        }
        
        # Get author email from sharing info
        author_email = self._extract_author_email(file_data)
        
        # Get file sharing permissions
        try:
            permissions = self.get_file_permissions(file_id)
            if permissions:
                processing_info['has_permissions'] = True
        except Exception as e:
            permissions = []
            processing_info['sync_error'] = f"Permission extraction failed: {e}"
        
        # Extract user emails
        user_emails = self.extract_user_emails(file_data, permissions)
        if author_email and author_email not in user_emails:
            user_emails.append(author_email)
        
        # Get content for text files and supported formats
        content = None
        should_download = self._should_download_content(file_name)
        
        if should_download:
            try:
                content = self.download_file_content(file_path, file_name)
                if content and len(content.strip()) > 10:
                    processing_info['has_content'] = True
            except Exception as e:
                processing_info['sync_error'] = f"Content extraction failed: {e}"
        
        # Determine sync success
        if author_email and user_emails and (processing_info['has_content'] or processing_info['has_permissions']):
            processing_info['sync_successful'] = True
            processing_info['sync_error'] = None
        else:
            processing_info['sync_successful'] = False
        
        # Build Dropbox URLs
        file_url = f"https://www.dropbox.com/home{file_path}"
        web_view_link = file_url
        
        # Prepare document with Dropbox-specific data
        doc_data = {
            "id": f"dropbox_{file_id}",
            "title": file_name,
            "content": content or "",
            "tool": "dropbox",
            "file_id": file_id,
            "user_emails_with_access": user_emails,
            "size": file_data.get('size', 0),
            "mime_type": self._get_mime_type(file_name),
            "updated_at": file_data.get('server_modified', file_data.get('client_modified')),
            "permissions": permissions,
            "author": author_email,
            "type": self._get_file_type(file_name),
            "has_content": content is not None,
            "content_size": len(content) if content else 0,
            
            # Universal fields
            "sync_successful": processing_info['sync_successful'],
            "account_id": self.account_id,
            "external_user_id": self.external_user_id,
            "author_email": author_email,
            "sync_error": processing_info['sync_error'],
            "integration_type": "dropbox",
            
            # Dropbox URLs
            "file_url": file_url,
            "web_view_link": web_view_link,
            "original_links": {
                "web_view_link": web_view_link,
                "direct_link": f"https://www.dropbox.com/s/direct_link{file_path}",
                "api_link": f"https://api.dropboxapi.com/2/files/get_metadata"
            }
        }
        
        self.analytics.track_content_processing(processing_info)
        return doc_data, processing_info
    
    def _extract_author_email(self, file_data: Dict) -> Optional[str]:
        """Extract author email from file data - for Dropbox this might be limited"""
        # Dropbox doesn't always provide owner email in basic file metadata
        # We'll use a placeholder for now, or try to get from sharing info
        return f"{self.external_user_id}@dropbox.user"
    
    def get_file_permissions(self, file_id: str) -> List[Dict]:
        """Get file sharing permissions"""
        try:
            api_url = "https://api.dropboxapi.com/2/sharing/list_file_members"
            data = {"file": file_id, "include_inherited": True}
            
            response = self._make_request_with_retry(api_url, f"get_permissions_{file_id}", method='POST', data=data)
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                users = data.get('users', [])
                groups = data.get('groups', [])
                
                permissions = []
                for user in users:
                    permissions.append({
                        'type': 'user',
                        'email': user.get('user', {}).get('email'),
                        'role': user.get('access_type', {}).get('.tag', 'viewer'),
                        'display_name': user.get('user', {}).get('display_name')
                    })
                
                for group in groups:
                    permissions.append({
                        'type': 'group',
                        'name': group.get('group', {}).get('group_name'),
                        'role': group.get('access_type', {}).get('.tag', 'viewer')
                    })
                
                return permissions
        except Exception as e:
            print(f"   ⚠️ Failed to get permissions for {file_id}: {e}")
        
        return []
    
    def download_file_content(self, file_path: str, file_name: str) -> Optional[str]:
        """Download file content for supported file types - NO LOCAL SAVING"""
        try:
            # Only download text files and small documents
            if not self._should_download_content(file_name):
                return f"Dropbox file: {file_name} (content not extracted)"
            
            api_url = "https://content.dropboxapi.com/2/files/download"
            data = {"path": file_path}
            
            response = self._make_request_with_retry(api_url, f"download_content_{file_path}", method='POST', data=data)
            
            if response and response.get('status') == 'success':
                content_data = response.get('data')
                if isinstance(content_data, str):
                    return content_data[:50000]  # Limit content size
                return str(content_data)[:50000]
            
            return None
                
        except Exception as e:
            return f"Dropbox file: {file_name} (content extraction failed)"
    
    def _should_download_content(self, file_name: str) -> bool:
        """Determine if file content should be downloaded"""
        if not file_name:
            return False
        
        file_name_lower = file_name.lower()
        
        # Text files
        text_extensions = ['.txt', '.md', '.csv', '.json', '.xml', '.yml', '.yaml']
        if any(file_name_lower.endswith(ext) for ext in text_extensions):
            return True
        
        # Small document files (with size limit checking in actual download)
        doc_extensions = ['.doc', '.docx', '.pdf']
        if any(file_name_lower.endswith(ext) for ext in doc_extensions):
            return True
        
        return False
    
    def _get_mime_type(self, file_name: str) -> str:
        """Get MIME type from file extension"""
        if not file_name:
            return "application/octet-stream"
        
        file_name_lower = file_name.lower()
        
        mime_map = {
            '.txt': 'text/plain',
            '.md': 'text/markdown',
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif'
        }
        
        for ext, mime_type in mime_map.items():
            if file_name_lower.endswith(ext):
                return mime_type
        
        return "application/octet-stream"
    
    def _make_request_with_retry(self, api_url: str, operation: str, method: str = 'GET', data: Dict = None) -> Optional[Dict]:
        """Make API request with retry"""
        for attempt in range(self.max_retries):
            start_time = time.time()
            try:
                response = self.pipedream.make_api_request(
                    account_id=self.account_id,
                    external_user_id=self.external_user_id,
                    url=api_url,
                    method=method,
                    data=data
                )
                
                duration = time.time() - start_time
                
                if response and response.get('status') == 'success':
                    self.analytics.track_pipedream_request(True, duration, is_retry=(attempt > 0))
                    return response
                else:
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                        continue
                    return None
                        
            except Exception as e:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
        return None
    
    def extract_user_emails(self, file_data: Dict, permissions: List[Dict]) -> List[str]:
        """Extract user emails with access"""
        emails = set()
        
        # Add owner email (if available)
        owner_email = self._extract_author_email(file_data)
        if owner_email and '@' in owner_email:
            emails.add(owner_email.lower())
        
        # Add permission emails
        for perm in permissions:
            email = perm.get('email')
            if email and '@' in email and perm.get('type') == 'user':
                emails.add(email.lower())
        
        return list(emails)
    
    def _get_file_type(self, file_name: str) -> str:
        """Get file type from file name"""
        if not file_name:
            return "file"
        
        file_name_lower = file_name.lower()
        
        if file_name_lower.endswith('.pdf'):
            return "pdf"
        elif file_name_lower.endswith(('.doc', '.docx')):
            return "docx"
        elif file_name_lower.endswith(('.ppt', '.pptx')):
            return "pptx"
        elif file_name_lower.endswith(('.xls', '.xlsx')):
            return "xlsx"
        elif file_name_lower.endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp')):
            return "image"
        elif file_name_lower.endswith(('.txt', '.md', '.csv', '.json', '.xml')):
            return "text"
        else:
            return "file"