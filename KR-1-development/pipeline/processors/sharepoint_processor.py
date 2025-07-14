import time
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class SharePointProcessor:
    """SharePoint connector for documents, lists, and sites"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_lmhm86Z"  # SharePoint account ID
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.chunk_all = chunk_all
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to SharePoint"""
        print(f"🔧 Connecting to SharePoint (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        account = self.pipedream.get_account_by_id(self.account_id)
        if not account:
            return False, f"SharePoint account {self.account_id} not found"
        
        print(f"✅ Connected to SharePoint")
        return True, "Success"
    
    def list_files(self, limit=None) -> List[Dict]:
        """List SharePoint files from document libraries"""
        print(f"📊 Fetching SharePoint files (limit: {limit or 'unlimited'})")
        
        all_files = []
        
        # First get all sites
        sites = self._get_sites()
        
        for site in sites[:5]:  # Limit to first 5 sites to avoid overwhelming
            site_id = site.get('id', '')
            site_name = site.get('displayName', 'Unknown Site')
            
            print(f"  📁 Processing site: {site_name}")
            
            # Get document libraries for this site
            libraries = self._get_document_libraries(site_id)
            
            for library in libraries:
                library_id = library.get('id', '')
                library_name = library.get('displayName', 'Documents')
                
                print(f"    📚 Processing library: {library_name}")
                
                # Get files from this library
                files = self._get_files_from_library(site_id, library_id, limit)
                
                # Add site and library context to each file
                for file in files:
                    file['site_info'] = {
                        'site_id': site_id,
                        'site_name': site_name,
                        'library_id': library_id,
                        'library_name': library_name
                    }
                
                all_files.extend(files)
                
                if limit and len(all_files) >= limit:
                    break
            
            if limit and len(all_files) >= limit:
                break
        
        if limit and len(all_files) > limit:
            all_files = all_files[:limit]
        
        print(f"✅ Retrieved {len(all_files)} files from SharePoint")
        return all_files
    
    def _get_sites(self) -> List[Dict]:
        """Get SharePoint sites"""
        api_url = "https://graph.microsoft.com/v1.0/sites?search=*"
        
        response = self._make_request_with_retry(api_url, "get_sites")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            return data.get('value', [])
        
        return []
    
    def _get_document_libraries(self, site_id: str) -> List[Dict]:
        """Get document libraries for a site"""
        api_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        
        response = self._make_request_with_retry(api_url, f"get_libraries_{site_id}")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            drives = data.get('value', [])
            
            # Filter for document libraries (not personal OneDrive)
            libraries = [drive for drive in drives if drive.get('driveType') == 'documentLibrary']
            return libraries
        
        return []
    
    def _get_files_from_library(self, site_id: str, library_id: str, limit: int = None) -> List[Dict]:
        """Get files from a document library"""
        files = []
        next_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{library_id}/root/children"
        
        while next_url and (not limit or len(files) < limit):
            if limit:
                # Add $top parameter to limit results
                separator = '&' if '?' in next_url else '?'
                next_url += f"{separator}$top={min(50, limit - len(files))}"
            
            response = self._make_request_with_retry(next_url, f"get_files_{library_id}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                items = data.get('value', [])
                
                # Filter for files (not folders)
                file_items = [item for item in items if item.get('file')]
                files.extend(file_items)
                
                # Check for next page
                next_url = data.get('@odata.nextLink')
                
                if not next_url or not file_items:
                    break
                
                time.sleep(0.5)
            else:
                break
        
        return files
    
    def process_file(self, file_data: Dict) -> Tuple[Dict, Dict]:
        """Process SharePoint file"""
        file_id = file_data.get('id', 'Unknown')
        file_name = file_data.get('name', 'Unknown')
        site_info = file_data.get('site_info', {})
        
        processing_info = {
            'has_content': False,
            'has_permissions': False,
            'sync_successful': False,
            'sync_error': None
        }
        
        try:
            # Extract file metadata
            file_info = file_data.get('file', {})
            mime_type = file_info.get('mimeType', '')
            
            # Extract author/creator information
            created_by = file_data.get('createdBy', {}).get('user', {})
            last_modified_by = file_data.get('lastModifiedBy', {}).get('user', {})
            
            author_email = (created_by.get('email') or 
                          last_modified_by.get('email') or 
                          'unknown@domain.com')
            
            # Get file permissions
            user_emails = self._get_file_permissions(site_info.get('site_id'), file_id, author_email)
            if user_emails:
                processing_info['has_permissions'] = True
            
            # Download file content
            content = self._download_file_content(site_info.get('site_id'), file_id, mime_type, file_name)
            if content and len(content.strip()) > 10:
                processing_info['has_content'] = True
            
            # Build comprehensive searchable content
            searchable_content = f"""
File: {file_name}
Site: {site_info.get('site_name', 'Unknown')}
Library: {site_info.get('library_name', 'Documents')}
Type: {mime_type}
Created by: {created_by.get('displayName', 'Unknown')}
Last modified by: {last_modified_by.get('displayName', 'Unknown')}

Content:
{content or 'No extractable content'}
            """.strip()
            
            processing_info['sync_successful'] = True
            
            # Prepare document
            doc_data = {
                "id": f"sharepoint_{file_id}",
                "title": file_name,
                "content": searchable_content,
                "tool": "sharepoint",
                "file_id": file_id,
                "user_emails_with_access": user_emails,
                "size": file_data.get('size'),
                "mime_type": mime_type,
                "created_at": file_data.get('createdDateTime'),
                "updated_at": file_data.get('lastModifiedDateTime'),
                "author": created_by.get('displayName', 'Unknown'),
                "type": self._get_file_type(mime_type),
                
                # Universal fields
                "sync_successful": processing_info['sync_successful'],
                "account_id": self.account_id,
                "external_user_id": self.external_user_id,
                "author_email": author_email.lower() if author_email else "unknown@domain.com",
                "integration_type": "sharepoint",
                
                # SharePoint-specific URLs
                "file_url": file_data.get('webUrl', ''),
                "web_view_link": file_data.get('webUrl', ''),
                
                # Metadata
                "metadata": {
                    "site_id": site_info.get('site_id'),
                    "site_name": site_info.get('site_name'),
                    "library_id": site_info.get('library_id'),
                    "library_name": site_info.get('library_name'),
                    "file_id": file_id,
                    "mime_type": mime_type,
                    "size": file_data.get('size', 0),
                    "created_by": created_by.get('displayName'),
                    "last_modified_by": last_modified_by.get('displayName'),
                    "has_content": processing_info['has_content'],
                    "content_size": len(content) if content else 0
                }
            }
            
            self.analytics.track_content_processing(processing_info)
            return doc_data, processing_info
            
        except Exception as e:
            processing_info['sync_successful'] = False
            processing_info['sync_error'] = str(e)
            print(f"❌ Failed to process file {file_name}: {e}")
            
            # Return minimal doc for failed processing
            doc_data = {
                "id": f"sharepoint_{file_id}",
                "title": f"{file_name} - Failed to Process",
                "content": f"SharePoint file {file_name} - processing failed",
                "tool": "sharepoint",
                "sync_successful": False,
                "sync_error": str(e)
            }
            
            return doc_data, processing_info
    
    def _get_file_permissions(self, site_id: str, file_id: str, author_email: str) -> List[str]:
        """Get file permissions"""
        user_emails = set()
        
        # Add author
        if author_email and '@' in author_email:
            user_emails.add(author_email.lower())
        
        try:
            # Get file permissions
            permissions_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/permissions"
            
            response = self._make_request_with_retry(permissions_url, f"get_permissions_{file_id}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                permissions = data.get('value', [])
                
                for permission in permissions:
                    # Check for user permissions
                    granted_to_v2 = permission.get('grantedToV2', {})
                    if granted_to_v2:
                        user = granted_to_v2.get('user', {})
                        if user and user.get('email'):
                            user_emails.add(user['email'].lower())
                    
                    # Check for legacy granted_to
                    granted_to = permission.get('grantedTo', {})
                    if granted_to:
                        user = granted_to.get('user', {})
                        if user and user.get('email'):
                            user_emails.add(user['email'].lower())
            
        except Exception as e:
            print(f"⚠️ Could not get permissions for file {file_id}: {e}")
        
        # If no permissions found, add some defaults
        if len(user_emails) <= 1:
            user_emails.add("team@domain.com")  # Placeholder
        
        return list(user_emails)
    
    def _download_file_content(self, site_id: str, file_id: str, mime_type: str, file_name: str) -> Optional[str]:
        """Download file content"""
        try:
            # For Microsoft Office files, try to get content via Graph API
            if self._is_office_file(mime_type):
                return self._extract_office_content(site_id, file_id, mime_type, file_name)
            
            # For text files, download directly
            elif mime_type.startswith('text/'):
                download_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content"
                
                response = self._make_request_with_retry(download_url, f"download_content_{file_id}")
                
                if response and response.get('status') == 'success':
                    content = response.get('data', '')
                    if isinstance(content, str):
                        return content
                    return str(content)
            
            # For other files, return metadata
            else:
                return f"SharePoint file: {file_name} ({mime_type})"
                
        except Exception as e:
            print(f"   ❌ Content extraction failed for {file_name}: {e}")
            return f"File: {file_name} (content extraction failed)"
    
    def _is_office_file(self, mime_type: str) -> bool:
        """Check if file is a Microsoft Office file"""
        office_types = [
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # .xlsx
            'application/vnd.openxmlformats-officedocument.presentationml.presentation',  # .pptx
            'application/msword',  # .doc
            'application/vnd.ms-excel',  # .xls
            'application/vnd.ms-powerpoint'  # .ppt
        ]
        return mime_type in office_types
    
    def _extract_office_content(self, site_id: str, file_id: str, mime_type: str, file_name: str) -> str:
        """Extract content from Office files"""
        try:
            print(f"   📄 Processing Office file: {file_name}")
            
            # Try to get content via Graph API search index
            search_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}"
            
            response = self._make_request_with_retry(search_url, f"get_office_content_{file_id}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                
                # Try to get searchable content
                search_content = data.get('content', {})
                if search_content and search_content.get('searchText'):
                    print(f"   ✅ Extracted searchable content: {len(search_content['searchText'])} chars")
                    return search_content['searchText']
                
                # Fall back to file metadata
                description = data.get('description', '')
                if description:
                    return f"Office Document: {file_name}\nDescription: {description}"
            
            # Final fallback
            return f"Microsoft Office Document: {file_name}\nContent: This is a {mime_type} file that may contain rich formatting."
            
        except Exception as e:
            print(f"   ⚠️ Office content extraction failed: {e}")
            return f"Office Document: {file_name} (content extraction limited)"
    
    def _get_file_type(self, mime_type: str) -> str:
        """Get file type from MIME type"""
        if not mime_type:
            return "file"
        
        mime_type = mime_type.lower()
        
        if 'pdf' in mime_type:
            return "pdf"
        elif 'wordprocessingml' in mime_type or 'msword' in mime_type:
            return "docx"
        elif 'presentationml' in mime_type or 'ms-powerpoint' in mime_type:
            return "pptx"
        elif 'spreadsheetml' in mime_type or 'ms-excel' in mime_type:
            return "xlsx"
        elif 'image/' in mime_type:
            return "image"
        elif 'text/' in mime_type:
            return "text"
        else:
            return "file"
    
    def _make_request_with_retry(self, api_url: str, operation: str) -> Optional[Dict]:
        """Make API request with retry"""
        for attempt in range(self.max_retries):
            start_time = time.time()
            try:
                response = self.pipedream.make_api_request(
                    account_id=self.account_id,
                    external_user_id=self.external_user_id,
                    url=api_url,
                    method='GET'
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