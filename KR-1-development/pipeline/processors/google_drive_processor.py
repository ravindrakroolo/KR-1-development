import time
import json
import base64
import zipfile
import io
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class GoogleDriveProcessor:
    """Google Drive connector with ENHANCED DOCX processing"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_7rhgvdD"
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.chunk_all = chunk_all
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to Google Drive"""
        print(f"🔧 Connecting to Google Drive (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        account = self.pipedream.get_account_by_id(self.account_id)
        if not account:
            return False, f"Google Drive account {self.account_id} not found"
        
        print(f"✅ Connected to Google Drive")
        return True, "Success"
    
    def list_files(self, limit=None) -> List[Dict]:
        """List Google Drive files"""
        print(f"📁 Fetching Google Drive files (limit: {limit or 'unlimited'})")
        
        all_files = []
        next_page_token = None
        page_count = 0
        max_pages = 50 if limit is None else max(1, (limit // 50) + 1)
        
        while page_count < max_pages:
            page_size = 50 if limit is None else min(50, limit - len(all_files))
            if page_size <= 0:
                break
                
            api_url = "https://www.googleapis.com/drive/v3/files"
            params = {
                'pageSize': page_size,
                'fields': 'nextPageToken,files(id,name,mimeType,size,modifiedTime,parents,owners,permissions,webViewLink)',
                'q': 'trashed=false'
            }
            
            if next_page_token:
                params['pageToken'] = next_page_token
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{api_url}?{param_string}"
            
            response = self._make_request_with_retry(full_url, f"list_files_page_{page_count + 1}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                files = data.get('files', [])
                all_files.extend(files)
                
                print(f"📥 Page {page_count + 1}: {len(files)} files")
                
                next_page_token = data.get('nextPageToken')
                page_count += 1
                
                if not next_page_token or (limit and len(all_files) >= limit):
                    break
                    
                time.sleep(0.5)
            else:
                break
        
        if limit and len(all_files) > limit:
            all_files = all_files[:limit]
        
        print(f"✅ Retrieved {len(all_files)} files")
        return all_files
    
    def process_file(self, file_data: Dict) -> Tuple[Dict, Dict]:
        """Process Google Drive file"""
        file_id = file_data.get('id')
        file_name = file_data.get('name', 'Unknown')
        mime_type = file_data.get('mimeType', '')
        
        processing_info = {
            'has_permissions': False,
            'has_content': False,
            'sync_successful': False,
            'sync_error': None
        }
        
        # Get author email
        author_email = self._extract_author_email(file_data)
        
        # Get permissions
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
        
        # Get content with ENHANCED DOCX processing
        content = None
        should_download = not ('folder' in mime_type.lower())
        
        if should_download:
            try:
                content = self.download_file_content(file_id, mime_type, file_name)
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
        
        # Prepare document with actual URLs from Google Drive API
        doc_data = {
            "id": f"gdrive_{file_id}",
            "title": file_name,
            "content": content or "",
            "tool": "google_drive",
            "file_id": file_id,
            "user_emails_with_access": user_emails,
            "size": file_data.get('size'),
            "mime_type": mime_type,
            "updated_at": file_data.get('modifiedTime'),
            "permissions": permissions,
            "author": self._extract_author(file_data),
            "type": self._get_file_type(mime_type),
            "has_content": content is not None,
            "content_size": len(content) if content else 0,
            
            # Universal fields
            "sync_successful": processing_info['sync_successful'],
            "account_id": self.account_id,
            "external_user_id": self.external_user_id,
            "author_email": author_email,
            "sync_error": processing_info['sync_error'],
            "integration_type": "google_drive",
            
            # ACTUAL URLs from Google Drive API (not constructed)
            "file_url": file_data.get('webViewLink', f"https://drive.google.com/file/d/{file_id}/view"),
            "web_view_link": file_data.get('webViewLink', f"https://drive.google.com/file/d/{file_id}/view"),
            "original_links": {
                "web_view_link": file_data.get('webViewLink', ''),
                "download_link": f"https://drive.google.com/uc?id={file_id}&export=download",
                "api_link": f"https://www.googleapis.com/drive/v3/files/{file_id}"
            }
        }
        
        self.analytics.track_content_processing(processing_info)
        return doc_data, processing_info
    
    def _extract_author_email(self, file_data: Dict) -> Optional[str]:
        """Extract author email from file data"""
        owners = file_data.get('owners', [])
        if owners:
            for owner in owners:
                email = owner.get('emailAddress')
                if email and '@' in email:
                    return email.lower().strip()
        return None
    
    def _is_image_file(self, file_data: Dict) -> bool:
        """Check if file is an image based on MIME type and extension"""
        mime_type = file_data.get('mimeType', '').lower()
        file_name = file_data.get('name', '').lower()
        
        # Check MIME type first
        if mime_type.startswith('image/'):
            return True
        
        # Check file extension as fallback
        image_extensions = {
            '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif',
            '.webp', '.svg', '.ico', '.heic', '.heif', '.raw', '.cr2',
            '.nef', '.arw', '.dng', '.orf', '.rw2'
        }
        
        for ext in image_extensions:
            if file_name.endswith(ext):
                return True
        
        return False
    
    def get_file_permissions(self, file_id: str) -> List[Dict]:
        """Get file permissions"""
        api_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions?fields=permissions(id,type,role,emailAddress,displayName)"
        
        response = self._make_request_with_retry(api_url, f"get_permissions_{file_id}")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            return data.get('permissions', [])
        return []
    
    def download_file_content(self, file_id: str, mime_type: str, file_name: str) -> Optional[str]:
        """ENHANCED download file content with robust DOCX processing - NO LOCAL SAVING"""
        try:
            # ENHANCED GOOGLE DOCS PROCESSING
            if 'google-apps.document' in mime_type:
                print(f"   📄 Processing Google Docs (DOCX): {file_name}")
                return self._extract_google_docs_content(file_id, file_name)
            
            # Google Sheets
            elif 'google-apps.spreadsheet' in mime_type:
                print(f"   📊 Processing Google Sheets: {file_name}")
                export_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=text/csv"
                response = self._make_request_with_retry(export_url, f"download_sheets_{file_id}")
                
                if response and response.get('status') == 'success':
                    content_data = response.get('data')
                    if isinstance(content_data, str) and len(content_data.strip()) > 10:
                        return f"Google Sheets Content:\n{content_data}"
                
                return f"Google Sheets: {file_name}"
            
            # Google Slides
            elif 'google-apps.presentation' in mime_type:
                print(f"   🎨 Processing Google Slides: {file_name}")
                export_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=text/plain"
                response = self._make_request_with_retry(export_url, f"download_slides_{file_id}")
                
                if response and response.get('status') == 'success':
                    content_data = response.get('data')
                    if isinstance(content_data, str) and len(content_data.strip()) > 10:
                        return f"Google Slides Content:\n{content_data}"
                
                return f"Google Slides: {file_name}"
            
            # Regular files
            else:
                print(f"   📁 Processing regular file: {file_name}")
                api_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
                response = self._make_request_with_retry(api_url, f"download_content_{file_id}")
                
                if response and response.get('status') == 'success':
                    content_data = response.get('data')
                    if isinstance(content_data, str):
                        return content_data
                    return str(content_data)
                return None
                
        except Exception as e:
            print(f"   ❌ Content extraction failed for {file_name}: {e}")
            return f"File: {file_name} (content extraction failed)"
    
    def _extract_google_docs_content(self, file_id: str, file_name: str) -> str:
        """ENHANCED Google Docs content extraction with multiple methods - NO LOCAL SAVING"""
        
        # Method 1: Export as plain text (most comprehensive)
        try:
            print(f"   📝 Trying plain text export for {file_name}")
            export_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=text/plain"
            response = self._make_request_with_retry(export_url, f"export_plain_{file_id}")
            
            if response and response.get('status') == 'success':
                content = response.get('data', '')
                if isinstance(content, str) and len(content.strip()) > 50:
                    print(f"   ✅ Plain text export successful: {len(content)} chars")
                    return content
        except Exception as e:
            print(f"   ⚠️ Plain text export failed: {e}")
        
        # Method 2: Export as HTML and strip tags (preserves more structure)
        try:
            print(f"   🌐 Trying HTML export for {file_name}")
            html_export_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=text/html"
            response = self._make_request_with_retry(html_export_url, f"export_html_{file_id}")
            
            if response and response.get('status') == 'success':
                html_content = response.get('data', '')
                if isinstance(html_content, str) and len(html_content.strip()) > 50:
                    # Convert HTML to text
                    text_content = self._html_to_text(html_content)
                    if len(text_content.strip()) > 50:
                        print(f"   ✅ HTML export successful: {len(text_content)} chars")
                        return text_content
        except Exception as e:
            print(f"   ⚠️ HTML export failed: {e}")
        
        # Method 3: Export as RTF and extract text
        try:
            print(f"   📝 Trying RTF export for {file_name}")
            rtf_export_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/export?mimeType=application/rtf"
            response = self._make_request_with_retry(rtf_export_url, f"export_rtf_{file_id}")
            
            if response and response.get('status') == 'success':
                rtf_content = response.get('data', '')
                if isinstance(rtf_content, str) and len(rtf_content.strip()) > 50:
                    # Simple RTF text extraction
                    text_content = self._rtf_to_text(rtf_content)
                    if len(text_content.strip()) > 50:
                        print(f"   ✅ RTF export successful: {len(text_content)} chars")
                        return text_content
        except Exception as e:
            print(f"   ⚠️ RTF export failed: {e}")
        
        # Method 4: Get document metadata as fallback
        try:
            print(f"   📋 Getting document metadata for {file_name}")
            metadata_url = f"https://www.googleapis.com/drive/v3/files/{file_id}?fields=name,description,properties"
            response = self._make_request_with_retry(metadata_url, f"get_metadata_{file_id}")
            
            if response and response.get('status') == 'success':
                metadata = response.get('data', {})
                fallback_content = f"Google Docs Document: {file_name}\n"
                
                if metadata.get('description'):
                    fallback_content += f"Description: {metadata['description']}\n"
                
                properties = metadata.get('properties', {})
                for key, value in properties.items():
                    fallback_content += f"{key}: {value}\n"
                
                fallback_content += f"\nNote: This is a Google Docs document that may contain rich formatting."
                print(f"   📋 Using metadata fallback: {len(fallback_content)} chars")
                return fallback_content
                
        except Exception as e:
            print(f"   ⚠️ Metadata extraction failed: {e}")
        
        # Final fallback
        fallback = f"Google Docs Document: {file_name}\nContent: This Google Docs document is accessible but content extraction was limited. The document is searchable by title."
        print(f"   💭 Using final fallback: {len(fallback)} chars")
        return fallback
    
    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML to text - NO LOCAL SAVING"""
        if not html_content:
            return ""
        
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Replace common HTML elements with text equivalents
        html_content = re.sub(r'<br[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<p[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<div[^>]*>', '\n', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<h[1-6][^>]*>', '\n## ', html_content, flags=re.IGNORECASE)
        html_content = re.sub(r'<li[^>]*>', '\n• ', html_content, flags=re.IGNORECASE)
        
        # Remove all HTML tags
        html_content = re.sub(r'<[^>]+>', '', html_content)
        
        # Decode HTML entities
        html_content = html_content.replace('&nbsp;', ' ')
        html_content = html_content.replace('&amp;', '&')
        html_content = html_content.replace('&lt;', '<')
        html_content = html_content.replace('&gt;', '>')
        html_content = html_content.replace('&quot;', '"')
        html_content = html_content.replace('&#39;', "'")
        
        # Clean up whitespace
        html_content = re.sub(r'\n\s*\n', '\n\n', html_content)
        html_content = re.sub(r'[ \t]+', ' ', html_content)
        
        return html_content.strip()
    
    def _rtf_to_text(self, rtf_content: str) -> str:
        """Simple RTF to text conversion - NO LOCAL SAVING"""
        if not rtf_content:
            return ""
        
        # Remove RTF control codes
        rtf_content = re.sub(r'\\[a-z0-9-]+\s?', '', rtf_content, flags=re.IGNORECASE)
        rtf_content = re.sub(r'\{|\}', '', rtf_content)
        
        # Clean up whitespace
        rtf_content = re.sub(r'\n\s*\n', '\n\n', rtf_content)
        rtf_content = re.sub(r'[ \t]+', ' ', rtf_content)
        
        return rtf_content.strip()
    
    def _get_export_mime_type(self, google_mime_type: str) -> Optional[str]:
        """Get export MIME type for Google Workspace files"""
        export_map = {
            'application/vnd.google-apps.document': 'text/plain',
            'application/vnd.google-apps.spreadsheet': 'text/csv',
            'application/vnd.google-apps.presentation': 'text/plain'
        }
        return export_map.get(google_mime_type)
    
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
    
    def extract_user_emails(self, file_data: Dict, permissions: List[Dict]) -> List[str]:
        """Extract user emails with access"""
        emails = set()
        
        # Add owner emails
        owners = file_data.get('owners', [])
        for owner in owners:
            email = owner.get('emailAddress')
            if email and '@' in email:
                emails.add(email.lower())
        
        # Add permission emails
        for perm in permissions:
            email = perm.get('emailAddress')
            if email and '@' in email and perm.get('type') == 'user':
                emails.add(email.lower())
        
        return list(emails)
    
    def _extract_author(self, file_data: Dict) -> Optional[str]:
        """Extract author from file data"""
        owners = file_data.get('owners', [])
        if owners:
            primary_owner = owners[0]
            return primary_owner.get('emailAddress') or primary_owner.get('displayName')
        return None
    
    def _get_file_type(self, mime_type: str) -> str:
        """Get file type from MIME type"""
        if not mime_type:
            return "file"
        
        mime_type = mime_type.lower()
        
        if 'pdf' in mime_type:
            return "pdf"
        elif 'google-apps.document' in mime_type:
            return "docx"
        elif 'google-apps.presentation' in mime_type:
            return "pptx"
        elif 'google-apps.spreadsheet' in mime_type:
            return "xlsx"
        elif 'image/' in mime_type:
            return "image"
        elif 'text/' in mime_type:
            return "text"
        else:
            return "file"