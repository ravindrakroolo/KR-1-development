import time
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class ConfluenceProcessor:
    """FIXED Confluence connector with proper API handling"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_V1h7oGa"  # Confluence account ID
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.chunk_all = chunk_all
        self.base_domain = None  # Will be determined from account
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to Confluence and get domain info"""
        print(f"🔧 Connecting to Confluence (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        account = self.pipedream.get_account_by_id(self.account_id)
        if not account:
            return False, f"Confluence account {self.account_id} not found"
        
        # 🔧 FIXED: Get actual domain from account credentials
        credentials = account.get('credentials', {})
        print(f"🔍 Account credentials keys: {list(credentials.keys())}")
        
        # Try to extract domain from various credential fields
        domain_candidates = [
            credentials.get('domain'),
            credentials.get('host'),
            credentials.get('server'),
            credentials.get('baseUrl'),
            credentials.get('instanceUrl'),
            credentials.get('site')
        ]
        
        for candidate in domain_candidates:
            if candidate and 'atlassian.net' in str(candidate):
                self.base_domain = str(candidate).rstrip('/')
                break
        
        if not self.base_domain:
            # Fallback - try to find any URL-like credential
            for key, value in credentials.items():
                if value and 'http' in str(value) and 'atlassian' in str(value):
                    self.base_domain = str(value).rstrip('/')
                    print(f"🔍 Found domain from {key}: {self.base_domain}")
                    break
        
        if not self.base_domain:
            print(f"⚠️ Could not determine Confluence domain, using placeholder")
            self.base_domain = "https://your-company.atlassian.net"
        
        print(f"✅ Connected to Confluence - Domain: {self.base_domain}")
        return True, "Success"
    
    def list_pages(self, limit=None) -> List[Dict]:
        """FIXED List Confluence pages with proper API calls"""
        print(f"📖 Fetching Confluence pages (limit: {limit or 'unlimited'})")
        
        all_pages = []
        
        # 🔧 FIXED: Try multiple API approaches
        
        # Method 1: Try new v2 API
        try:
            print(f"   🔍 Method 1: Trying Confluence REST API v2...")
            pages = self._try_rest_api_v2(limit)
            if pages:
                print(f"   ✅ REST API v2 successful: {len(pages)} pages")
                all_pages.extend(pages)
        except Exception as e:
            print(f"   ⚠️ REST API v2 failed: {e}")
        
        # Method 2: Try v1 API
        try:
            print(f"   🔍 Method 2: Trying Confluence REST API v1...")
            pages = self._try_rest_api_v1(limit)
            if pages:
                print(f"   ✅ REST API v1 successful: {len(pages)} pages")
                all_pages.extend(pages)
        except Exception as e:
            print(f"   ⚠️ REST API v1 failed: {e}")
        
        # Method 3: Try space-based approach
        try:
            print(f"   🔍 Method 3: Trying space-based page discovery...")
            pages = self._try_space_based_discovery(limit)
            if pages:
                print(f"   ✅ Space-based discovery successful: {len(pages)} pages")
                all_pages.extend(pages)
        except Exception as e:
            print(f"   ⚠️ Space-based discovery failed: {e}")
        
        # Remove duplicates by ID
        seen_ids = set()
        unique_pages = []
        for page in all_pages:
            page_id = page.get('id')
            if page_id and page_id not in seen_ids:
                seen_ids.add(page_id)
                unique_pages.append(page)
        
        if limit and len(unique_pages) > limit:
            unique_pages = unique_pages[:limit]
        
        print(f"✅ Retrieved {len(unique_pages)} unique pages")
        return unique_pages
    
    def _try_rest_api_v2(self, limit=None) -> List[Dict]:
        """Try Confluence REST API v2"""
        all_pages = []
        
        # Try pages endpoint
        try:
            api_path = "/wiki/api/v2/pages"
            params = {
                'limit': min(50, limit) if limit else 50,
                'sort': '-modified-date',
                'body-format': 'storage'
            }
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{self.base_domain}{api_path}?{param_string}"
            
            print(f"     🔗 Trying: {api_path}")
            
            response = self._make_request_with_retry(full_url, "pages_v2")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                pages = data.get('results', [])
                
                if pages:
                    print(f"     ✅ Found {len(pages)} pages")
                    all_pages.extend(pages)
        
        except Exception as e:
            print(f"     ⚠️ Pages API v2 failed: {e}")
        
        # Try blog posts endpoint
        try:
            api_path = "/wiki/api/v2/blogposts"
            params = {
                'limit': min(25, (limit // 2)) if limit else 25,
                'sort': '-modified-date',
                'body-format': 'storage'
            }
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{self.base_domain}{api_path}?{param_string}"
            
            print(f"     🔗 Trying: {api_path}")
            
            response = self._make_request_with_retry(full_url, "blogposts_v2")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                posts = data.get('results', [])
                
                if posts:
                    # Mark as blog posts
                    for post in posts:
                        post['content_type'] = 'blogpost'
                    
                    print(f"     ✅ Found {len(posts)} blog posts")
                    all_pages.extend(posts)
        
        except Exception as e:
            print(f"     ⚠️ Blog posts API v2 failed: {e}")
        
        return all_pages
    
    def _try_rest_api_v1(self, limit=None) -> List[Dict]:
        """Try Confluence REST API v1"""
        try:
            api_path = "/wiki/rest/api/content"
            params = {
                'limit': min(50, limit) if limit else 50,
                'orderby': 'lastModified',
                'expand': 'body.storage,version,space'
            }
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{self.base_domain}{api_path}?{param_string}"
            
            print(f"     🔗 Trying: {api_path}")
            
            response = self._make_request_with_retry(full_url, "content_v1")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                results = data.get('results', [])
                
                if results:
                    print(f"     ✅ Found {len(results)} content items")
                    return results
        
        except Exception as e:
            print(f"     ⚠️ Content API v1 failed: {e}")
        
        return []
    
    def _try_space_based_discovery(self, limit=None) -> List[Dict]:
        """Try to discover pages by first getting spaces"""
        all_pages = []
        
        try:
            # First, get list of spaces
            spaces_path = "/wiki/rest/api/space"
            spaces_url = f"{self.base_domain}{spaces_path}?limit=10"
            
            print(f"     🔗 Getting spaces: {spaces_path}")
            
            response = self._make_request_with_retry(spaces_url, "list_spaces")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                spaces = data.get('results', [])
                
                print(f"     📁 Found {len(spaces)} spaces")
                
                # Get pages from each space
                pages_per_space = max(1, (limit // len(spaces))) if limit and spaces else 10
                
                for space in spaces[:3]:  # Limit to first 3 spaces
                    space_key = space.get('key')
                    space_name = space.get('name', 'Unknown')
                    
                    if space_key:
                        print(f"     📖 Getting pages from space: {space_name} ({space_key})")
                        space_pages = self._get_pages_from_space(space_key, pages_per_space)
                        all_pages.extend(space_pages)
                        
                        if limit and len(all_pages) >= limit:
                            break
        
        except Exception as e:
            print(f"     ⚠️ Space-based discovery failed: {e}")
        
        return all_pages
    
    def _get_pages_from_space(self, space_key: str, limit=10) -> List[Dict]:
        """Get pages from a specific space"""
        try:
            api_path = "/wiki/rest/api/content"
            params = {
                'spaceKey': space_key,
                'type': 'page',
                'limit': limit,
                'orderby': 'lastModified',
                'expand': 'body.storage,version'
            }
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{self.base_domain}{api_path}?{param_string}"
            
            response = self._make_request_with_retry(full_url, f"space_pages_{space_key}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                results = data.get('results', [])
                
                if results:
                    print(f"       ✅ Found {len(results)} pages in space {space_key}")
                    return results
        
        except Exception as e:
            print(f"       ⚠️ Failed to get pages from space {space_key}: {e}")
        
        return []
    
    def process_page(self, page_data: Dict) -> Tuple[Dict, Dict]:
        """Process Confluence page or blog post - FIXED"""
        page_id = page_data.get('id', 'Unknown')
        title = page_data.get('title', 'No Title')
        content_type = page_data.get('content_type', page_data.get('type', 'page'))
        
        processing_info = {
            'has_content': False,
            'sync_successful': False,
            'sync_error': None
        }
        
        try:
            # Extract page content (handle multiple formats)
            content = self._extract_page_content(page_data)
            
            if content and len(content.strip()) > 10:
                processing_info['has_content'] = True
            
            # Extract author information (handle multiple API versions)
            author_info = self._extract_author_info(page_data)
            author_email = author_info.get('email', 'unknown@domain.com')
            
            # Extract space information
            space_info = self._extract_space_info(page_data)
            
            # Get page permissions/access
            user_emails = self._extract_page_access(page_data, author_email)
            
            # Build comprehensive searchable content
            searchable_content = f"""
{content_type.title()}: {title}
Space: {space_info.get('name', 'Unknown')}
Author: {author_info.get('display_name', 'Unknown')}

Content:
{content}
            """.strip()
            
            processing_info['sync_successful'] = True
            
            # 🔧 FIXED: Normalize emails properly
            normalized_emails = []
            for email in user_emails:
                if email and '@' in str(email):
                    normalized_emails.append(str(email).lower().strip())
            
            # Ensure we have at least one email
            if not normalized_emails:
                normalized_emails = [author_email.lower().strip() if author_email else "unknown@domain.com"]
            
            # Prepare document
            doc_data = {
                "id": f"confluence_{page_id}",
                "title": title,
                "content": searchable_content,
                "tool": "confluence",
                "file_id": page_id,
                "user_emails_with_access": normalized_emails,  # 🔧 FIXED
                "created_at": page_data.get('createdAt', page_data.get('created')),
                "updated_at": page_data.get('lastModified', page_data.get('updated')),
                "author": author_info.get('display_name', 'Unknown'),
                "type": f"confluence_{content_type}",
                
                # Universal fields
                "sync_successful": processing_info['sync_successful'],
                "account_id": self.account_id,
                "external_user_id": self.external_user_id,
                "author_email": author_email.lower().strip() if author_email else "unknown@domain.com",  # 🔧 FIXED
                "integration_type": "confluence",
                
                # Confluence-specific URLs
                "file_url": f"{self.base_domain}/wiki/spaces/{space_info.get('key', 'UNKNOWN')}/pages/{page_id}",
                "web_view_link": page_data.get('_links', {}).get('webui', f"{self.base_domain}/wiki/spaces/{space_info.get('key', 'UNKNOWN')}/pages/{page_id}"),
                
                # Metadata
                "metadata": {
                    "page_id": page_id,
                    "space_key": space_info.get('key'),
                    "space_name": space_info.get('name'),
                    "content_type": content_type,
                    "author_info": author_info,
                    "has_content": processing_info['has_content']
                }
            }
            
            self.analytics.track_content_processing(processing_info)
            return doc_data, processing_info
            
        except Exception as e:
            processing_info['sync_successful'] = False
            processing_info['sync_error'] = str(e)
            print(f"❌ Failed to process page {page_id}: {e}")
            
            # Return minimal doc for failed processing
            doc_data = {
                "id": f"confluence_{page_id}",
                "title": f"{title} - Failed to Process",
                "content": f"Confluence {content_type} {title} - processing failed",
                "tool": "confluence",
                "sync_successful": False,
                "sync_error": str(e),
                "user_emails_with_access": ["unknown@domain.com"],
                "author_email": "unknown@domain.com",
                "account_id": self.account_id,
                "external_user_id": self.external_user_id,
                "integration_type": "confluence"
            }
            
            return doc_data, processing_info
    
    def _extract_page_content(self, page_data: Dict) -> str:
        """Extract content from page data (handle multiple formats)"""
        content = ""
        
        # Try body.storage (v1 API)
        body = page_data.get('body', {})
        if body:
            storage = body.get('storage', {})
            if storage and storage.get('value'):
                content = self._clean_confluence_storage(storage['value'])
        
        # Try direct body field (v2 API)
        if not content:
            body_content = page_data.get('body', '')
            if isinstance(body_content, str):
                content = body_content
        
        # Try description field
        if not content:
            description = page_data.get('description', '')
            if description:
                content = str(description)
        
        # Fallback to title and metadata
        if not content:
            title = page_data.get('title', 'No Title')
            content = f"Confluence page: {title}"
        
        return content
    
    def _extract_author_info(self, page_data: Dict) -> Dict:
        """Extract author information from page data"""
        author_info = {
            'email': 'unknown@domain.com',
            'display_name': 'Unknown',
            'account_id': None
        }
        
        # Try version.by (v1 API)
        version = page_data.get('version', {})
        if version:
            by = version.get('by', {})
            if by:
                author_info['email'] = by.get('email', by.get('emailAddress', f"{by.get('accountId', 'unknown')}@domain.com"))
                author_info['display_name'] = by.get('displayName', 'Unknown')
                author_info['account_id'] = by.get('accountId')
        
        # Try createdBy (v2 API)
        if author_info['email'] == 'unknown@domain.com':
            created_by = page_data.get('createdBy', {})
            if created_by:
                author_info['email'] = created_by.get('email', f"{created_by.get('accountId', 'unknown')}@domain.com")
                author_info['display_name'] = created_by.get('displayName', 'Unknown')
                author_info['account_id'] = created_by.get('accountId')
        
        return author_info
    
    def _extract_space_info(self, page_data: Dict) -> Dict:
        """Extract space information from page data"""
        space_info = {
            'key': 'UNKNOWN',
            'name': 'Unknown Space'
        }
        
        # Try space field
        space = page_data.get('space', {})
        if space:
            space_info['key'] = space.get('key', 'UNKNOWN')
            space_info['name'] = space.get('name', 'Unknown Space')
        
        # Try spaceId (v2 API)
        if space_info['key'] == 'UNKNOWN':
            space_id = page_data.get('spaceId')
            if space_id:
                space_info['key'] = space_id
                space_info['name'] = f"Space {space_id}"
        
        return space_info
    
    def _extract_page_access(self, page_data: Dict, author_email: str) -> List[str]:
        """Extract page access emails"""
        user_emails = set()
        
        # Add author
        if author_email and '@' in author_email:
            user_emails.add(author_email.lower())
        
        # For now, we'll add some default access since Confluence access is complex
        # In a real implementation, you'd need to query the permissions API
        user_emails.add("team@domain.com")
        
        return list(user_emails)
    
    def _clean_confluence_storage(self, content: str) -> str:
        """Clean Confluence storage format content"""
        if not content:
            return ""
        
        # Remove Confluence-specific macros
        content = re.sub(r'<ac:structured-macro[^>]*>.*?</ac:structured-macro>', '', content, flags=re.DOTALL)
        content = re.sub(r'<ac:image[^>]*>.*?</ac:image>', '[Image]', content, flags=re.DOTALL)
        content = re.sub(r'<ac:link[^>]*>.*?</ac:link>', '[Link]', content, flags=re.DOTALL)
        
        # Clean HTML-like tags
        content = self._clean_html_content(content)
        
        return content
    
    def _clean_html_content(self, content: str) -> str:
        """Clean HTML content to extract text"""
        if not content:
            return ""
        
        # Replace line breaks
        content = re.sub(r'<br[^>]*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<p[^>]*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<div[^>]*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<h[1-6][^>]*>', '\n## ', content, flags=re.IGNORECASE)
        content = re.sub(r'<li[^>]*>', '\n• ', content, flags=re.IGNORECASE)
        
        # Remove all HTML/XML tags
        content = re.sub(r'<[^>]+>', '', content)
        
        # Decode HTML entities
        content = content.replace('&nbsp;', ' ')
        content = content.replace('&amp;', '&')
        content = content.replace('&lt;', '<')
        content = content.replace('&gt;', '>')
        content = content.replace('&quot;', '"')
        content = content.replace('&#39;', "'")
        
        # Clean up whitespace
        content = re.sub(r'\n\s*\n', '\n\n', content)
        content = re.sub(r'[ \t]+', ' ', content)
        
        return content.strip()
    
    def _make_request_with_retry(self, api_url: str, operation: str) -> Optional[Dict]:
        """Make API request with retry using Pipedream proxy"""
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
                    error_msg = response.get('message', 'Unknown error') if response else 'No response'
                    print(f"     ⚠️ API call failed (attempt {attempt + 1}): {error_msg}")
                    
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                        continue
                    return None
                        
            except Exception as e:
                print(f"     ❌ Request exception (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    raise
        return None