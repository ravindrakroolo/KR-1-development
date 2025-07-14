import time
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

class JiraProcessor:
    """FIXED Jira connector with proper API handling"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_oOhWBlr"  # Jira account ID
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.chunk_all = chunk_all
        self.base_domain = None  # Will be determined from account
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to Jira and get domain info"""
        print(f"🔧 Connecting to Jira (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        account = self.pipedream.get_account_by_id(self.account_id)
        if not account:
            return False, f"Jira account {self.account_id} not found"
        
        # 🔧 FIXED: Get actual domain from account credentials
        credentials = account.get('credentials', {})
        print(f"🔍 Account credentials keys: {list(credentials.keys())}")
        
        # Try to extract domain from various credential fields
        domain_candidates = [
            credentials.get('domain'),
            credentials.get('host'),
            credentials.get('server'),
            credentials.get('baseUrl'),
            credentials.get('instanceUrl')
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
            print(f"⚠️ Could not determine Jira domain, using placeholder")
            self.base_domain = "https://your-company.atlassian.net"
        
        print(f"✅ Connected to Jira - Domain: {self.base_domain}")
        return True, "Success"
    
    def list_issues(self, limit=None) -> List[Dict]:
        """FIXED List Jira issues with proper API calls"""
        print(f"🎫 Fetching Jira issues (limit: {limit or 'unlimited'})")
        
        all_issues = []
        start_at = 0
        max_results = 50
        
        # 🔧 FIXED: Try multiple API approaches
        
        # Method 1: Try using Pipedream proxy with REST API v3
        try:
            print(f"   🔍 Method 1: Trying Jira REST API v3...")
            issues = self._try_rest_api_v3(limit)
            if issues:
                print(f"   ✅ REST API v3 successful: {len(issues)} issues")
                return issues
        except Exception as e:
            print(f"   ⚠️ REST API v3 failed: {e}")
        
        # Method 2: Try REST API v2
        try:
            print(f"   🔍 Method 2: Trying Jira REST API v2...")
            issues = self._try_rest_api_v2(limit)
            if issues:
                print(f"   ✅ REST API v2 successful: {len(issues)} issues")
                return issues
        except Exception as e:
            print(f"   ⚠️ REST API v2 failed: {e}")
        
        # Method 3: Try simple search
        try:
            print(f"   🔍 Method 3: Trying simple issue search...")
            issues = self._try_simple_search(limit)
            if issues:
                print(f"   ✅ Simple search successful: {len(issues)} issues")
                return issues
        except Exception as e:
            print(f"   ⚠️ Simple search failed: {e}")
        
        print(f"❌ All methods failed - no Jira issues found")
        return []
    
    def _try_rest_api_v3(self, limit=None) -> List[Dict]:
        """Try Jira REST API v3"""
        api_path = "/rest/api/3/search"
        
        # Simple JQL to get recent issues
        jql_queries = [
            "ORDER BY updated DESC",
            "project is not EMPTY ORDER BY updated DESC",
            "updated >= -30d ORDER BY updated DESC",
            ""  # Empty JQL as last resort
        ]
        
        for jql in jql_queries:
            try:
                params = {
                    'jql': jql,
                    'startAt': 0,
                    'maxResults': min(50, limit) if limit else 50,
                    'fields': 'summary,description,creator,reporter,assignee,status,priority,issuetype,created,updated,comment'
                }
                
                param_string = '&'.join([f"{k}={v}" for k, v in params.items() if v])
                full_url = f"{self.base_domain}{api_path}?{param_string}"
                
                print(f"     🔗 Trying JQL: '{jql}' at {api_path}")
                
                response = self._make_request_with_retry(full_url, f"search_v3_{jql[:20]}")
                
                if response and response.get('status') == 'success':
                    data = response.get('data', {})
                    issues = data.get('issues', [])
                    
                    if issues:
                        print(f"     ✅ Found {len(issues)} issues with JQL: '{jql}'")
                        return issues[:limit] if limit else issues
                
            except Exception as e:
                print(f"     ⚠️ JQL '{jql}' failed: {e}")
                continue
        
        return []
    
    def _try_rest_api_v2(self, limit=None) -> List[Dict]:
        """Try Jira REST API v2"""
        api_path = "/rest/api/2/search"
        
        params = {
            'jql': 'ORDER BY updated DESC',
            'startAt': 0,
            'maxResults': min(50, limit) if limit else 50,
            'fields': 'summary,description,creator,reporter,assignee,status,priority,issuetype,created,updated'
        }
        
        param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        full_url = f"{self.base_domain}{api_path}?{param_string}"
        
        print(f"     🔗 Trying: {api_path}")
        
        response = self._make_request_with_retry(full_url, "search_v2")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            issues = data.get('issues', [])
            
            if issues:
                return issues[:limit] if limit else issues
        
        return []
    
    def _try_simple_search(self, limit=None) -> List[Dict]:
        """Try simple project listing first, then issues"""
        
        # First, try to get projects to see if we have any access
        try:
            projects_path = "/rest/api/2/project"
            projects_url = f"{self.base_domain}{projects_path}"
            
            print(f"     🔗 Checking projects at: {projects_path}")
            
            response = self._make_request_with_retry(projects_url, "list_projects")
            
            if response and response.get('status') == 'success':
                projects = response.get('data', [])
                print(f"     📁 Found {len(projects) if isinstance(projects, list) else 'some'} projects")
                
                if isinstance(projects, list) and projects:
                    # Try to get issues from the first project
                    first_project = projects[0]
                    project_key = first_project.get('key', first_project.get('id', ''))
                    
                    if project_key:
                        return self._get_issues_from_project(project_key, limit)
            
        except Exception as e:
            print(f"     ⚠️ Project listing failed: {e}")
        
        return []
    
    def _get_issues_from_project(self, project_key: str, limit=None) -> List[Dict]:
        """Get issues from a specific project"""
        try:
            api_path = "/rest/api/2/search"
            params = {
                'jql': f'project = {project_key} ORDER BY updated DESC',
                'startAt': 0,
                'maxResults': min(10, limit) if limit else 10,
                'fields': 'summary,description,creator,reporter,status,updated'
            }
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{self.base_domain}{api_path}?{param_string}"
            
            print(f"     🔗 Getting issues from project {project_key}")
            
            response = self._make_request_with_retry(full_url, f"project_issues_{project_key}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                issues = data.get('issues', [])
                
                if issues:
                    print(f"     ✅ Found {len(issues)} issues in project {project_key}")
                    return issues
            
        except Exception as e:
            print(f"     ⚠️ Failed to get issues from project {project_key}: {e}")
        
        return []
    
    def process_issue(self, issue_data: Dict) -> Tuple[Dict, Dict]:
        """Process Jira issue - FIXED"""
        issue_key = issue_data.get('key', 'Unknown')
        fields = issue_data.get('fields', {})
        
        processing_info = {
            'has_content': False,
            'sync_successful': False,
            'sync_error': None
        }
        
        try:
            # Extract issue details
            summary = fields.get('summary', 'No Summary')
            description = fields.get('description', {})
            
            # Extract content from description (handle both string and ADF format)
            content = self._extract_content_from_description(description)
            if content and len(content.strip()) > 10:
                processing_info['has_content'] = True
            
            # Extract creator/reporter emails
            creator = fields.get('creator', {})
            reporter = fields.get('reporter', {})
            assignee = fields.get('assignee', {})
            
            author_email = (creator.get('emailAddress') or 
                          reporter.get('emailAddress') or 
                          f"{creator.get('accountId', 'unknown')}@domain.com")
            
            # Extract user emails with access
            user_emails = set()
            
            # Add creator
            if creator.get('emailAddress'):
                user_emails.add(creator['emailAddress'].lower())
            elif creator.get('accountId'):
                user_emails.add(f"{creator['accountId']}@domain.com")
            
            # Add reporter
            if reporter.get('emailAddress'):
                user_emails.add(reporter['emailAddress'].lower())
            elif reporter.get('accountId'):
                user_emails.add(f"{reporter['accountId']}@domain.com")
            
            # Add assignee
            if assignee and assignee.get('emailAddress'):
                user_emails.add(assignee['emailAddress'].lower())
            elif assignee and assignee.get('accountId'):
                user_emails.add(f"{assignee['accountId']}@domain.com")
            
            # Build comprehensive searchable content
            searchable_content = f"""
Issue: {summary}
Key: {issue_key}
Status: {fields.get('status', {}).get('name', 'Unknown')}
Priority: {fields.get('priority', {}).get('name', 'Unknown')}
Type: {fields.get('issuetype', {}).get('name', 'Unknown')}
Creator: {creator.get('displayName', 'Unknown')}
Reporter: {reporter.get('displayName', 'Unknown')}
Assignee: {assignee.get('displayName', 'Unassigned') if assignee else 'Unassigned'}

Description:
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
                "id": f"jira_{issue_key}",
                "title": f"[{issue_key}] {summary}",
                "content": searchable_content,
                "tool": "jira",
                "file_id": issue_key,
                "user_emails_with_access": normalized_emails,  # 🔧 FIXED
                "created_at": fields.get('created'),
                "updated_at": fields.get('updated'),
                "author": creator.get('displayName', 'Unknown'),
                "type": "jira_issue",
                
                # Universal fields
                "sync_successful": processing_info['sync_successful'],
                "account_id": self.account_id,
                "external_user_id": self.external_user_id,
                "author_email": author_email.lower().strip() if author_email else "unknown@domain.com",  # 🔧 FIXED
                "integration_type": "jira",
                
                # Jira-specific URLs
                "file_url": f"{self.base_domain}/browse/{issue_key}",
                "web_view_link": f"{self.base_domain}/browse/{issue_key}",
                
                # Metadata
                "metadata": {
                    "issue_key": issue_key,
                    "status": fields.get('status', {}).get('name'),
                    "priority": fields.get('priority', {}).get('name'),
                    "issue_type": fields.get('issuetype', {}).get('name'),
                    "creator": creator.get('displayName'),
                    "reporter": reporter.get('displayName'),
                    "assignee": assignee.get('displayName') if assignee else None,
                    "has_content": processing_info['has_content']
                }
            }
            
            self.analytics.track_content_processing(processing_info)
            return doc_data, processing_info
            
        except Exception as e:
            processing_info['sync_successful'] = False
            processing_info['sync_error'] = str(e)
            print(f"❌ Failed to process issue {issue_key}: {e}")
            
            # Return minimal doc for failed processing
            doc_data = {
                "id": f"jira_{issue_key}",
                "title": f"[{issue_key}] Failed to Process",
                "content": f"Jira issue {issue_key} - processing failed",
                "tool": "jira",
                "sync_successful": False,
                "sync_error": str(e),
                "user_emails_with_access": ["unknown@domain.com"],
                "author_email": "unknown@domain.com",
                "account_id": self.account_id,
                "external_user_id": self.external_user_id,
                "integration_type": "jira"
            }
            
            return doc_data, processing_info
    
    def _extract_content_from_description(self, description) -> str:
        """Extract text content from description (handle multiple formats)"""
        if not description:
            return ""
        
        # If it's already a string, return it
        if isinstance(description, str):
            return description
        
        # If it's ADF (Atlassian Document Format)
        if isinstance(description, dict):
            return self._extract_from_adf(description)
        
        # Convert to string as fallback
        return str(description)
    
    def _extract_from_adf(self, node) -> str:
        """Extract text from Atlassian Document Format"""
        if isinstance(node, str):
            return node
        
        if isinstance(node, dict):
            text_parts = []
            
            # Extract text from text nodes
            if node.get('type') == 'text':
                return node.get('text', '')
            
            # Extract from content array
            content = node.get('content', [])
            if isinstance(content, list):
                for item in content:
                    text_parts.append(self._extract_from_adf(item))
            
            return ' '.join(filter(None, text_parts))
        
        if isinstance(node, list):
            return ' '.join(self._extract_from_adf(item) for item in node)
        
        return ""
    
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