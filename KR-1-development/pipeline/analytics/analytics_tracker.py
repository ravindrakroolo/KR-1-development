import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from collections import Counter, defaultdict

class AnalyticsTracker:
    """Tracks performance metrics across all connectors + search analytics"""
    
    def __init__(self):
        self.metrics = {
            'pipedream': {
                'total_requests': 0,
                'successful_requests': 0,
                'failed_requests': 0,
                'timeout_errors': 0,
                'retry_attempts': 0,
                'total_time': 0.0,
                'errors': []
            },
            'astra_db': {
                'total_operations': 0,
                'successful_inserts': 0,
                'failed_inserts': 0,
                'successful_searches': 0,
                'failed_searches': 0,
                'total_documents_stored': 0,
                'search_times': [],
                'errors': []
            },
            'content_processing': {
                'total_items_found': 0,
                'items_with_permissions': 0,
                'items_without_permissions': 0,
                'items_with_content': 0,
                'items_without_content': 0,
                'large_items_skipped': 0,
                'timeout_items_skipped': 0,
                'sync_successful': 0,
                'sync_failed': 0
            },
            'access_control': {
                'total_searches': 0,
                'searches_with_user_email': 0,
                'searches_without_user_email': 0,
                'total_documents_filtered': 0,
                'documents_accessible_to_users': 0,
                'new_rbac_usage': 0,
                'legacy_rbac_usage': 0
            },
            # NEW: Search Analytics
            'search_analytics': {
                'total_searches': 0,
                'unique_users': set(),
                'search_queries': [],
                'trending_queries': Counter(),
                'failed_searches': 0,
                'zero_result_searches': 0,
                'search_response_times': [],
                'popular_integrations': Counter(),
                'search_by_hour': defaultdict(int),
                'search_by_account': Counter()
            },
            # NEW: Suggestion Analytics
            'suggestion_analytics': {
                'dynamic_suggestions_generated': 0,
                'trending_suggestions_generated': 0,
                'recent_suggestions_generated': 0,
                'document_suggestions_generated': 0,
                'suggestion_clicks': 0,
                'suggestion_types_used': Counter(),
                'successful_suggestions': 0,
                'failed_suggestions': 0,
                'rbac_filtered_suggestions': 0
            },
            # NEW: Document Access Analytics
            'document_analytics': {
                'total_document_accesses': 0,
                'popular_documents': Counter(),
                'document_access_by_integration': Counter(),
                'recent_document_views': [],
                'suggested_document_clicks': 0,
                'trending_document_clicks': 0,
                'search_result_clicks': 0
            },
            # NEW: RBAC Analytics
            'rbac_analytics': {
                'new_structure_queries': 0,
                'legacy_structure_queries': 0,
                'rbac_filtering_time': [],
                'documents_filtered_by_rbac': 0,
                'rbac_access_granted': 0,
                'rbac_access_denied': 0,
                'account_id_usage': Counter(),
                'external_user_usage': Counter()
            }
        }
        self.start_time = time.time()
    
    # ==================== EXISTING METHODS ====================
    
    def track_pipedream_request(self, success: bool, duration: float, error: str = None, is_timeout: bool = False, is_retry: bool = False):
        """Track Pipedream API request"""
        self.metrics['pipedream']['total_requests'] += 1
        self.metrics['pipedream']['total_time'] += duration
        
        if is_retry:
            self.metrics['pipedream']['retry_attempts'] += 1
        
        if success:
            self.metrics['pipedream']['successful_requests'] += 1
        else:
            self.metrics['pipedream']['failed_requests'] += 1
            if is_timeout:
                self.metrics['pipedream']['timeout_errors'] += 1
            if error:
                self.metrics['pipedream']['errors'].append({
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': error,
                    'is_timeout': is_timeout,
                    'is_retry': is_retry
                })
    
    def track_astra_operation(self, operation: str, success: bool, duration: float, error: str = None):
        """Track Astra DB operation"""
        self.metrics['astra_db']['total_operations'] += 1
        
        if operation == 'insert':
            if success:
                self.metrics['astra_db']['successful_inserts'] += 1
                self.metrics['astra_db']['total_documents_stored'] += 1
            else:
                self.metrics['astra_db']['failed_inserts'] += 1
        elif operation == 'search':
            if success:
                self.metrics['astra_db']['successful_searches'] += 1
                self.metrics['astra_db']['search_times'].append(duration)
            else:
                self.metrics['astra_db']['failed_searches'] += 1
        
        if not success and error:
            self.metrics['astra_db']['errors'].append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'operation': operation,
                'error': error
            })
    
    def track_content_processing(self, item_info: Dict):
        """Track content processing metrics"""
        self.metrics['content_processing']['total_items_found'] += 1
        
        if item_info.get('has_permissions'):
            self.metrics['content_processing']['items_with_permissions'] += 1
        else:
            self.metrics['content_processing']['items_without_permissions'] += 1
        
        if item_info.get('has_content'):
            self.metrics['content_processing']['items_with_content'] += 1
        else:
            self.metrics['content_processing']['items_without_content'] += 1
        
        if item_info.get('skipped_large_file'):
            self.metrics['content_processing']['large_items_skipped'] += 1
            
        if item_info.get('skipped_timeout'):
            self.metrics['content_processing']['timeout_items_skipped'] += 1
        
        if item_info.get('sync_successful'):
            self.metrics['content_processing']['sync_successful'] += 1
        else:
            self.metrics['content_processing']['sync_failed'] += 1
    
    def track_access_control(self, user_email_provided: bool, documents_found: int, documents_accessible: int, use_new_rbac: bool = False):
        """Track access control metrics"""
        self.metrics['access_control']['total_searches'] += 1
        
        if user_email_provided:
            self.metrics['access_control']['searches_with_user_email'] += 1
        else:
            self.metrics['access_control']['searches_without_user_email'] += 1
        
        self.metrics['access_control']['total_documents_filtered'] += documents_found
        self.metrics['access_control']['documents_accessible_to_users'] += documents_accessible
        
        # Track RBAC structure usage
        if use_new_rbac:
            self.metrics['access_control']['new_rbac_usage'] += 1
        else:
            self.metrics['access_control']['legacy_rbac_usage'] += 1
    
    # ==================== NEW SEARCH ANALYTICS METHODS ====================
    
    def track_search_query(self, query: str, user_email: str, account_id: str = None, 
                          external_user_id: str = None, results_count: int = 0, 
                          response_time: float = 0, integration_types: List[str] = None,
                          success: bool = True):
        """Track individual search queries with comprehensive analytics"""
        current_time = datetime.now(timezone.utc)
        
        self.metrics['search_analytics']['total_searches'] += 1
        self.metrics['search_analytics']['unique_users'].add(user_email)
        
        # Track query details
        search_record = {
            'query': query.lower(),
            'user_email': user_email,
            'account_id': account_id,
            'external_user_id': external_user_id,
            'timestamp': current_time.isoformat(),
            'results_count': results_count,
            'response_time': response_time,
            'integrations': integration_types or [],
            'success': success
        }
        
        self.metrics['search_analytics']['search_queries'].append(search_record)
        
        # Keep only last 1000 searches to prevent memory issues
        if len(self.metrics['search_analytics']['search_queries']) > 1000:
            self.metrics['search_analytics']['search_queries'] = self.metrics['search_analytics']['search_queries'][-1000:]
        
        # Track trending queries
        if success and query.strip():
            self.metrics['search_analytics']['trending_queries'][query.lower()] += 1
        
        # Track failures and zero results
        if not success:
            self.metrics['search_analytics']['failed_searches'] += 1
        elif results_count == 0:
            self.metrics['search_analytics']['zero_result_searches'] += 1
        
        # Track response times
        if response_time > 0:
            self.metrics['search_analytics']['search_response_times'].append(response_time)
        
        # Track popular integrations
        if integration_types:
            for integration in integration_types:
                self.metrics['search_analytics']['popular_integrations'][integration] += 1
        
        # Track search patterns by hour
        hour = current_time.hour
        self.metrics['search_analytics']['search_by_hour'][hour] += 1
        
        # Track search patterns by account
        if account_id:
            self.metrics['search_analytics']['search_by_account'][account_id] += 1
    
    def track_suggestion_generation(self, suggestion_type: str, user_email: str, 
                                   account_id: str, suggestions_count: int, 
                                   rbac_filtered_count: int = 0, success: bool = True):
        """Track suggestion generation analytics"""
        if suggestion_type == 'dynamic':
            self.metrics['suggestion_analytics']['dynamic_suggestions_generated'] += 1
        elif suggestion_type == 'trending':
            self.metrics['suggestion_analytics']['trending_suggestions_generated'] += 1
        elif suggestion_type == 'recent':
            self.metrics['suggestion_analytics']['recent_suggestions_generated'] += 1
        elif suggestion_type == 'documents':
            self.metrics['suggestion_analytics']['document_suggestions_generated'] += 1
        
        self.metrics['suggestion_analytics']['suggestion_types_used'][suggestion_type] += 1
        
        if success:
            self.metrics['suggestion_analytics']['successful_suggestions'] += suggestions_count
        else:
            self.metrics['suggestion_analytics']['failed_suggestions'] += 1
        
        if rbac_filtered_count > 0:
            self.metrics['suggestion_analytics']['rbac_filtered_suggestions'] += rbac_filtered_count
    
    def track_suggestion_click(self, suggestion_type: str, suggestion_text: str, 
                              user_email: str, clicked_link: str):
        """Track when users click on suggestions"""
        self.metrics['suggestion_analytics']['suggestion_clicks'] += 1
        
        # Could store click details for more advanced analytics
        click_record = {
            'suggestion_type': suggestion_type,
            'suggestion_text': suggestion_text,
            'user_email': user_email,
            'clicked_link': clicked_link,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        # Store in a separate collection or extend metrics if needed
    
    def track_document_access(self, document_title: str, document_id: str, 
                             integration_type: str, user_email: str, 
                             access_method: str = 'direct'):
        """Track document access patterns"""
        self.metrics['document_analytics']['total_document_accesses'] += 1
        self.metrics['document_analytics']['popular_documents'][document_title] += 1
        self.metrics['document_analytics']['document_access_by_integration'][integration_type] += 1
        
        # Track recent document views (keep last 100)
        view_record = {
            'document_title': document_title,
            'document_id': document_id,
            'integration_type': integration_type,
            'user_email': user_email,
            'access_method': access_method,  # 'direct', 'suggestion', 'search_result', 'trending'
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        self.metrics['document_analytics']['recent_document_views'].append(view_record)
        if len(self.metrics['document_analytics']['recent_document_views']) > 100:
            self.metrics['document_analytics']['recent_document_views'] = self.metrics['document_analytics']['recent_document_views'][-100:]
        
        # Track specific access methods
        if access_method == 'suggestion':
            self.metrics['document_analytics']['suggested_document_clicks'] += 1
        elif access_method == 'trending':
            self.metrics['document_analytics']['trending_document_clicks'] += 1
        elif access_method == 'search_result':
            self.metrics['document_analytics']['search_result_clicks'] += 1
    
    def track_rbac_operation(self, operation_type: str, account_id: str, 
                           external_user_id: str, user_email: str, 
                           documents_found: int, documents_granted: int, 
                           processing_time: float, use_new_structure: bool = True):
        """Track RBAC operations for analytics"""
        if use_new_structure:
            self.metrics['rbac_analytics']['new_structure_queries'] += 1
        else:
            self.metrics['rbac_analytics']['legacy_structure_queries'] += 1
        
        self.metrics['rbac_analytics']['rbac_filtering_time'].append(processing_time)
        self.metrics['rbac_analytics']['documents_filtered_by_rbac'] += documents_found
        self.metrics['rbac_analytics']['rbac_access_granted'] += documents_granted
        self.metrics['rbac_analytics']['rbac_access_denied'] += (documents_found - documents_granted)
        
        # Track usage patterns
        if account_id:
            self.metrics['rbac_analytics']['account_id_usage'][account_id] += 1
        if external_user_id:
            self.metrics['rbac_analytics']['external_user_usage'][external_user_id] += 1
    
    # ==================== ANALYTICS QUERY METHODS ====================
    
    def get_trending_queries(self, limit: int = 10) -> List[Dict]:
        """Get trending search queries"""
        trending = []
        for query, count in self.metrics['search_analytics']['trending_queries'].most_common(limit):
            trending.append({
                'query': query,
                'search_count': count,
                'trend_score': count  # Could be more sophisticated
            })
        return trending
    
    def get_popular_documents(self, limit: int = 10) -> List[Dict]:
        """Get most accessed documents"""
        popular = []
        for doc_title, access_count in self.metrics['document_analytics']['popular_documents'].most_common(limit):
            popular.append({
                'document_title': doc_title,
                'access_count': access_count,
                'popularity_score': access_count
            })
        return popular
    
    def get_user_search_history(self, user_email: str, limit: int = 10) -> List[Dict]:
        """Get recent searches for a specific user"""
        user_searches = []
        for search in reversed(self.metrics['search_analytics']['search_queries']):
            if search['user_email'] == user_email:
                user_searches.append(search)
                if len(user_searches) >= limit:
                    break
        return user_searches
    
    def get_search_analytics_summary(self) -> Dict:
        """Get comprehensive search analytics summary"""
        total_searches = self.metrics['search_analytics']['total_searches']
        unique_users_count = len(self.metrics['search_analytics']['unique_users'])
        
        avg_response_time = 0
        if self.metrics['search_analytics']['search_response_times']:
            avg_response_time = sum(self.metrics['search_analytics']['search_response_times']) / len(self.metrics['search_analytics']['search_response_times'])
        
        success_rate = 0
        if total_searches > 0:
            successful_searches = total_searches - self.metrics['search_analytics']['failed_searches']
            success_rate = (successful_searches / total_searches) * 100
        
        return {
            'total_searches': total_searches,
            'unique_users': unique_users_count,
            'success_rate_percent': round(success_rate, 2),
            'average_response_time_seconds': round(avg_response_time, 3),
            'zero_result_searches': self.metrics['search_analytics']['zero_result_searches'],
            'top_trending_queries': self.get_trending_queries(5),
            'popular_integrations': dict(self.metrics['search_analytics']['popular_integrations'].most_common(5)),
            'search_patterns_by_hour': dict(self.metrics['search_analytics']['search_by_hour']),
            'rbac_new_vs_legacy': {
                'new_structure': self.metrics['rbac_analytics']['new_structure_queries'],
                'legacy_structure': self.metrics['rbac_analytics']['legacy_structure_queries']
            }
        }
    
    # ==================== ENHANCED REPORTING ====================
    
    def generate_report(self) -> Dict:
        """Generate comprehensive analytics report with search analytics"""
        total_runtime = time.time() - self.start_time
        
        # Calculate existing rates
        pipedream_success_rate = 0
        if self.metrics['pipedream']['total_requests'] > 0:
            pipedream_success_rate = (
                self.metrics['pipedream']['successful_requests'] / 
                self.metrics['pipedream']['total_requests'] * 100
            )
        
        astra_success_rate = 0
        if self.metrics['astra_db']['total_operations'] > 0:
            successful_ops = (
                self.metrics['astra_db']['successful_inserts'] + 
                self.metrics['astra_db']['successful_searches']
            )
            astra_success_rate = successful_ops / self.metrics['astra_db']['total_operations'] * 100
        
        content_extraction_rate = 0
        if self.metrics['content_processing']['total_items_found'] > 0:
            content_extraction_rate = (
                self.metrics['content_processing']['items_with_content'] / 
                self.metrics['content_processing']['total_items_found'] * 100
            )
        
        sync_success_rate = 0
        if self.metrics['content_processing']['total_items_found'] > 0:
            sync_success_rate = (
                self.metrics['content_processing']['sync_successful'] / 
                self.metrics['content_processing']['total_items_found'] * 100
            )
        
        return {
            'pipeline_summary': {
                'total_runtime_seconds': round(total_runtime, 2),
                'overall_health_score': round((pipedream_success_rate + astra_success_rate + content_extraction_rate) / 3, 2),
                'sync_success_rate_percent': round(sync_success_rate, 2)
            },
            'pipedream_performance': {
                'total_requests': self.metrics['pipedream']['total_requests'],
                'success_rate_percent': round(pipedream_success_rate, 2),
                'timeout_errors': self.metrics['pipedream']['timeout_errors'],
                'retry_attempts': self.metrics['pipedream']['retry_attempts'],
                'recent_errors': self.metrics['pipedream']['errors'][-3:]
            },
            'astra_db_performance': {
                'total_operations': self.metrics['astra_db']['total_operations'],
                'success_rate_percent': round(astra_success_rate, 2),
                'documents_stored': self.metrics['astra_db']['total_documents_stored'],
                'recent_errors': self.metrics['astra_db']['errors'][-3:]
            },
            'content_processing': {
                'total_items_discovered': self.metrics['content_processing']['total_items_found'],
                'content_extraction_rate_percent': round(content_extraction_rate, 2),
                'sync_success_rate_percent': round(sync_success_rate, 2),
                'items_synced_successfully': self.metrics['content_processing']['sync_successful'],
                'items_sync_failed': self.metrics['content_processing']['sync_failed']
            },
            # NEW: Search Analytics Section
            'search_analytics': self.get_search_analytics_summary(),
            # NEW: Suggestion Analytics
            'suggestion_analytics': {
                'total_suggestions_generated': sum([
                    self.metrics['suggestion_analytics']['dynamic_suggestions_generated'],
                    self.metrics['suggestion_analytics']['trending_suggestions_generated'],
                    self.metrics['suggestion_analytics']['recent_suggestions_generated'],
                    self.metrics['suggestion_analytics']['document_suggestions_generated']
                ]),
                'suggestion_clicks': self.metrics['suggestion_analytics']['suggestion_clicks'],
                'most_used_suggestion_types': dict(self.metrics['suggestion_analytics']['suggestion_types_used'].most_common(5)),
                'rbac_filtered_suggestions': self.metrics['suggestion_analytics']['rbac_filtered_suggestions']
            },
            # NEW: Document Analytics
            'document_analytics': {
                'total_document_accesses': self.metrics['document_analytics']['total_document_accesses'],
                'popular_documents': self.get_popular_documents(5),
                'access_by_integration': dict(self.metrics['document_analytics']['document_access_by_integration'].most_common(5)),
                'suggestion_click_rate': {
                    'suggested_document_clicks': self.metrics['document_analytics']['suggested_document_clicks'],
                    'trending_document_clicks': self.metrics['document_analytics']['trending_document_clicks'],
                    'search_result_clicks': self.metrics['document_analytics']['search_result_clicks']
                }
            }
        }