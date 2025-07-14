import os
import time
import json
import urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from astrapy import DataAPIClient
import difflib
import re
from analytics.analytics_tracker import AnalyticsTracker
from analytics.spellcorrector import SpellCorrector

class AstraDBManager:
    """Universal document storage with access control and enhanced features"""
    
    def __init__(self, analytics_tracker: AnalyticsTracker, empty_collections: bool = False, chunk_all: bool = False):
        self.token = os.getenv('TOKEN_ASTRA_DB')
        self.endpoint = "https://12489268-0070-438b-8336-debf65746a43-us-east-2.apps.astra.datastax.com"
        self.analytics = analytics_tracker
        self.empty_collections = empty_collections
        self.chunk_all = chunk_all
        
        if not self.token:
            raise ValueError("TOKEN_ASTRA_DB not found in environment variables")
        
        self.client = DataAPIClient(self.token)
        self.db = self.client.get_database_by_api_endpoint(self.endpoint)
        
        self.documents_collection_name = "enterprise_documents"
        self.analytics_collection_name = "search_analytics"
        
        # Initialize spell corrector
        self.spell_corrector = SpellCorrector()
        
        print(f"✅ Connected to Astra DB: {self.db.list_collection_names()}")
        if not chunk_all:
            print(f"📦 Chunking Mode: First chunk only (use --chunkall for full chunking)")
        else:
            print(f"📦 Chunking Mode: All chunks enabled")
        self._ensure_collections()
    
    def _ensure_collections(self):
        """Create or empty collections"""
        existing_collections = self.db.list_collection_names()
        
        if self.empty_collections:
            if self.documents_collection_name not in existing_collections:
                raise ValueError(f"❌ Collection '{self.documents_collection_name}' does not exist")
            
            self.docs_collection = self.db.get_collection(self.documents_collection_name)
            self.search_analytics_collection = self.db.get_collection(self.analytics_collection_name)
            
            docs_deleted = self._empty_collection(self.docs_collection)
            analytics_deleted = self._empty_collection(self.search_analytics_collection)
            print(f"✅ Emptied collections: {docs_deleted + analytics_deleted} documents deleted")
        else:
            if self.documents_collection_name not in existing_collections:
                self.db.create_collection(self.documents_collection_name)
                print(f"✅ Created {self.documents_collection_name}")
            
            if self.analytics_collection_name not in existing_collections:
                self.db.create_collection(self.analytics_collection_name)
                print(f"✅ Created {self.analytics_collection_name}")
            
            self.docs_collection = self.db.get_collection(self.documents_collection_name)
            self.search_analytics_collection = self.db.get_collection(self.analytics_collection_name)
    
    def _empty_collection(self, collection) -> int:
        """Empty collection and return count"""
        try:
            doc_count = collection.count_documents({}, upper_bound=100000)
            if doc_count == 0:
                return 0
            
            batch_size = 1000
            total_deleted = 0
            
            while True:
                docs_to_delete = list(collection.find({}, limit=batch_size, projection={"_id": 1}))
                if not docs_to_delete:
                    break
                
                for doc in docs_to_delete:
                    collection.delete_one({"_id": doc["_id"]})
                
                total_deleted += len(docs_to_delete)
                time.sleep(0.1)
            
            return total_deleted
            
        except Exception as e:
            print(f"❌ Error emptying collection: {e}")
            return 0
    
    def store_document(self, doc_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Store document with universal schema and automatic content chunking for large documents"""
        start_time = time.time()
        
        try:
            title = doc_data.get("title", doc_data.get("name", "Untitled"))
            content = doc_data.get("content", "")
            
            current_timestamp = datetime.now(timezone.utc).isoformat()
            
            # Extract author email (critical for access control)
            author_email = doc_data.get("author_email") or doc_data.get("author")
            if not author_email or '@' not in str(author_email):
                user_emails = doc_data.get("user_emails_with_access", [])
                author_email = user_emails[0] if user_emails else "unknown@domain.com"
            
            # 🔧 FIXED: Normalize and ensure consistent email handling
            authorized_emails = doc_data.get("user_emails_with_access", [])
            if author_email and author_email not in authorized_emails:
                authorized_emails.append(author_email)
            
            # Normalize all emails for consistent querying
            authorized_emails = self.normalize_email_list(authorized_emails)
            
            # Determine sync status
            sync_status = doc_data.get("sync_successful", False)
            if not sync_status:
                sync_status = bool(content and len(content.strip()) > 10)
            
            # CONTENT CHUNKING FOR LARGE DOCUMENTS
            MAX_CONTENT_SIZE = 4000
            content_bytes = len(content.encode('utf-8'))
            
            if content_bytes <= MAX_CONTENT_SIZE:
                # Small document - store normally
                return self._store_single_document(doc_data, title, content, author_email, sync_status, current_timestamp, start_time)
            else:
                # Large document - chunk the content
                print(f"   📦 Large document detected ({content_bytes} bytes) - chunking content...")
                return self._store_chunked_document_safe(doc_data, title, content, author_email, sync_status, current_timestamp, start_time, MAX_CONTENT_SIZE)
                
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self.analytics.track_astra_operation('insert', False, duration, error_msg)
            print(f"❌ Failed to store document: {error_msg}")
            return False, error_msg
    
    def _store_single_document(self, doc_data: Dict[str, Any], title: str, content: str, 
                              author_email: str, sync_status: bool, current_timestamp: str, start_time: float) -> Tuple[bool, str]:
        """Store a single document (normal case)"""
        searchable_text = f"{title} {content}".strip()[:8000]
        
        # 🔧 FIXED: Get and normalize authorized emails
        authorized_emails = doc_data.get("user_emails_with_access", [])
        if author_email and author_email not in authorized_emails:
            authorized_emails.append(author_email)
        authorized_emails = self.normalize_email_list(authorized_emails)
        
        # 🔧 FIXED: ENHANCED UNIVERSAL DOCUMENT SCHEMA with consistent RBAC structure
        document = {
            "id": doc_data.get("id"),
            "title": title,
            "content": content,
            "tool": doc_data.get("tool"),
            "file_id": doc_data.get("file_id", doc_data.get("id")),
            "created_at": doc_data.get("created_at", current_timestamp),
            "updated_at": doc_data.get("updated_at", current_timestamp),
            
            # Universal tracking fields
            "sync_status": sync_status,
            "last_sync_updated": current_timestamp,
            "integration_type": doc_data.get("integration_type", doc_data.get("tool", "unknown")),
            "file_url": doc_data.get("file_url", ""),
            "web_view_link": doc_data.get("web_view_link", ""),
            
            # 🔧 FIXED: Store BOTH legacy and new formats for compatibility
            # Legacy fields (keep for backward compatibility)
            "user_emails_with_access": authorized_emails,  # ✅ Normalized emails
            "external_user_id": doc_data.get("external_user_id", "kroolo-developer"),
            "account_id": doc_data.get("account_id", "unknown"),
            "author_email": author_email,  # ✅ Normalized email
            
            # 🔧 FIXED: Enhanced access control structure (for future use)
            "access_control": {
                "account_id": doc_data.get("account_id", "unknown"),
                "external_user_id": doc_data.get("external_user_id", "kroolo-developer"),
                "authorized_emails": authorized_emails,  # ✅ Consistent with legacy field
                "permission_level": doc_data.get("permission_level", "shared"),
                "created_by": author_email,  # ✅ Normalized email
                "last_updated": current_timestamp
            },
            
            # CHUNKING METADATA
            "is_chunked": False,
            "chunk_info": {
                "total_chunks": 1,
                "chunk_number": 1,
                "original_size": len(content.encode('utf-8')),
                "parent_document_id": doc_data.get("id")
            },
            
            "metadata": {
                "size": doc_data.get("size"),
                "mime_type": doc_data.get("mime_type"),
                "path": doc_data.get("path"),
                "permissions": doc_data.get("permissions", {}),
                "author": doc_data.get("author"),
                "type": doc_data.get("type"),
                "last_updated": doc_data.get("last_updated"),
                "has_content": doc_data.get("has_content", False),
                "content_size": doc_data.get("content_size", 0),
                "sync_attempts": doc_data.get("sync_attempts", 1),
                "sync_error": doc_data.get("sync_error"),
                "original_links": doc_data.get("original_links", {})
            },
            "searchable_text": searchable_text
        }
        
        result = self.docs_collection.insert_one(document)
        
        duration = time.time() - start_time
        self.analytics.track_astra_operation('insert', True, duration)
        
        title_short = document['title'][:50] + "..." if len(document['title']) > 50 else document['title']
        sync_indicator = "✅" if sync_status else "⚠️"
        print(f"{sync_indicator} Stored: {title_short}")
        
        return True, "Success"
    
    def _store_chunked_document_safe(self, doc_data: Dict[str, Any], title: str, content: str,
                                    author_email: str, sync_status: bool, current_timestamp: str, 
                                    start_time: float, max_chunk_size: int) -> Tuple[bool, str]:
        """Store a large document as chunks"""
        try:
            parent_doc_id = doc_data.get("id")
            original_size = len(content.encode('utf-8'))
            
            chunk_sizes_to_try = [max_chunk_size, 3000, 2000, 1500, 1000]
            
            for attempt, chunk_size in enumerate(chunk_sizes_to_try, 1):
                if self.chunk_all:
                    print(f"   📝 Attempt {attempt}: Splitting into chunks of {chunk_size} bytes (all chunks)...")
                else:
                    print(f"   📝 Attempt {attempt}: Creating first chunk of {chunk_size} bytes only...")
                
                try:
                    chunks = self._split_content_into_chunks(content, chunk_size)
                    total_chunks = len(chunks)
                    
                    if not self.chunk_all:
                        chunks = chunks[:1]
                        print(f"   📦 Created 1 chunk (first of {total_chunks}), attempting storage...")
                    else:
                        print(f"   📦 Created {total_chunks} chunks, attempting storage...")
                    
                    stored_chunks = 0
                    failed_chunks = 0
                    
                    for chunk_num, chunk_content in enumerate(chunks, 1):
                        try:
                            chunk_success = self._store_single_chunk(
                                doc_data, title, chunk_content, author_email, sync_status, 
                                current_timestamp, parent_doc_id, chunk_num, total_chunks, original_size
                            )
                            
                            if chunk_success:
                                stored_chunks += 1
                                if self.chunk_all and chunk_num % 10 == 0:
                                    print(f"   📄 Progress: {chunk_num}/{len(chunks)} chunks stored...")
                            else:
                                failed_chunks += 1
                        
                        except Exception as chunk_error:
                            failed_chunks += 1
                            print(f"   ⚠️ Chunk {chunk_num} failed: {str(chunk_error)[:100]}...")
                    
                    duration = time.time() - start_time
                    
                    if stored_chunks > 0:
                        self.analytics.track_astra_operation('insert', True, duration)
                        title_short = title[:50] + "..." if len(title) > 50 else title
                        sync_indicator = "✅" if sync_status else "⚠️"
                        
                        if self.chunk_all:
                            success_rate = (stored_chunks / len(chunks)) * 100
                            print(f"{sync_indicator} Stored (chunked): {title_short}")
                            print(f"   📊 Success: {stored_chunks}/{len(chunks)} chunks ({success_rate:.1f}%)")
                            return True, f"Stored {stored_chunks}/{len(chunks)} chunks successfully"
                        else:
                            print(f"{sync_indicator} Stored (first chunk only): {title_short}")
                            print(f"   📦 Size: {chunk_size} bytes | Total chunks available: {total_chunks}")
                            return True, f"Stored first chunk only (1/{total_chunks})"
                    
                except Exception as e:
                    print(f"   ❌ Attempt {attempt} failed: {str(e)[:100]}...")
                    if attempt < len(chunk_sizes_to_try):
                        continue
            
            # Final fallback - store document metadata only
            print(f"   📋 All chunking attempts failed, storing metadata only...")
            
            duration = time.time() - start_time
            self.analytics.track_astra_operation('insert', True, duration)
            print(f"⚠️ Stored metadata only: {title[:50]}... (content too large)")
            return True, "Stored metadata only - content too large for chunking"
                
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self.analytics.track_astra_operation('insert', False, duration, error_msg)
            print(f"❌ Critical chunking error: {error_msg}")
            return False, error_msg
    
    def _store_single_chunk(self, doc_data: Dict[str, Any], title: str, chunk_content: str,
                           author_email: str, sync_status: bool, current_timestamp: str,
                           parent_doc_id: str, chunk_num: int, total_chunks: int, original_size: int) -> bool:
        """Store a single chunk with minimal metadata"""
        try:
            chunk_id = f"{parent_doc_id}_chunk_{chunk_num}"
            chunk_title = f"{title} (Part {chunk_num}/{total_chunks})"
            
            # 🔧 FIXED: Minimal chunk document with consistent RBAC structure
            # Get normalized emails from parent document processing
            authorized_emails = doc_data.get("user_emails_with_access", [])
            if author_email and author_email not in authorized_emails:
                authorized_emails.append(author_email)
            
            # Normalize all emails
            authorized_emails = self.normalize_email_list(authorized_emails)
            
            chunk_document = {
                "id": chunk_id,
                "title": chunk_title,
                "content": chunk_content,
                "tool": doc_data.get("tool"),
                "file_id": doc_data.get("file_id", doc_data.get("id")),
                "sync_status": sync_status,
                "last_sync_updated": current_timestamp,
                "integration_type": doc_data.get("integration_type", doc_data.get("tool", "unknown")),
                "file_url": doc_data.get("file_url", ""),
                "web_view_link": doc_data.get("web_view_link", ""),
                
                # 🔧 FIXED: Store BOTH legacy and new formats for compatibility
                # Legacy fields (keep for backward compatibility)
                "user_emails_with_access": authorized_emails,  # ✅ Normalized emails
                "external_user_id": doc_data.get("external_user_id", "kroolo-developer"),
                "account_id": doc_data.get("account_id", "unknown"),
                "author_email": author_email,  # ✅ Normalized email
                
                # 🔧 FIXED: Enhanced access control structure (for future use)
                "access_control": {
                    "account_id": doc_data.get("account_id", "unknown"),
                    "external_user_id": doc_data.get("external_user_id", "kroolo-developer"),
                    "authorized_emails": authorized_emails,  # ✅ Consistent with legacy field
                    "permission_level": doc_data.get("permission_level", "shared"),
                    "created_by": author_email,  # ✅ Normalized email
                    "last_updated": current_timestamp
                },
                
                # Minimal chunking metadata
                "is_chunked": True,
                "chunk_info": {
                    "total_chunks": total_chunks,
                    "chunk_number": chunk_num,
                    "original_size": original_size,
                    "parent_document_id": parent_doc_id,
                    "original_title": title
                },
                
                # Minimal searchable text
                "searchable_text": f"{title} {chunk_content}"[:4000]
            }
            
            result = self.docs_collection.insert_one(chunk_document)
            return True
            
        except Exception as e:
            print(f"   ❌ Chunk {chunk_num} storage failed: {str(e)[:100]}...")
            return False
    
    def _split_content_into_chunks(self, content: str, max_chunk_size: int) -> List[str]:
        """Split content into chunks while trying to preserve sentence/paragraph boundaries"""
        if len(content.encode('utf-8')) <= max_chunk_size:
            return [content]
        
        chunks = []
        current_chunk = ""
        
        # Try to split by paragraphs first
        paragraphs = content.split('\n\n')
        
        for paragraph in paragraphs:
            paragraph_bytes = len(paragraph.encode('utf-8'))
            current_chunk_bytes = len(current_chunk.encode('utf-8'))
            
            # If adding this paragraph would exceed the limit
            if current_chunk_bytes + paragraph_bytes > max_chunk_size:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                    current_chunk = ""
                
                # If single paragraph is too large, split by sentences
                if paragraph_bytes > max_chunk_size:
                    sentence_chunks = self._split_by_sentences(paragraph, max_chunk_size)
                    chunks.extend(sentence_chunks)
                else:
                    current_chunk = paragraph
            else:
                # Add paragraph to current chunk
                if current_chunk:
                    current_chunk += "\n\n" + paragraph
                else:
                    current_chunk = paragraph
        
        # Add remaining content
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _split_by_sentences(self, text: str, max_chunk_size: int) -> List[str]:
        """Split text by sentences when paragraphs are too large"""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            sentence_bytes = len(sentence.encode('utf-8'))
            current_chunk_bytes = len(current_chunk.encode('utf-8'))
            
            if current_chunk_bytes + sentence_bytes > max_chunk_size:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                    current_chunk = ""
                
                # If single sentence is too large, force split by characters
                if sentence_bytes > max_chunk_size:
                    char_chunks = self._split_by_characters(sentence, max_chunk_size)
                    chunks.extend(char_chunks)
                else:
                    current_chunk = sentence
            else:
                # Add sentence to current chunk
                if current_chunk:
                    current_chunk += " " + sentence
                else:
                    current_chunk = sentence
        
        # Add remaining content
        if current_chunk.strip():
            chunks.append(current_chunk.strip())
        
        return chunks
    
    def _split_by_characters(self, text: str, max_chunk_size: int) -> List[str]:
        """Force split by characters as last resort"""
        chunks = []
        start = 0
        
        while start < len(text):
            # Find a safe end position (avoid splitting in middle of multi-byte characters)
            end = start + max_chunk_size
            if end >= len(text):
                chunks.append(text[start:])
                break
            
            # Try to find a good break point (space, punctuation)
            safe_end = end
            for i in range(min(100, end - start)):  # Look back up to 100 chars
                check_pos = end - i
                if text[check_pos] in ' \n\t.,;:!?':
                    safe_end = check_pos + 1
                    break
            
            chunk = text[start:safe_end].strip()
            if chunk:
                chunks.append(chunk)
            
            start = safe_end
        
        return chunks
    
    def normalize_email(self, email: str) -> str:
        """Normalize email for consistent comparison"""
        if not email or '@' not in str(email):
            return ""
        return str(email).lower().strip()
    
    def normalize_email_list(self, emails: List[str]) -> List[str]:
        """Normalize list of emails"""
        normalized = []
        for email in emails:
            norm_email = self.normalize_email(email)
            if norm_email and norm_email not in normalized:
                normalized.append(norm_email)
        return normalized
    
    def validate_rbac_data(self, doc_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate RBAC data before storage"""
        
        # Check required fields
        account_id = doc_data.get("account_id")
        external_user_id = doc_data.get("external_user_id")
        author_email = doc_data.get("author_email")
        
        if not account_id:
            return False, "Missing account_id"
        
        if not external_user_id:
            return False, "Missing external_user_id"
        
        if not author_email or '@' not in str(author_email):
            return False, "Invalid or missing author_email"
        
        # Validate email format
        emails = doc_data.get("user_emails_with_access", [])
        for email in emails:
            if email and '@' not in str(email):
                return False, f"Invalid email format: {email}"
        
        return True, "Valid"

    def search_documents(self, query: str, user_email: str, filters: Dict = None, 
                        sort_by: str = "relevance", limit: int = 10, offset: int = 0) -> Tuple[Dict, float]:
        """FIXED SEARCH with Triple Verification Logic (account_id + external_user_id + user_email)"""
        start_time = time.time()
        
        if not user_email:
            error_msg = "User email required for access control"
            duration = time.time() - start_time
            self.analytics.track_astra_operation('search', False, duration, error_msg)
            self.analytics.track_access_control(False, 0, 0)
            return {
                "results": [],
                "summary": {"total": 0, "by_source": {}, "error": error_msg}
            }, duration
        
        try:
            # Normalize user email for consistent comparison
            user_email_normalized = self.normalize_email(user_email)
            
            # Build base access filter
            access_filter = {
                "sync_status": True
            }
            
            # 🔐 Apply FIXED Triple Verification Logic if filters contain the required fields
            if filters and filters.get('external_user_id') and filters.get('account_id'):
                print(f"🔐 Applying FIXED Triple Verification Logic:")
                print(f"   Account ID: {filters['account_id']}")
                print(f"   External User ID: {filters['external_user_id']}")
                print(f"   User Email: {user_email}")
                
                # CRITICAL FIX: Use proper $or structure for new vs legacy compatibility
                access_filter.update({
                    "account_id": filters['account_id'],
                    "$or": [
                        # NEW structure
                        {
                            "access_control.external_user_id": filters['external_user_id'],
                            "access_control.authorized_emails": {"$in": [user_email_normalized]}
                        },
                        # LEGACY structure (CRITICAL for backward compatibility)
                        {
                            "external_user_id": filters['external_user_id'],
                            "user_emails_with_access": {"$in": [user_email_normalized]}
                        }
                    ]
                })
            else:
                # Fallback: Basic email-based access control
                access_filter["$or"] = [
                    {"user_emails_with_access": {"$in": [user_email_normalized]}},
                    {"author_email": {"$eq": user_email_normalized}},
                ]
            
            # Apply additional filters
            if filters:
                if filters.get("apps"):
                    access_filter["tool"] = {"$in": filters["apps"]}
                
                if filters.get("author"):
                    access_filter["$and"] = access_filter.get("$and", [])
                    access_filter["$and"].append({
                        "$or": [
                            {"metadata.author": {"$eq": filters["author"]}},
                            {"author_email": {"$eq": filters["author"]}}
                        ]
                    })
                
                if filters.get("file_types"):
                    access_filter["metadata.type"] = {"$in": filters["file_types"]}
                
                if filters.get("from_date") or filters.get("to_date"):
                    date_filter = {}
                    if filters.get("from_date"):
                        date_filter["$gte"] = filters["from_date"]
                    if filters.get("to_date"):
                        date_filter["$lte"] = filters["to_date"]
                    if date_filter:
                        access_filter["last_sync_updated"] = date_filter
                
                if filters.get("include_failed_sync"):
                    # Remove sync_status filter to include failed syncs
                    access_filter.pop("sync_status", None)
            
            # Get documents with MORE relaxed search
            search_limit = limit * 10  # Get way more results for better filtering
            
            cursor = self.docs_collection.find(
                access_filter,
                sort={"last_sync_updated": -1},
                limit=search_limit,
                skip=offset,
                projection={
                    "title": True, "content": True, "searchable_text": True,
                    "tool": True, "metadata": True, "updated_at": True,
                    "file_id": True, "sync_status": True, "author_email": True,
                    "last_sync_updated": True, "account_id": True,
                    "integration_type": True, "file_url": True, "web_view_link": True,
                    "user_emails_with_access": True, "is_chunked": True, "chunk_info": True
                }
            )
            
            all_results = list(cursor)
            print(f"   🔍 Database returned {len(all_results)} results before text filtering")
            
            # ENHANCED TEXT FILTERING - Search in ALL fields
            filtered_results = []
            query_words = []
            
            if query.strip():
                # Extract query words more intelligently
                query_words = [word.lower().strip() for word in re.split(r'[^\w@.-]', query) if len(word.strip()) > 1]
                print(f"   🔍 Searching for words: {query_words}")
            
            for doc in all_results:
                # Get ALL searchable text from document
                title = doc.get("title", "").lower()
                content = doc.get("content", "").lower()
                searchable_text = doc.get("searchable_text", "").lower()
                author_email = doc.get("author_email", "").lower()
                user_emails = " ".join(doc.get("user_emails_with_access", [])).lower()
                integration_type = doc.get("integration_type", "").lower()
                tool = doc.get("tool", "").lower()
                file_url = doc.get("file_url", "").lower()
                
                # Get metadata
                metadata = doc.get("metadata", {})
                metadata_author = str(metadata.get("author", "")).lower()
                metadata_type = str(metadata.get("type", "")).lower()
                metadata_path = str(metadata.get("path", "")).lower()
                
                # Combine ALL searchable fields
                all_text = " ".join([
                    title, content, searchable_text, author_email, user_emails,
                    integration_type, tool, metadata_author, metadata_type, metadata_path, file_url
                ])
                
                # Check if query matches
                if not query_words:
                    # No query - include all results (recent documents)
                    filtered_results.append(doc)
                else:
                    # Check for matches in ANY field
                    match_found = False
                    exact_matches = 0
                    partial_matches = 0
                    
                    # STRATEGY 1: Exact phrase search
                    if query.lower() in all_text:
                        match_found = True
                        exact_matches += 5  # High score for exact phrase
                    
                    # STRATEGY 2: Individual word search
                    for word in query_words:
                        if word in all_text:
                            match_found = True
                            if word in title:
                                exact_matches += 3  # High score for title match
                            elif word in author_email or word in user_emails:
                                exact_matches += 2  # Medium score for email match
                            else:
                                exact_matches += 1  # Basic match
                    
                    # STRATEGY 3: Partial word matching for longer words
                    for word in query_words:
                        if len(word) >= 4:
                            for text_word in all_text.split():
                                if word in text_word and len(text_word) >= 4:
                                    match_found = True
                                    partial_matches += 1
                    
                    if match_found:
                        # Calculate relevance score
                        relevance_score = exact_matches * 2 + partial_matches
                        doc["relevance_score"] = relevance_score
                        doc["exact_matches"] = exact_matches
                        doc["partial_matches"] = partial_matches
                        filtered_results.append(doc)
            
            print(f"   ✅ Text filtering found {len(filtered_results)} matching results")
            
            # If still no results, try fallback strategies
            if not filtered_results and query_words:
                print(f"   🔄 No matches found, trying fallback strategies...")
                
                # FALLBACK 1: Remove sync status requirement
                fallback_filter = access_filter.copy()
                fallback_filter.pop("sync_status", None)
                
                fallback_cursor = self.docs_collection.find(
                    fallback_filter,
                    sort={"last_sync_updated": -1},
                    limit=search_limit // 2
                )
                
                fallback_results = list(fallback_cursor)
                print(f"   🔍 Fallback search returned {len(fallback_results)} results")
                
                # Apply text filtering to fallback results
                for doc in fallback_results:
                    all_text = " ".join([
                        doc.get("title", "").lower(),
                        doc.get("content", "").lower(),
                        doc.get("author_email", "").lower(),
                        " ".join(doc.get("user_emails_with_access", [])).lower()
                    ])
                    
                    if any(word in all_text for word in query_words):
                        doc["relevance_score"] = 1  # Lower score for fallback
                        doc["is_fallback_result"] = True
                        filtered_results.append(doc)
                
                print(f"   ✅ Fallback filtering found {len(filtered_results)} additional results")
            
            # Generate "Did you mean?" suggestions if still few results
            suggestions = []
            if len(filtered_results) < 3 and query_words:
                suggestion_queries = self.spell_corrector.get_query_suggestions(query)
                for suggestion in suggestion_queries[:2]:
                    suggestions.append({
                        "query": suggestion,
                        "link": f"https://yourapp.com/search?q={urllib.parse.quote(suggestion)}"
                    })
            
            # Sort by relevance score (highest first)
            if query_words and sort_by == "relevance":
                filtered_results.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)
            elif sort_by == "date":
                filtered_results.sort(key=lambda x: x.get("last_sync_updated", ""), reverse=True)
            
            # Take only the requested limit
            processed_results = filtered_results[:limit]
            
            # Process results with enhanced info including author and access lists
            final_results = []
            for doc in processed_results:
                # Generate enhanced snippet with highlighting
                snippet = self._generate_enhanced_snippet(doc.get("content", ""), query_words, max_length=250)
                
                # Use actual URLs
                link = doc.get("web_view_link") or doc.get("file_url") or f"https://yourapp.com/docs/{doc.get('file_id')}"
                
                # Enhanced result with author and access info
                result_data = {
                    "title": doc.get("title", "Untitled"),
                    "snippet": snippet,
                    "link": link,
                    "author": doc.get("author_email", doc.get("metadata", {}).get("author", "Unknown")),
                    "source": doc.get("tool", "unknown"),
                    "integration_type": doc.get("integration_type", doc.get("tool", "unknown")),
                    "last_updated": doc.get("last_sync_updated", doc.get("updated_at", "")),
                    "type": doc.get("metadata", {}).get("type", "file"),
                    "sync_status": doc.get("sync_status", False),
                    "account_id": doc.get("account_id", "unknown"),
                    "relevance_score": doc.get("relevance_score", 0),
                    "is_fallback_result": doc.get("is_fallback_result", False),
                    
                    # ENHANCED: Show author and access lists
                    "author_email": doc.get("author_email", "Unknown"),
                    "users_with_access": doc.get("user_emails_with_access", []),
                    "access_count": len(doc.get("user_emails_with_access", [])),
                    
                    # Chunking information
                    "is_chunked": doc.get("is_chunked", False),
                    "chunk_info": doc.get("chunk_info", {}),
                    
                    # Highlighting
                    "title_matches": self._highlight_text(doc.get("title", ""), query_words),
                    "has_title_match": any(word.lower() in doc.get("title", "").lower() for word in query_words),
                    "has_content_match": any(word.lower() in doc.get("content", "").lower() for word in query_words),
                }
                
                final_results.append(result_data)
            
            # Calculate distributions
            source_counts = {}
            integration_counts = {}
            sync_status_counts = {"synced": 0, "failed": 0}
            
            for result in final_results:
                source = result.get("source", "unknown")
                integration = result.get("integration_type", "unknown")
                
                source_counts[source] = source_counts.get(source, 0) + 1
                integration_counts[integration] = integration_counts.get(integration, 0) + 1
                
                if result.get("sync_status"):
                    sync_status_counts["synced"] += 1
                else:
                    sync_status_counts["failed"] += 1
            
            duration = time.time() - start_time
            self.analytics.track_astra_operation('search', True, duration)
            self.analytics.track_access_control(True, len(all_results), len(final_results))
            
            # Log search
            self._log_search(query, user_email, len(final_results))
            
            # Enhanced search result summary
            search_summary = {
                "total": len(filtered_results),
                "returned": len(final_results),
                "by_source": source_counts,
                "by_integration": integration_counts,
                "by_sync_status": sync_status_counts,
                "search_time_ms": round(duration * 1000, 2),
                "user": user_email,
                "query": query,
                "query_words": query_words,
                "search_method": "enhanced_aggressive_search",
                "db_results": len(all_results),
                "filtered_results": len(filtered_results),
                "suggestions": suggestions,
                "status": "results_found" if final_results else "no_results_found"
            }
            
            return {
                "results": final_results,
                "summary": search_summary
            }, duration
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = str(e)
            self.analytics.track_astra_operation('search', False, duration, error_msg)
            print(f"❌ Search error: {error_msg}")
            return {
                "results": [],
                "summary": {"total": 0, "by_source": {}, "error": error_msg}
            }, duration
    
    def _generate_enhanced_snippet(self, content: str, query_words: List[str], max_length: int = 250) -> str:
        """Generate enhanced snippet with highlighting"""
        if not content:
            return "No content available"
        
        if not query_words:
            snippet = content[:max_length]
            return snippet + "..." if len(content) > max_length else snippet
        
        content_lower = content.lower()
        
        # Find best snippet position based on query word density
        best_pos = 0
        best_score = 0
        
        step_size = max_length // 4
        for i in range(0, max(1, len(content) - max_length + 1), step_size):
            snippet_text = content_lower[i:i + max_length]
            score = sum(snippet_text.count(word.lower()) for word in query_words)
            
            if score > best_score:
                best_score = score
                best_pos = i
        
        # Extract the best snippet
        snippet = content[best_pos:best_pos + max_length]
        
        # Highlight matching terms
        highlighted_snippet = snippet
        for word in query_words:
            if word.lower() in snippet.lower():
                pattern = re.compile(re.escape(word), re.IGNORECASE)
                highlighted_snippet = pattern.sub(
                    lambda m: f"**{m.group()}**",
                    highlighted_snippet
                )
        
        # Add ellipsis if truncated
        if best_pos > 0:
            highlighted_snippet = "..." + highlighted_snippet
        if best_pos + max_length < len(content):
            highlighted_snippet = highlighted_snippet + "..."
        
        return highlighted_snippet
    
    def _highlight_text(self, text: str, query_words: List[str]) -> str:
        """Highlight query words in text"""
        if not query_words or not text:
            return text
        
        highlighted = text
        for word in query_words:
            if word.lower() in text.lower():
                pattern = re.compile(re.escape(word), re.IGNORECASE)
                highlighted = pattern.sub(
                    lambda m: f"**{m.group()}**",
                    highlighted
                )
        
        return highlighted
    
    def _log_search(self, query: str, user_email: str, results_count: int):
        """Log search for analytics"""
        try:
            search_log = {
                "query": query,
                "user_email": user_email,
                "searched_at": datetime.now(timezone.utc).isoformat(),
                "results_count": results_count,
                "timestamp": time.time()
            }
            self.search_analytics_collection.insert_one(search_log)
        except Exception as e:
            print(f"⚠️ Failed to log search: {e}")
    
    def get_sync_status_summary(self) -> Dict:
        """Get summary of sync status across all documents"""
        try:
            total_docs = self.docs_collection.count_documents({}, upper_bound=10000)
            synced_docs = self.docs_collection.count_documents({"sync_status": True}, upper_bound=10000)
            failed_docs = self.docs_collection.count_documents({"sync_status": False}, upper_bound=10000)
            
            recent_failures = list(self.docs_collection.find(
                {"sync_status": False},
                sort={"last_sync_updated": -1},
                limit=5,
                projection={"title": True, "sync_error": True, "last_sync_updated": True}
            ))
            
            return {
                "total_documents": total_docs,
                "successfully_synced": synced_docs,
                "sync_failed": failed_docs,
                "sync_success_rate": round((synced_docs / max(1, total_docs)) * 100, 2),
                "recent_failures": recent_failures
            }
            
        except Exception as e:
            print(f"❌ Failed to get sync status summary: {e}")
            return {"error": str(e)}
    
    def get_user_file_info(self, external_user_id: str, limit: int = None) -> Dict:
        """Get files for external user ID"""
        try:
            base_query = {
                "$or": [
                    {"external_user_id": external_user_id},
                    {"account_id": external_user_id}
                ]
            }
            
            matches = self.docs_collection.count_documents(base_query, upper_bound=100000)
            working_query = base_query
            total_matches = matches

            if not total_matches:
                all_docs_cursor = self.docs_collection.find(
                    {},
                    limit=10000,
                    projection={"external_user_id": True, "account_id": True}
                )
                
                all_docs = list(all_docs_cursor)
                external_user_id_lower = external_user_id.lower()
                
                matching_ids = []
                for doc in all_docs:
                    doc_external_id = str(doc.get("external_user_id", "")).lower()
                    doc_account_id = str(doc.get("account_id", "")).lower()
                    
                    if (external_user_id_lower in doc_external_id or 
                        external_user_id_lower in doc_account_id or
                        doc_external_id in external_user_id_lower or
                        doc_account_id in external_user_id_lower):
                        matching_ids.append(doc.get("_id"))
                
                if matching_ids:
                    working_query = {"_id": {"$in": matching_ids}}
                    total_matches = len(matching_ids)
                    print(f"✅ Client-side filtering found {total_matches} matches")

            if not total_matches:
                return {
                    "external_user_id": external_user_id,
                    "total_files": 0,
                    "files_returned": 0,
                    "files": []
                }

            cursor = self.docs_collection.find(
                working_query,
                sort={"last_sync_updated": -1},
                limit=limit or 0
            )

            files = list(cursor)
            processed_files = []
            
            for doc in files:
                file_url = doc.get("file_url", "")
                web_view_link = doc.get("web_view_link", "")
                
                if not file_url and not web_view_link:
                    integration = doc.get("integration_type", "unknown")
                    file_id = doc.get("file_id", "")
                    if integration == "google_drive":
                        file_url = f"https://drive.google.com/file/d/{file_id}/view"
                    elif integration == "slack":
                        file_url = f"https://app.slack.com/client/{file_id}"
                    else:
                        file_url = f"https://yourapp.com/docs/{file_id}"

                processed_files.append({
                    "database_id": str(doc.get("_id", "")),
                    "file_id": doc.get("file_id", ""),
                    "document_id": doc.get("id", ""),
                    "filename": doc.get("title", "Untitled"),
                    "source_platform": doc.get("tool", "unknown"),
                    "integration_type": doc.get("integration_type", "unknown"),
                    "account_id": doc.get("account_id", ""),
                    "external_user_id": doc.get("external_user_id", ""),
                    "author_email": doc.get("author_email", ""),
                    "sync_status": doc.get("sync_status", False),
                    "last_sync_updated": doc.get("last_sync_updated", ""),
                    "users_with_access": doc.get("user_emails_with_access", []),
                    "total_users_with_access": len(doc.get("user_emails_with_access", [])),
                    "file_url": file_url,
                    "web_view_link": web_view_link or file_url,
                    "view_link": web_view_link or file_url,
                    "file_type": doc.get("metadata", {}).get("type", "unknown"),
                    "content_size": doc.get("metadata", {}).get("content_size", 0),
                    "has_content": doc.get("metadata", {}).get("has_content", False)
                })

            return {
                "external_user_id": external_user_id,
                "total_files": len(processed_files),
                "files_returned": len(processed_files),
                "files": processed_files
            }

        except Exception as e:
            return {"error": str(e), "files": []}
    
    def close_connection(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            print("✅ AstraDB connection closed")
    
    def get_dynamic_suggestions(self, partial_query: str, user_email: str = None, limit: int = 10) -> Dict:
        """Get intelligent dynamic suggestions for autocomplete - NEW ENHANCED METHOD"""
        try:
            if not partial_query or len(partial_query.strip()) < 1:
                return {"data": [], "total": 0}
            
            partial_lower = partial_query.lower().strip()
            all_suggestions = []
            
            # 1. Recent user searches (personalized)
            if user_email:
                recent_suggestions = self._get_recent_search_suggestions(partial_lower, user_email, 3)
                all_suggestions.extend(recent_suggestions)
            
            # 2. Trending searches (popular globally)
            trending_suggestions = self._get_trending_search_suggestions(partial_lower, 3)
            all_suggestions.extend(trending_suggestions)
            
            # 3. Content-based suggestions (from document titles)
            content_suggestions = self._get_content_based_suggestions(partial_lower, user_email, 4)
            all_suggestions.extend(content_suggestions)
            
            # 4. Document suggestions (actual documents matching query)
            document_suggestions = self._get_document_suggestions(partial_lower, user_email, 3)
            all_suggestions.extend(document_suggestions)
            
            # 5. Autocomplete predictions (next-word suggestions)
            autocomplete_suggestions = self._get_autocomplete_predictions(partial_lower, 2)
            all_suggestions.extend(autocomplete_suggestions)
            
            # 6. Spell correction suggestions
            spell_suggestions = self._get_spell_correction_suggestions(partial_lower, 2)
            all_suggestions.extend(spell_suggestions)
            
            # Remove duplicates while preserving order
            seen_texts = set()
            unique_suggestions = []
            for suggestion in all_suggestions:
                text_key = suggestion.get("text", "").lower()
                if text_key not in seen_texts and text_key != partial_lower:
                    seen_texts.add(text_key)
                    unique_suggestions.append(suggestion)
            
            # Sort by relevance score and limit results
            unique_suggestions.sort(key=lambda x: x.get("score", 0), reverse=True)
            final_suggestions = unique_suggestions[:limit]
            
            return {"data": final_suggestions, "total": len(final_suggestions)}
            
        except Exception as e:
            print(f"❌ Failed to get dynamic suggestions: {e}")
            return {"data": [], "total": 0}
    
    def _get_recent_search_suggestions(self, partial_query: str, user_email: str, limit: int = 3) -> List[Dict]:
        """Get suggestions from user's recent searches"""
        try:
            cursor = self.search_analytics_collection.find(
                {
                    "user_email": user_email,
                    "query": {"$regex": f".*{re.escape(partial_query)}.*", "$options": "i"}
                },
                sort={"searched_at": -1},
                limit=limit,
                projection={"query": True, "results_count": True}
            )
            
            suggestions = []
            for search in cursor:
                query = search.get("query", "")
                results_count = search.get("results_count", 0)
                
                if query and query.lower() != partial_query:
                    suggestions.append({
                        "text": query,
                        "type": "recent_search",
                        "score": 8.0 + (results_count * 0.1),  # Higher score for searches with more results
                        "icon": "🕒",
                        "description": f"Recent search ({results_count} results)"
                    })
            
            return suggestions
            
        except Exception as e:
            print(f"❌ Error getting recent search suggestions: {e}")
            return []
    
    def _get_trending_search_suggestions(self, partial_query: str, limit: int = 3) -> List[Dict]:
        """Get trending/popular search suggestions"""
        try:
            # Aggregate to find most popular searches containing the partial query
            pipeline = [
                {
                    "$match": {
                        "query": {"$regex": f".*{re.escape(partial_query)}.*", "$options": "i"},
                        "searched_at": {"$gte": (datetime.now() - timedelta(days=30)).isoformat()}  # Last 30 days
                    }
                },
                {
                    "$group": {
                        "_id": "$query",
                        "search_count": {"$sum": 1},
                        "avg_results": {"$avg": "$results_count"}
                    }
                },
                {"$sort": {"search_count": -1}},
                {"$limit": limit}
            ]
            
            suggestions = []
            for result in self.search_analytics_collection.aggregate(pipeline):
                query = result.get("_id", "")
                search_count = result.get("search_count", 0)
                avg_results = result.get("avg_results", 0)
                
                if query and query.lower() != partial_query:
                    suggestions.append({
                        "text": query,
                        "type": "trending",
                        "score": 7.0 + (search_count * 0.2),
                        "icon": "🔥",
                        "description": f"Trending ({search_count} searches)"
                    })
            
            return suggestions
            
        except Exception as e:
            print(f"❌ Error getting trending suggestions: {e}")
            return []
    
    def _get_content_based_suggestions(self, partial_query: str, user_email: str = None, limit: int = 4) -> List[Dict]:
        """Get suggestions based on document titles and content"""
        try:
            # AstraDB doesn't support $regex, so we'll fetch documents and filter manually
            base_filter = {"sync_status": True}
            
            # Filter by user access - support both legacy and new structure
            if user_email:
                base_filter["$or"] = [
                    {"user_emails_with_access": {"$in": [user_email.lower()]}},  # Legacy
                    {"access_control.authorized_emails": {"$in": [user_email.lower()]}}  # New structure
                ]
            
            cursor = self.docs_collection.find(
                base_filter,
                limit=50,  # Get more documents to filter manually
                projection={"title": True, "content": True, "searchable_text": True, "integration_type": True, "metadata.type": True}
            )
            
            suggestions = []
            seen_titles = set()
            partial_lower = partial_query.lower()
            
            for doc in cursor:
                title = doc.get("title", "")
                content = doc.get("content", "")
                searchable_text = doc.get("searchable_text", "")
                
                # Check if partial query matches title or content
                title_matches = partial_lower in title.lower()
                content_matches = partial_lower in content.lower()
                searchable_matches = partial_lower in searchable_text.lower()
                
                if (title_matches or content_matches or searchable_matches) and title and title.lower() not in seen_titles:
                    seen_titles.add(title.lower())
                    
                    # Extract meaningful phrases from title
                    title_words = title.split()
                    for i in range(len(title_words)):
                        for j in range(i + 1, min(i + 4, len(title_words) + 1)):  # 1-3 word phrases
                            phrase = " ".join(title_words[i:j])
                            if (len(phrase) > len(partial_query) and 
                                partial_lower in phrase.lower() and 
                                phrase.lower() != partial_lower):
                                
                                integration = doc.get("integration_type", "unknown").upper()
                                file_type = doc.get("metadata", {}).get("type", "file")
                                
                                suggestions.append({
                                    "text": phrase,
                                    "type": "content",
                                    "score": 6.0 + (len(phrase.split()) * 0.5),
                                    "icon": "📄",
                                    "description": f"From {integration} {file_type}",
                                    "document_title": title  # Add for RBAC verification
                                })
                                
                                if len(suggestions) >= limit:
                                    break
                        if len(suggestions) >= limit:
                            break
                    if len(suggestions) >= limit:
                        break
            
            return suggestions[:limit]
            
        except Exception as e:
            print(f"❌ Error getting content-based suggestions: {e}")
            return []
    
    def _get_document_suggestions(self, partial_query: str, user_email: str = None, limit: int = 3) -> List[Dict]:
        """Get actual document suggestions"""
        try:
            base_filter = {"sync_status": True}
            
            # Support both legacy and new structure
            if user_email:
                base_filter["$or"] = [
                    {"user_emails_with_access": {"$in": [user_email.lower()]}},  # Legacy
                    {"access_control.authorized_emails": {"$in": [user_email.lower()]}}  # New structure
                ]
            
            cursor = self.docs_collection.find(
                base_filter,
                limit=50,  # Get more documents to filter manually
                projection={"title": True, "integration_type": True, "web_view_link": True, "file_url": True}
            )
            
            suggestions = []
            partial_lower = partial_query.lower()
            
            for doc in cursor:
                title = doc.get("title", "")
                # Filter documents that contain the partial query in title
                if not title or partial_lower not in title.lower():
                    continue
                    
                if title:
                    integration = doc.get("integration_type", "unknown").upper()
                    url = doc.get("web_view_link") or doc.get("file_url") or "#"
                    
                    suggestions.append({
                        "text": title,
                        "type": "document",
                        "score": 9.0,  # High score for direct document matches
                        "icon": "📋",
                        "description": f"Document from {integration}",
                        "url": url,
                        "document_title": title  # Add for RBAC verification
                    })
                    
                    # Stop if we have enough suggestions
                    if len(suggestions) >= limit:
                        break
            
            return suggestions[:limit]
            
        except Exception as e:
            print(f"❌ Error getting document suggestions: {e}")
            return []
    
    def _get_autocomplete_predictions(self, partial_query: str, limit: int = 2) -> List[Dict]:
        """Get next-word predictions for autocomplete"""
        try:
            # Simple word-based predictions from search history
            words = partial_query.split()
            if not words:
                return []
            
            last_word = words[-1]
            
            # Find queries that start with our partial query
            cursor = self.search_analytics_collection.find(
                {"query": {"$regex": f"^{re.escape(partial_query)}", "$options": "i"}},
                limit=limit * 3,
                projection={"query": True}
            )
            
            suggestions = []
            seen_predictions = set()
            
            for search in cursor:
                query = search.get("query", "")
                if len(query) > len(partial_query):
                    # Extract the next word(s)
                    remaining = query[len(partial_query):].strip()
                    if remaining and remaining not in seen_predictions:
                        seen_predictions.add(remaining)
                        
                        prediction = partial_query + " " + remaining.split()[0]  # Just next word
                        suggestions.append({
                            "text": prediction,
                            "type": "prediction",
                            "score": 5.0,
                            "icon": "💡",
                            "description": "Autocomplete suggestion"
                        })
                        
                        if len(suggestions) >= limit:
                            break
            
            return suggestions
            
        except Exception as e:
            print(f"❌ Error getting autocomplete predictions: {e}")
            return []
    
    def _get_spell_correction_suggestions(self, partial_query: str, limit: int = 2) -> List[Dict]:
        """Get spell correction suggestions"""
        try:
            # Simple spell correction using common search terms
            words = partial_query.split()
            if not words:
                return []
            
            suggestions = []
            
            # Common typo corrections (you can expand this)
            typo_corrections = {
                "teh": "the",
                "adn": "and",
                "taht": "that",
                "thier": "their",
                "recieve": "receive",
                "seperate": "separate",
                "definately": "definitely",
                "occured": "occurred"
            }
            
            corrected_words = []
            has_correction = False
            
            for word in words:
                if word.lower() in typo_corrections:
                    corrected_words.append(typo_corrections[word.lower()])
                    has_correction = True
                else:
                    corrected_words.append(word)
            
            if has_correction:
                corrected_query = " ".join(corrected_words)
                suggestions.append({
                    "text": corrected_query,
                    "type": "correction",
                    "score": 4.0,
                    "icon": "✏️",
                    "description": "Did you mean?"
                })
            
            return suggestions[:limit]
            
        except Exception as e:
            print(f"❌ Error getting spell correction suggestions: {e}")
            return []
    
    def _find_document_for_query(self, query: str, user_email: str = None) -> Optional[Dict]:
        """Find the best matching document for a search query"""
        try:
            query_words = [word.lower().strip() for word in query.split() if len(word.strip()) > 2]
            if not query_words:
                return None
            
            # Search for documents that match the query
            base_filter = {"sync_status": True}
            
            # If user email provided, filter by access - support both legacy and new structure
            if user_email:
                base_filter["$or"] = [
                    {"user_emails_with_access": {"$in": [user_email.lower()]}},  # Legacy
                    {"access_control.authorized_emails": {"$in": [user_email.lower()]}}  # New structure
                ]
            
            cursor = self.docs_collection.find(
                base_filter,
                limit=50,  # Get reasonable sample for matching
                projection={
                    "title": True,
                    "content": True,
                    "searchable_text": True,
                    "integration_type": True,
                    "file_url": True,
                    "web_view_link": True,
                    "author_email": True,
                    "last_sync_updated": True
                }
            )
            
            documents = list(cursor)
            
            best_doc = None
            best_score = 0
            
            for doc in documents:
                title = doc.get("title", "").lower()
                content = doc.get("content", "").lower()
                searchable_text = doc.get("searchable_text", "").lower()
                
                # Calculate relevance score
                score = 0
                for word in query_words:
                    if word in title:
                        score += 10  # Title matches are most important
                    if word in content:
                        score += 2
                    if word in searchable_text:
                        score += 1
                
                # Prefer documents with real URLs
                if doc.get("web_view_link") or doc.get("file_url"):
                    score += 1
                
                if score > best_score:
                    best_score = score
                    best_doc = doc
            
            return best_doc
            
        except Exception as e:
            print(f"❌ Error finding document for query '{query}': {e}")
            return None
    
    def get_trending_searches(self, limit: int = 10) -> Dict:
        """Get trending searches from analytics data"""
        try:
            # Aggregate to find most popular searches in the last 30 days
            pipeline = [
                {
                    "$match": {
                        "searched_at": {"$gte": (datetime.now() - timedelta(days=30)).isoformat()}
                    }
                },
                {
                    "$group": {
                        "_id": "$query",
                        "search_count": {"$sum": 1},
                        "avg_results": {"$avg": "$results_count"},
                        "last_searched": {"$max": "$searched_at"}
                    }
                },
                {"$sort": {"search_count": -1}},
                {"$limit": limit}
            ]
            
            trending_data = []
            for result in self.search_analytics_collection.aggregate(pipeline):
                query = result.get("_id", "")
                search_count = result.get("search_count", 0)
                avg_results = result.get("avg_results", 0)
                last_searched = result.get("last_searched", "")
                
                # Find if there's a document associated with this query
                doc = self._find_document_for_query(query)
                has_document = doc is not None
                document_title = doc.get("title", "") if doc else ""
                
                trending_data.append({
                    "query": query,
                    "search_count": search_count,
                    "avg_results": round(avg_results, 1),
                    "last_searched": last_searched,
                    "has_document": has_document,
                    "document_title": document_title,
                    "score": search_count * 0.5 + avg_results * 0.3
                })
            
            return {
                "status": "success",
                "data": trending_data,
                "total": len(trending_data)
            }
            
        except Exception as e:
            print(f"❌ Error getting trending searches: {e}")
            return {
                "status": "error",
                "error": str(e),
                "data": [],
                "total": 0
            }
    
    def get_recent_searches(self, user_email: str, limit: int = 10) -> Dict:
        """Get recent searches for a specific user"""
        try:
            # Normalize email
            normalized_email = user_email.lower().strip()
            
            # Find recent searches for this user
            cursor = self.search_analytics_collection.find(
                {"user_email": normalized_email},
                sort=[("searched_at", -1)],
                limit=limit,
                projection={
                    "query": True,
                    "searched_at": True,
                    "results_count": True,
                    "integration_types": True
                }
            )
            
            recent_searches = []
            seen_queries = set()
            
            for search in cursor:
                query = search.get("query", "")
                if query and query not in seen_queries:
                    seen_queries.add(query)
                    
                    # Find if there's a document associated with this query
                    doc = self._find_document_for_query(query, user_email)
                    has_document = doc is not None
                    document_title = doc.get("title", "") if doc else ""
                    
                    recent_searches.append({
                        "query": query,
                        "searched_at": search.get("searched_at", ""),
                        "results_count": search.get("results_count", 0),
                        "integration_types": search.get("integration_types", []),
                        "has_document": has_document,
                        "document_title": document_title
                    })
            
            return {
                "status": "success",
                "data": recent_searches,
                "total": len(recent_searches)
            }
            
        except Exception as e:
            print(f"❌ Error getting recent searches for {user_email}: {e}")
            return {
                "status": "error",
                "error": str(e),
                "data": [],
                "total": 0
            }