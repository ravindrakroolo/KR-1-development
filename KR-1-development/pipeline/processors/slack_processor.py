import time
import json
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone

class SlackProcessor:
    """Slack connector"""
    
    def __init__(self, pipedream_connector, external_user_id, analytics_tracker, chunk_all=False):
        self.pipedream = pipedream_connector
        self.external_user_id = external_user_id
        self.account_id = "apn_MGh06ly"
        self.analytics = analytics_tracker
        self.max_retries = 3
        self.retry_delay = 2
        self.message_limit_per_conversation = 500
        self.chunk_all = chunk_all
    
    def connect(self) -> Tuple[bool, str]:
        """Connect to Slack"""
        print(f"🔧 Connecting to Slack (account: {self.account_id})")
        
        if not self.pipedream.authenticate():
            return False, "Pipedream authentication failed"
        
        test_response = self._make_request_with_retry(
            "https://slack.com/api/auth.test",
            "verify_slack_auth"
        )
        
        if test_response and test_response.get('status') == 'success':
            auth_data = test_response.get('data', {})
            if auth_data.get('ok'):
                team_name = auth_data.get('team', 'Unknown Team')
                print(f"✅ Connected to Slack team: {team_name}")
                return True, "Success"
        
        return False, "Slack authentication failed"
    
    def list_conversations(self, limit=None) -> List[Dict]:
        """List Slack conversations"""
        print(f"💬 Fetching Slack conversations (limit: {limit or 'unlimited'})")
        
        all_conversations = []
        next_cursor = None
        page_count = 0
        max_pages = 20 if limit is None else max(1, (limit // 100) + 1)
        
        while page_count < max_pages:
            page_size = 100 if limit is None else min(100, limit - len(all_conversations))
            if page_size <= 0:
                break
            
            api_url = "https://slack.com/api/conversations.list"
            params = {
                'limit': page_size,
                'types': 'public_channel,private_channel,mpim,im',
                'exclude_archived': 'true'
            }
            
            if next_cursor:
                params['cursor'] = next_cursor
            
            param_string = '&'.join([f"{k}={v}" for k, v in params.items()])
            full_url = f"{api_url}?{param_string}"
            
            response = self._make_request_with_retry(full_url, f"list_conversations_page_{page_count + 1}")
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                if data.get('ok'):
                    conversations = data.get('channels', [])
                    all_conversations.extend(conversations)
                    
                    print(f"📥 Page {page_count + 1}: {len(conversations)} conversations")
                    
                    response_metadata = data.get('response_metadata', {})
                    next_cursor = response_metadata.get('next_cursor')
                    page_count += 1
                    
                    if not next_cursor or (limit and len(all_conversations) >= limit):
                        break
                        
                    time.sleep(0.5)
                else:
                    break
            else:
                break
        
        if limit and len(all_conversations) > limit:
            all_conversations = all_conversations[:limit]
        
        print(f"✅ Retrieved {len(all_conversations)} conversations")
        return all_conversations
    
    def process_conversation(self, conversation_data: Dict) -> Tuple[Dict, Dict]:
        """Process Slack conversation with enhanced content extraction"""
        conv_id = conversation_data.get('id')
        conv_name = conversation_data.get('name', 'Unnamed Conversation')
        conv_type = self._get_conversation_type(conversation_data)
        
        processing_info = {
            'has_permissions': False,
            'has_content': False,
            'sync_successful': False,
            'sync_error': None,
            'debug_info': {}
        }
        
        print(f"📄 Processing {conv_type}: {conv_name} (ID: {conv_id})")
        
        # Get conversation members
        try:
            print(f"   👥 Fetching members for {conv_name}...")
            members = self.get_conversation_members(conv_id)
            if members:
                processing_info['has_permissions'] = True
                print(f"   ✅ Found {len(members)} members")
                processing_info['debug_info']['members_count'] = len(members)
            else:
                print(f"   ⚠️ No members found for {conv_name}")
                processing_info['debug_info']['members_count'] = 0
        except Exception as e:
            members = []
            error_msg = f"Member extraction failed: {e}"
            processing_info['sync_error'] = error_msg
            print(f"   ❌ Member extraction error: {e}")
            processing_info['debug_info']['member_error'] = str(e)
        
        # Get conversation messages
        try:
            print(f"   💬 Fetching messages for {conv_name}...")
            messages = self.get_conversation_messages(conv_id, self.message_limit_per_conversation)
            if messages:
                processing_info['has_content'] = True
                print(f"   ✅ Found {len(messages)} messages")
                processing_info['debug_info']['messages_count'] = len(messages)
                
                if messages:
                    sample_msg = messages[0]
                    sample_text = sample_msg.get('text', '')[:100]
                    print(f"   📝 Sample message: {sample_text}...")
                    processing_info['debug_info']['sample_message'] = sample_text
            else:
                print(f"   ⚠️ No messages found for {conv_name}")
                processing_info['debug_info']['messages_count'] = 0
        except Exception as e:
            messages = []
            error_msg = f"Message extraction failed: {e}"
            processing_info['sync_error'] = error_msg
            print(f"   ❌ Message extraction error: {e}")
            processing_info['debug_info']['message_error'] = str(e)
        
        # Aggregate messages into content
        try:
            print(f"   🔄 Aggregating content for {conv_name}...")
            content = self._aggregate_messages_to_content(messages, conv_name)
            
            if content and len(content.strip()) > 50:
                processing_info['has_content'] = True
                print(f"   ✅ Generated content: {len(content)} characters")
                processing_info['debug_info']['content_length'] = len(content)
            else:
                print(f"   ⚠️ Generated minimal content for {conv_name}")
                processing_info['debug_info']['content_length'] = len(content) if content else 0
                
            # If no real content, add some fallback content
            if not content or len(content.strip()) <= 50:
                fallback_content = f"Slack Channel: #{conv_name}\nChannel Type: {conv_type}\n"
                
                if conversation_data.get('topic', {}).get('value'):
                    fallback_content += f"Topic: {conversation_data['topic']['value']}\n"
                if conversation_data.get('purpose', {}).get('value'):
                    fallback_content += f"Purpose: {conversation_data['purpose']['value']}\n"
                
                fallback_content += f"Members: {len(members)} users\n"
                fallback_content += f"Channel ID: {conv_id}\n"
                fallback_content += "Note: Limited message history available for this channel."
                
                content = fallback_content
                print(f"   📋 Using fallback content: {len(content)} characters")
                processing_info['debug_info']['used_fallback'] = True
                
        except Exception as e:
            error_msg = f"Content aggregation failed: {e}"
            processing_info['sync_error'] = error_msg
            print(f"   ❌ Content aggregation error: {e}")
            processing_info['debug_info']['aggregation_error'] = str(e)
            
            # Fallback content
            content = f"Slack Channel: #{conv_name}\nError: Unable to load messages. {error_msg}"
        
        # Extract user emails from members
        user_emails = self._extract_member_emails(members)
        processing_info['debug_info']['user_emails_count'] = len(user_emails)
        
        # Determine author email
        author_email = self._extract_conversation_author(conversation_data, members, messages)
        if author_email:
            processing_info['debug_info']['author_found'] = True
        else:
            processing_info['debug_info']['author_found'] = False
            if user_emails:
                author_email = user_emails[0]
                print(f"   📧 Using first member as author: {author_email}")
        
        # Determine sync success
        has_meaningful_content = content and len(content.strip()) > 50
        has_access_info = author_email and user_emails
        
        if has_access_info and (processing_info['has_content'] or has_meaningful_content):
            processing_info['sync_successful'] = True
            processing_info['sync_error'] = None
            print(f"   ✅ Sync successful for {conv_name}")
        else:
            processing_info['sync_successful'] = False
            if not has_access_info:
                processing_info['sync_error'] = "Missing author or user access information"
            elif not has_meaningful_content:
                processing_info['sync_error'] = "Insufficient content extracted"
            print(f"   ⚠️ Sync incomplete for {conv_name}: {processing_info['sync_error']}")
        
        # Build Slack URLs
        file_url, web_view_link = self._build_slack_urls(conversation_data, conv_id)
        
        # Prepare enhanced document
        doc_data = {
            "id": f"slack_{conv_id}",
            "title": f"#{conv_name}" if conv_type == "channel" else conv_name,
            "content": content or "",
            "tool": "slack",
            "file_id": conv_id,
            "user_emails_with_access": user_emails,
            "created_at": self._format_timestamp(conversation_data.get('created')),
            "updated_at": self._format_timestamp(conversation_data.get('updated')),
            
            # Universal fields
            "sync_successful": processing_info['sync_successful'],
            "account_id": self.account_id,
            "external_user_id": self.external_user_id,
            "author_email": author_email,
            "sync_error": processing_info['sync_error'],
            "integration_type": "slack",
            "file_url": file_url,
            "web_view_link": web_view_link,
            
            "metadata": {
                "size": len(content) if content else 0,
                "mime_type": "application/slack-conversation",
                "path": f"#{conv_name}" if conv_type == "channel" else None,
                "permissions": members,
                "author": author_email,
                "type": conv_type,
                "has_content": processing_info['has_content'],
                "content_size": len(content) if content else 0,
                "sync_attempts": 1,
                "sync_error": processing_info['sync_error'],
                "original_links": {
                    "web_view_link": web_view_link,
                    "api_link": f"https://slack.com/api/conversations.info?channel={conv_id}"
                },
                "conversation_info": {
                    "is_channel": conversation_data.get('is_channel', False),
                    "is_group": conversation_data.get('is_group', False),
                    "is_im": conversation_data.get('is_im', False),
                    "is_mpim": conversation_data.get('is_mpim', False),
                    "is_private": conversation_data.get('is_private', False),
                    "topic": conversation_data.get('topic', {}).get('value', ''),
                    "purpose": conversation_data.get('purpose', {}).get('value', ''),
                    "message_count": len(messages),
                    "member_count": len(members)
                },
                "debug_info": processing_info['debug_info']
            }
        }
        
        self.analytics.track_content_processing(processing_info)
        
        print(f"   📊 Final status: {conv_name} - Content: {len(content)}chars, Members: {len(user_emails)}, Sync: {processing_info['sync_successful']}")
        
        return doc_data, processing_info
    
    def get_conversation_members(self, conversation_id: str) -> List[Dict]:
        """Get conversation members"""
        api_url = f"https://slack.com/api/conversations.members?channel={conversation_id}&limit=1000"
        
        response = self._make_request_with_retry(api_url, f"get_members_{conversation_id}")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            if data.get('ok'):
                member_ids = data.get('members', [])
                
                # Get user info for each member
                members_with_emails = []
                for member_id in member_ids:
                    user_info = self.get_user_info(member_id)
                    if user_info:
                        members_with_emails.append(user_info)
                
                return members_with_emails
        
        return []
    
    def get_user_info(self, user_id: str) -> Optional[Dict]:
        """Get user information"""
        api_url = f"https://slack.com/api/users.info?user={user_id}"
        
        response = self._make_request_with_retry(api_url, f"get_user_info_{user_id}")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            if data.get('ok'):
                user = data.get('user', {})
                profile = user.get('profile', {})
                
                return {
                    'id': user.get('id'),
                    'name': user.get('name'),
                    'real_name': user.get('real_name'),
                    'email': profile.get('email'),
                    'display_name': profile.get('display_name'),
                    'is_bot': user.get('is_bot', False)
                }
        
        return None
    
    def get_conversation_messages(self, conversation_id: str, limit: int = 200) -> List[Dict]:
        """Get conversation messages with enhanced content extraction"""
        print(f"   📥 Fetching messages from conversation: {conversation_id}")
        
        api_url = f"https://slack.com/api/conversations.history?channel={conversation_id}&limit={limit}&inclusive=true"
        
        response = self._make_request_with_retry(api_url, f"get_messages_{conversation_id}")
        
        if response and response.get('status') == 'success':
            data = response.get('data', {})
            if data.get('ok'):
                messages = data.get('messages', [])
                print(f"   📝 Raw messages retrieved: {len(messages)}")
                
                # Filter and process user messages
                processed_messages = []
                for msg in messages:
                    if (msg.get('type') == 'message' and 
                        msg.get('text')):
                        processed_messages.append(msg)
                
                print(f"   ✅ Processed messages: {len(processed_messages)}")
                return processed_messages
            else:
                error = data.get('error', 'Unknown error')
                print(f"   ❌ Slack API error: {error}")
        else:
            print(f"   ❌ Failed to fetch messages from conversation {conversation_id}")
        
        return []
    
    def _aggregate_messages_to_content(self, messages: List[Dict], conv_name: str) -> str:
        """Aggregate messages into comprehensive searchable content"""
        if not messages:
            print(f"   ⚠️ No messages found for {conv_name}")
            return f"Slack channel: {conv_name} - No recent messages available"
        
        print(f"   📝 Aggregating {len(messages)} messages for {conv_name}")
        
        # Sort messages by timestamp (oldest first for chronological order)
        try:
            sorted_messages = sorted(messages, key=lambda x: float(x.get('ts', 0)))
        except (ValueError, TypeError):
            sorted_messages = messages
        
        content_parts = [
            f"Slack Channel: #{conv_name}",
            f"Total Messages: {len(sorted_messages)}",
            f"Message History:",
            "=" * 50
        ]
        
        # Process messages
        message_count = 0
        for i, msg in enumerate(sorted_messages, 1):
            text = msg.get('text', '')
            user = msg.get('user', 'Unknown')
            subtype = msg.get('subtype', '')
            timestamp = msg.get('ts', '')
            
            if text and text.strip():
                clean_text = self._clean_slack_text(text)
                
                if len(clean_text.strip()) > 5:
                    message_count += 1
                    
                    # Format timestamp if available
                    time_str = ""
                    if timestamp:
                        try:
                            import datetime
                            dt = datetime.datetime.fromtimestamp(float(timestamp))
                            time_str = dt.strftime(" [%Y-%m-%d %H:%M]")
                        except:
                            time_str = f" [ts:{timestamp}]"
                    
                    # Add message with enhanced context
                    if subtype:
                        message_line = f"[{message_count}] {user} ({subtype}){time_str}: {clean_text}"
                    else:
                        message_line = f"[{message_count}] {user}{time_str}: {clean_text}"
                    
                    content_parts.append(message_line)
                    
                    # Add thread information if present
                    thread_ts = msg.get('thread_ts')
                    if thread_ts and thread_ts != timestamp:
                        content_parts.append(f"   └─ Thread reply")
                    
                    # Add reactions if present
                    reactions = msg.get('reactions', [])
                    if reactions:
                        reaction_names = [r.get('name', '') for r in reactions if r.get('name')]
                        if reaction_names:
                            content_parts.append(f"   Reactions: {', '.join(reaction_names)}")
                    
                    # Add file attachments info
                    files = msg.get('files', [])
                    if files:
                        file_names = [f.get('name', 'file') for f in files]
                        content_parts.append(f"   Files: {', '.join(file_names)}")
                    
                    # Limit to prevent overly long content
                    if message_count >= 150:
                        remaining = len(sorted_messages) - i
                        if remaining > 0:
                            content_parts.append(f"... and {remaining} more messages")
                        break
        
        # Add conversation metadata for enhanced searchability
        content_parts.extend([
            "=" * 50,
            f"Channel Summary:",
            f"Channel: #{conv_name}",
            f"Messages Processed: {message_count}/{len(sorted_messages)}",
            f"Unique Users: {len(set(msg.get('user', 'Unknown') for msg in sorted_messages if msg.get('user')))}",
            f"Content Keywords: slack, channel, conversation, {conv_name}, messages, chat"
        ])
        
        # Create comprehensive searchable content
        full_content = "\n".join(content_parts)
        
        # Extract common words from messages for search enhancement
        all_text = " ".join([msg.get('text', '') for msg in sorted_messages])
        clean_all_text = self._clean_slack_text(all_text)
        
        # Add most common words as keywords
        words = clean_all_text.lower().split()
        word_freq = {}
        for word in words:
            if len(word) > 3 and word.isalpha():
                word_freq[word] = word_freq.get(word, 0) + 1
        
        # Add top 10 most frequent words as keywords
        top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        keywords = [conv_name, "slack", "channel", "conversation", "messages"]
        keywords.extend([word for word, count in top_words if count > 2])
        
        # Combine full content with keywords for maximum searchability
        enhanced_content = f"{full_content}\n\nSEARCH_KEYWORDS: {' '.join(set(keywords))}"
        
        print(f"   ✅ Generated content: {len(enhanced_content)} characters, {message_count} messages")
        
        return enhanced_content
    
    def _clean_slack_text(self, text: str) -> str:
        """Clean Slack formatting while preserving maximum searchable content"""
        if not text:
            return ""
        
        original_text = text
        
        # Replace user mentions with readable format
        text = re.sub(r'<@([A-Z0-9]+)>', r'@user_\1', text)
        
        # Replace channel mentions with readable format
        text = re.sub(r'<#([A-Z0-9]+)\|([^>]+)>', r'#\2', text)
        text = re.sub(r'<#([A-Z0-9]+)>', r'#channel_\1', text)
        
        # Handle URLs but keep both URL and display text
        text = re.sub(r'<(https?://[^>|]+)\|([^>]+)>', r'\2 (\1)', text)
        text = re.sub(r'<(https?://[^>]+)>', r'URL: \1', text)
        
        # Handle special Slack formatting
        text = re.sub(r'<mailto:([^>|]+)\|([^>]+)>', r'\2 (email: \1)', text)
        text = re.sub(r'<mailto:([^>]+)>', r'email: \1', text)
        
        # Convert Slack markdown to readable format
        text = re.sub(r'\*([^*]+)\*', r'**\1**', text)
        text = re.sub(r'_([^_]+)_', r'*\1*', text)
        text = re.sub(r'`([^`]+)`', r'"\1"', text)
        text = re.sub(r'```([^`]+)```', r'CODE: \1', text)
        
        # Convert emoji codes to readable text
        text = re.sub(r':([a-zA-Z0-9_+-]+):', r'(\1)', text)
        
        # Handle Slack-specific syntax
        text = re.sub(r'<!([^>]+)>', r'@\1', text)
        
        # Clean excessive whitespace but preserve structure
        text = re.sub(r'\n\s*\n', '\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = text.strip()
        
        # If cleaning removed too much content, use gentler approach
        if len(text) < len(original_text) * 0.3:
            gentle_text = original_text
            gentle_text = re.sub(r'<@[A-Z0-9]+>', '@user', gentle_text)
            gentle_text = re.sub(r'<#[A-Z0-9]+\|([^>]+)>', r'#\1', gentle_text)
            gentle_text = re.sub(r'<(https?://[^>]+)>', r'[URL]', gentle_text)
            gentle_text = re.sub(r':([a-zA-Z0-9_+-]+):', r'(\1)', gentle_text)
            gentle_text = ' '.join(gentle_text.split())
            text = gentle_text
        
        return text
    
    def _extract_member_emails(self, members: List[Dict]) -> List[str]:
        """Extract email addresses from members"""
        emails = []
        for member in members:
            email = member.get('email')
            if email and '@' in email and not member.get('is_bot', False):
                emails.append(email.lower())
        return list(set(emails))
    
    def _extract_conversation_author(self, conversation_data: Dict, members: List[Dict], messages: List[Dict]) -> Optional[str]:
        """Extract conversation author/creator email"""
        # Try creator from conversation
        creator_id = conversation_data.get('creator')
        if creator_id:
            for member in members:
                if member.get('id') == creator_id:
                    email = member.get('email')
                    if email and '@' in email:
                        return email.lower()
        
        # Fallback to first message author
        if messages:
            first_msg_user = messages[-1].get('user')
            if first_msg_user:
                for member in members:
                    if member.get('id') == first_msg_user:
                        email = member.get('email')
                        if email and '@' in email:
                            return email.lower()
        
        # Final fallback
        for member in members:
            if not member.get('is_bot', False):
                email = member.get('email')
                if email and '@' in email:
                    return email.lower()
        
        return None
    
    def _get_conversation_type(self, conversation_data: Dict) -> str:
        """Determine conversation type"""
        if conversation_data.get('is_im'):
            return "direct_message"
        elif conversation_data.get('is_mpim'):
            return "group_message"
        elif conversation_data.get('is_private'):
            return "private_channel"
        elif conversation_data.get('is_channel'):
            return "channel"
        else:
            return "conversation"
    
    def _build_slack_urls(self, conversation_data: Dict, conv_id: str) -> Tuple[str, str]:
        """Build Slack URLs"""
        team_id = conversation_data.get('context_team_id') or "TEAM"
        
        file_url = f"https://app.slack.com/client/{team_id}/{conv_id}"
        web_view_link = file_url
        
        return file_url, web_view_link
    
    def _format_timestamp(self, timestamp) -> str:
        """Format timestamp to ISO"""
        if not timestamp:
            return datetime.now(timezone.utc).isoformat()
        
        try:
            if isinstance(timestamp, (int, float)):
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            else:
                dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
            return dt.isoformat()
        except (ValueError, TypeError):
            return datetime.now(timezone.utc).isoformat()
    
    def _make_request_with_retry(self, api_url: str, operation: str) -> Optional[Dict]:
        """Make Slack API request with retry"""
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