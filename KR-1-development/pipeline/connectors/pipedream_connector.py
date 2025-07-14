import requests
import base64
from typing import Dict, Any, Optional

class PipedreamConnector:
    def __init__(self, client_id, client_secret, project_id, base_url="https://api.pipedream.com/v1", external_user_id=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.project_id = project_id
        self.base_url = base_url
        self.external_user_id = external_user_id or "kroolo-developer"
        self.access_token = None
        
        if not self.client_secret:
            raise ValueError("Missing PIPEDREAM_CLIENT_SECRET in environment variables")
    
    def authenticate(self):
        """Authenticate with Pipedream using OAuth 2.0"""
        auth_url = f"{self.base_url}/oauth/token"
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        try:
            response = requests.post(auth_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get('access_token')
            
            if not self.access_token:
                raise Exception("No access token received")
            
            print("✅ Pipedream authenticated")
            return True
            
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            return False
    
    def get_account_by_id(self, target_account_id: str):
        """Get specific account by ID"""
        try:
            url = f"{self.base_url}/connect/{self.project_id}/accounts"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'X-PD-Environment': 'development'
            }
            params = {'include_credentials': 'true'}
            
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                accounts = data.get('data', [])
                
                for account in accounts:
                    if account.get('id') == target_account_id:
                        print(f"✅ Found target account: {target_account_id}")
                        return account
                
                print(f"❌ Account {target_account_id} not found")
                return None
            else:
                print(f"❌ API call failed: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Error getting account: {e}")
            return None
    
    def make_api_request(self, account_id, external_user_id, url, method='GET', params=None, data=None):
        """Make authenticated API request through Pipedream proxy"""
        try:
            import base64
            
            url_encoded = base64.urlsafe_b64encode(url.encode()).decode().rstrip('=')
            proxy_url = f"{self.base_url}/connect/{self.project_id}/proxy/{url_encoded}"
            
            query_params = {
                'external_user_id': external_user_id,
                'account_id': account_id
            }
            
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json',
                'X-PD-Environment': 'development'
            }
            
            if method.upper() == 'GET':
                response = requests.get(proxy_url, headers=headers, params=query_params)
            else:
                response = requests.post(proxy_url, headers=headers, params=query_params, json=data)
            
            if response.status_code == 200:
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type:
                    return {'status': 'success', 'data': response.json()}
                else:
                    return {'status': 'success', 'data': response.text, 'content_type': content_type}
            else:
                return {'status': 'error', 'message': f"HTTP {response.status_code}: {response.text}"}
                
        except Exception as e:
            return {'status': 'error', 'message': str(e)}