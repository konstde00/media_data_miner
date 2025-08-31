import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Dict, Any, List
from .config import SETTINGS


class RequestHandler:
    def __init__(self):
        self.current_proxy_index = 0
        self.current_ua_index = 0
        self.session = self._create_session_with_retries()
    
    def _create_session_with_retries(self) -> requests.Session:
        """Create session with connection pooling and retry strategy"""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        # Create adapter with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10
        )
        
        # Mount adapter for both protocols
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def __del__(self):
        """Clean up session on deletion"""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
            except:
                pass
    
    def __enter__(self):
        """Context manager support"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up on context exit"""
        self.close()
        return False
    
    def close(self):
        """Explicitly close the session"""
        if hasattr(self, 'session') and self.session:
            try:
                self.session.close()
            except:
                pass
        
    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """Get the next proxy from the pool"""
        if not SETTINGS.enable_proxy_rotation or not SETTINGS.proxy_pool:
            return None
            
        proxy_url = SETTINGS.proxy_pool[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(SETTINGS.proxy_pool)
        
        return {
            "http": proxy_url,
            "https": proxy_url
        }
    
    def get_next_user_agent(self) -> str:
        """Get the next user agent from the rotation"""
        ua = SETTINGS.user_agents[self.current_ua_index]
        self.current_ua_index = (self.current_ua_index + 1) % len(SETTINGS.user_agents)
        return ua
    
    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None, 
                    max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Make a request with retry logic, proxy rotation, and rate limiting"""
        
        for attempt in range(max_retries):
            try:
                # Prepare headers
                headers = {
                    "User-Agent": self.get_next_user_agent()
                }
                
                # Get proxy if enabled
                proxies = self.get_next_proxy()
                
                # Make request with rate limiting
                if attempt > 0:  # Add exponential backoff on retries
                    time.sleep(2 ** attempt)
                else:
                    time.sleep(SETTINGS.request_delay)
                
                # Add response size limit via stream
                response = self.session.get(
                    url, 
                    headers=headers, 
                    proxies=proxies, 
                    params=params or {},
                    timeout=30,
                    stream=True
                )
                
                # Check content length before reading
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB limit
                    response.close()
                    print(f"Response too large: {int(content_length) / 1024 / 1024:.1f} MB")
                    return None
                
                # Read the response content
                response._content = response.raw.read(50 * 1024 * 1024)  # Max 50MB
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limited - honor Retry-After header if present
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        try:
                            # Could be seconds (int) or HTTP date string
                            wait_time = int(retry_after)
                        except ValueError:
                            # If it's a date string, default to 60 seconds
                            wait_time = 60
                        # Cap at 5 minutes maximum
                        wait_time = min(wait_time, 300)
                        print(f"Rate limited, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        time.sleep(60)  # Wait 1 minute if no header
                    continue
                elif response.status_code in [403, 503]:
                    print(f"Blocked (HTTP {response.status_code}), trying different proxy/UA...")
                    continue
                else:
                    print(f"HTTP {response.status_code}: {response.text[:200]}")
                    continue
                    
            except requests.RequestException as e:
                print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    continue
                    
        return None


def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary with fallback"""
    try:
        return data.get(key, default)
    except (KeyError, AttributeError):
        return default


def clean_text(text: str) -> str:
    """Clean and normalize text content"""
    if not text:
        return ""
    return text.replace('\x00', '').replace('\r\n', '\n').strip()


def validate_post_data(post: Dict[str, Any]) -> bool:
    """Validate that post data contains required fields"""
    required_fields = ['id', 'title', 'created_utc', 'subreddit']
    return all(field in post for field in required_fields)


def extract_post_fields(post_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all relevant fields from Reddit post JSON"""
    return {
        # Basic fields
        "id": safe_get(post_data, "id"),
        "title": clean_text(safe_get(post_data, "title", "")),
        "author": safe_get(post_data, "author", "[deleted]"),
        "selftext": clean_text(safe_get(post_data, "selftext", "")),
        "score": safe_get(post_data, "score", 0),
        "ups": safe_get(post_data, "ups", 0),
        "downs": safe_get(post_data, "downs", 0),
        "num_comments": safe_get(post_data, "num_comments", 0),
        "created_utc": safe_get(post_data, "created_utc", 0),
        "subreddit": safe_get(post_data, "subreddit"),
        "permalink": f"https://reddit.com{safe_get(post_data, 'permalink', '')}",
        "url": safe_get(post_data, "url", ""),
        
        # Additional fields
        "is_self": safe_get(post_data, "is_self", False),
        "over_18": safe_get(post_data, "over_18", False),
        "stickied": safe_get(post_data, "stickied", False),
        "flair": safe_get(post_data, "link_flair_text"),
        "upvote_ratio": safe_get(post_data, "upvote_ratio"),
        "thumbnail": safe_get(post_data, "thumbnail"),
        "domain": safe_get(post_data, "domain"),
        "gilded": safe_get(post_data, "gilded", 0),
        "total_awards_received": safe_get(post_data, "total_awards_received", 0),
        "is_video": safe_get(post_data, "is_video", False),
        "locked": safe_get(post_data, "locked", False),
        "spoiler": safe_get(post_data, "spoiler", False),
        "hidden": safe_get(post_data, "hidden", False),
        "archived": safe_get(post_data, "archived", False),
        
        # Media fields
        "media": safe_get(post_data, "media"),
        "media_embed": safe_get(post_data, "media_embed"),
        "preview": safe_get(post_data, "preview"),
        
        # Metadata
        "distinguished": safe_get(post_data, "distinguished"),
        "edited": safe_get(post_data, "edited", False),
        "subreddit_name_prefixed": safe_get(post_data, "subreddit_name_prefixed"),
        "subreddit_type": safe_get(post_data, "subreddit_type"),
        "subreddit_id": safe_get(post_data, "subreddit_id"),
        "author_fullname": safe_get(post_data, "author_fullname"),
        
        # Engagement metrics
        "view_count": safe_get(post_data, "view_count"),
        "visited": safe_get(post_data, "visited", False),
        "saved": safe_get(post_data, "saved", False),
        "clicked": safe_get(post_data, "clicked", False),
    }


def extract_comment_fields(comment_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all relevant fields from Reddit comment JSON"""
    return {
        "id": safe_get(comment_data, "id"),
        "author": safe_get(comment_data, "author", "[deleted]"),
        "body": clean_text(safe_get(comment_data, "body", "")),
        "score": safe_get(comment_data, "score", 0),
        "ups": safe_get(comment_data, "ups", 0),
        "downs": safe_get(comment_data, "downs", 0),
        "created_utc": safe_get(comment_data, "created_utc", 0),
        "parent_id": safe_get(comment_data, "parent_id"),
        "link_id": safe_get(comment_data, "link_id"),
        "subreddit": safe_get(comment_data, "subreddit"),
        "permalink": f"https://reddit.com{safe_get(comment_data, 'permalink', '')}",
        
        # Additional fields
        "gilded": safe_get(comment_data, "gilded", 0),
        "total_awards_received": safe_get(comment_data, "total_awards_received", 0),
        "distinguished": safe_get(comment_data, "distinguished"),
        "edited": safe_get(comment_data, "edited", False),
        "stickied": safe_get(comment_data, "stickied", False),
        "is_submitter": safe_get(comment_data, "is_submitter", False),
        "score_hidden": safe_get(comment_data, "score_hidden", False),
        "archived": safe_get(comment_data, "archived", False),
        "author_fullname": safe_get(comment_data, "author_fullname"),
        "subreddit_id": safe_get(comment_data, "subreddit_id"),
        "controversiality": safe_get(comment_data, "controversiality", 0),
        "depth": safe_get(comment_data, "depth", 0),
    }