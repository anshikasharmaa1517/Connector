"""
This file contains the client for the Elasticsearch metadata extraction application.

Note:
- The ES_CONFIG is configured to setup the connection parameters for the Elasticsearch cluster.
"""

from typing import Any, Dict

from application_sdk.clients.base import BaseClient
from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)


class ElasticsearchClient(BaseClient):
    """
    This client handles connection configuration and authentication
    for Elasticsearch clusters using HTTP-based operations.
    """

    ES_CONFIG = {
        "default_port": 9200,
        "default_protocol": "http",  # HTTP for local development
        "supported_auth_types": ["basic", "api_key", "bearer"],
        "required_basic": ["host", "username", "password"],
        "required_api_key": ["host", "api_key_id", "api_key"],
        "required_bearer": ["host", "token"],
    }

    async def load(self, **kwargs: Any) -> None:
        """
        Initialize the Elasticsearch client with credentials.
        
        Args:
            **kwargs: Credentials and configuration parameters
        """
        credentials = kwargs.get("credentials", {})
        
        # Extract connection parameters
        host = credentials.get("host", "localhost")
        port = credentials.get("port", self.ES_CONFIG["default_port"])
        protocol = credentials.get("protocol", self.ES_CONFIG["default_protocol"])
        auth_type = credentials.get("auth_type", "basic")
        
        # Build base URL
        self.base_url = f"{protocol}://{host}:{port}"
        
        # Set up authentication headers based on auth type
        # Only set auth headers if credentials are provided
        if auth_type == "basic":
            username = credentials.get("username", "")
            password = credentials.get("password", "")
            if username and password:
                import base64
                auth_string = base64.b64encode(f"{username}:{password}".encode()).decode()
                self.http_headers = {"Authorization": f"Basic {auth_string}"}
            else:
                # No authentication - cluster has security disabled
                self.http_headers = {}
        elif auth_type == "api_key":
            api_key_id = credentials.get("api_key_id", "")
            api_key = credentials.get("api_key", "")
            if api_key_id and api_key:
                import base64
                auth_string = base64.b64encode(f"{api_key_id}:{api_key}".encode()).decode()
                self.http_headers = {"Authorization": f"ApiKey {auth_string}"}
            else:
                self.http_headers = {}
        elif auth_type == "bearer":
            token = credentials.get("token", "")
            if token:
                self.http_headers = {"Authorization": f"Bearer {token}"}
            else:
                self.http_headers = {}
        else:
            # No authentication
            self.http_headers = {}
        
        # Set SSL verification
        ssl_verify = credentials.get("ssl_verify", True)
        
        # Configure HTTP transport with SSL verification
        import httpx
        if ssl_verify:
            # Use default transport with SSL verification enabled
            self.http_retry_transport = httpx.AsyncHTTPTransport()
        else:
            # Disable SSL verification for development
            self.http_retry_transport = httpx.AsyncHTTPTransport(verify=False)
        
        logger.info(f"Elasticsearch client initialized for {self.base_url} with {auth_type} auth, SSL verify: {ssl_verify}")