"""
This file contains the handlers for the Elasticsearch metadata extraction application.

Note:
- Handlers extend BaseHandler for HTTP-based data sources
- Follows the same pattern as MySQL connector but for REST/HTTP operations
- Custom logic for Elasticsearch-specific operations only
"""

from typing import Any, Dict

from application_sdk.handlers.base import BaseHandler
from application_sdk.observability.logger_adaptor import get_logger

logger = get_logger(__name__)


class ElasticsearchHandler(BaseHandler):
    """
    Handler for Elasticsearch metadata extraction operations.

    This handler provides Elasticsearch-specific implementations for
    authentication, preflight checks, and metadata fetching.
    """

    async def load(self, credentials: Dict[str, Any]) -> None:
        """Load credentials into the client.

        Args:
            credentials: Dictionary containing connection credentials.
        """
        if self.client:
            await self.client.load(credentials=credentials)

    async def test_auth(self, **kwargs) -> bool:
        """Test authentication to Elasticsearch cluster.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        try:
            logger.info("Testing Elasticsearch authentication")
            
            # Test basic connectivity
            response = await self.client.execute_http_get_request(f"{self.client.base_url}/")
            
            if response and response.status_code == 200:
                cluster_info = response.json()
                logger.info(f"Successfully authenticated to Elasticsearch cluster: {cluster_info.get('cluster_name', 'unknown')}")
                return True
            else:
                logger.error(f"Authentication failed with status: {response.status_code if response else 'No response'}")
                return False
                
        except Exception as e:
            logger.error(f"Authentication test failed: {str(e)}", exc_info=True)
            return False

    async def preflight_check(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Perform preflight checks for Elasticsearch connection.

        Args:
            payload: Configuration dictionary containing metadata and other settings.

        Returns:
            Dict[str, Any]: Preflight check results.
        """
        try:
            logger.info("Starting Elasticsearch preflight check")
            
            # Test connectivity
            connectivity_result = await self.test_auth()
            
            if not connectivity_result:
                return {
                    "error": "Failed to connect to Elasticsearch cluster",
                    "success": False
                }
            
            # Get cluster info for version check
            response = await self.client.execute_http_get_request(f"{self.client.base_url}/")
            if response and response.status_code == 200:
                cluster_info = response.json()
                cluster_name = cluster_info.get('cluster_name', 'unknown')
                version = cluster_info.get('version', {}).get('number', 'unknown')
                
                logger.info(f"Preflight check successful for cluster: {cluster_name} (version: {version})")
                
                return {
                    "success": True,
                    "data": {
                        "connectivityCheck": {
                            "success": True,
                            "message": f"Connected to {cluster_name} cluster"
                        },
                        "versionCheck": {
                            "success": True,
                            "message": f"Elasticsearch version: {version}"
                        }
                    }
                }
            else:
                return {
                    "error": "Failed to retrieve cluster information",
                    "success": False
                }
                
        except Exception as e:
            logger.error(f"Preflight check failed: {str(e)}", exc_info=True)
            return {
                "error": str(e),
                "success": False
            }

    async def fetch_metadata(self, **kwargs) -> Dict[str, Any]:
        """Fetch metadata from Elasticsearch cluster.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Fetched metadata.
        """
        try:
            logger.info("Fetching Elasticsearch metadata")
            
            # This method is called by the /workflow/v1/metadata endpoint
            # For now, return a placeholder response
            return {
                "success": True,
                "message": "Metadata fetching not implemented yet",
                "data": {}
            }
            
        except Exception as e:
            logger.error(f"Metadata fetching failed: {str(e)}", exc_info=True)
            return {
                "error": str(e),
                "success": False
            }
