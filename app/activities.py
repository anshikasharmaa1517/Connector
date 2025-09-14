"""
This file contains the activities for the Elasticsearch metadata extraction application.

Note:
- Activities extend BaseMetadataExtractionActivities for HTTP-based data sources
- Following the same thin pattern as MySQL connector
- Custom logic for Elasticsearch-specific operations only
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from application_sdk.activities.common.models import ActivityStatistics
from application_sdk.activities.common.utils import auto_heartbeater
from application_sdk.activities.metadata_extraction.base import (
    BaseMetadataExtractionActivities,
)
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces
from application_sdk.services.secretstore import SecretStore
from temporalio import activity

from app.clients import ElasticsearchClient
from app.transformer import ElasticsearchAtlasTransformer
from app.handlers import ElasticsearchHandler

logger = get_logger(__name__)
activity.logger = logger
metrics = get_metrics()
traces = get_traces()


class ElasticsearchMetadataExtractionActivities(BaseMetadataExtractionActivities):
    """
    Activities for extracting metadata from Elasticsearch clusters.
    
    Following the MySQL pattern: thin activities that leverage framework capabilities.
    """

    def __init__(self, **kwargs):
        """Initialize Elasticsearch metadata extraction activities."""
        super().__init__(
            client_class=ElasticsearchClient,
            handler_class=ElasticsearchHandler,
            **kwargs
        )


    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def credential_extraction_demo_activity(
        self, workflow_args: Dict[str, Any]
    ) -> Optional[ActivityStatistics]:
        """A custom activity demonstrating credential extraction.

        Args:
            workflow_args: The workflow arguments.

        Returns:
            Optional[ActivityStatistics]: The activity statistics.
        """
        try:
            # Reference to credentials passed as user inputs are available as 'credential_guid' in workflow_args
            credential_guid = workflow_args.get("credential_guid")
            if not credential_guid:
                logger.error("No credential_guid provided in workflow_args")
                return None
            
            credentials = await SecretStore.get_credentials(credential_guid)
            if not credentials:
                logger.error("Failed to retrieve credentials from SecretStore")
                return None
                
            logger.info("Elasticsearch credentials retrieved successfully")
            return None
        except Exception as e:
            logger.error(f"Error in credential_extraction_demo_activity: {e}", exc_info=True)
            return None


    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def test_connection(
        self, workflow_args: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Test connection to Elasticsearch cluster.

        Args:
            workflow_args: The workflow arguments containing connection details.

        Returns:
            Dict[str, Any]: Connection test results with cluster information.
        """
        try:
            # Get credentials - either directly from workflow_args or from SecretStore
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided in workflow_args")
                    return {
                        "success": False,
                        "error": "No credentials or credential_guid provided for connection test"
                    }
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore")
                    return {
                        "success": False,
                        "error": "Failed to retrieve credentials from SecretStore"
                    }
                
                logger.info("Successfully retrieved credentials from SecretStore for connection test")
            
            logger.info(f"Testing connection with credentials: {list(credentials.keys())}")
            
            # Initialize client with credentials
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Test basic connectivity
            full_url = f"{client.base_url}/"
            logger.info(f"Making request to: {full_url}")
            logger.info(f"Client headers: {client.http_headers}")
            response = await client.execute_http_get_request(full_url)
            logger.info(f"Response received: {response is not None}")
            if response:
                logger.info(f"Response status: {response.status_code}")
            else:
                logger.error("No response received from execute_http_get_request")
            
            if response and response.status_code == 200:
                cluster_info = response.json()
                logger.info(f"Successfully connected to Elasticsearch cluster: {cluster_info.get('cluster_name', 'unknown')}")
                
                return {
                    "success": True,
                    "cluster_name": cluster_info.get("cluster_name", "unknown"),
                    "version": cluster_info.get("version", {}).get("number", "unknown"),
                    "message": f"Connected to {cluster_info.get('cluster_name', 'Elasticsearch')} ({cluster_info.get('version', {}).get('number', 'Unknown version')})"
                }
            else:
                error_msg = f"HTTP {response.status_code if response else 'No response'}"
                if response:
                    try:
                        error_detail = response.text
                        error_msg += f": {error_detail}"
                    except:
                        pass
                logger.error(f"Failed to connect to Elasticsearch: {error_msg}")
                return {
                    "success": False,
                    "error": error_msg
                }
                
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def preflight_check(self, workflow_args: Dict[str, Any]) -> Dict[str, Any]:
        """Perform preflight checks before starting metadata extraction.

        Args:
            workflow_args: The workflow arguments containing credentials.

        Returns:
            Dict containing the preflight check results.
        """
        try:
            # Get credentials from workflow args - handle nested structure
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided in workflow_args")
                    return {
                        "error": "No credentials provided",
                        "success": False
                    }
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore")
                    return {
                        "error": "Failed to retrieve credentials from SecretStore",
                        "success": False
                    }
                
                logger.info("Successfully retrieved credentials from SecretStore")
            
            # Initialize the client
            client = ElasticsearchClient()
            await client.load(credentials=credentials)

            # Test connectivity
            response = await client.execute_http_get_request(f"{client.base_url}/")
            if not response or response.status_code != 200:
                return {
                    "error": "Failed to connect to Elasticsearch",
                    "success": False
                }
            
            # Get cluster info for version check
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

        except Exception as e:
            logger.error(f"Preflight check failed: {e}", exc_info=True)
            return {
                "error": f"Preflight check failed: {str(e)}",
                "success": False
            }

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def extract_cluster_metadata(
        self, workflow_args: Dict[str, Any]
    ) -> ActivityStatistics:
        """Extract cluster metadata from Elasticsearch.

        Args:
            workflow_args: The workflow arguments.

        Returns:
            ActivityStatistics: The activity statistics.
        """
        try:
            # Get credentials from SecretStore using credential_guid
            credential_guid = workflow_args.get("credential_guid")
            if not credential_guid:
                logger.error("No credential_guid provided for metadata extraction")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="cluster"
                )
            
            credentials = await SecretStore.get_credentials(credential_guid)
            if not credentials:
                logger.error("Failed to retrieve credentials from SecretStore")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="cluster"
                )
            
            # Get connection info
            connection_name = workflow_args.get("connection_name", "Unknown")
            connection_description = workflow_args.get("connection_description", "")
            tags = workflow_args.get("tags", [])
            
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Get cluster info
            logger.info(f"Making request to cluster info endpoint: {client.base_url}/")
            response = await client.execute_http_get_request(f"{client.base_url}/")
            logger.info(f"Cluster info response: {response is not None}, status: {response.status_code if response else 'None'}")
            
            if response and response.status_code == 200:
                cluster_info = response.json()
                cluster_name = cluster_info.get('cluster_name', 'unknown')
                logger.info(f"Successfully extracted cluster metadata: {cluster_name}")
                
                # Get cluster health and stats
                health_response = await client.execute_http_get_request(f"{client.base_url}/_cluster/health")
                health_info = health_response.json() if health_response and health_response.status_code == 200 else {}
                
                # Get node information
                nodes_response = await client.execute_http_get_request(f"{client.base_url}/_cat/nodes?format=json")
                nodes_data = nodes_response.json() if nodes_response and nodes_response.status_code == 200 else []
                
                # Get indices info
                indices_response = await client.execute_http_get_request(f"{client.base_url}/_cat/indices?format=json&h=index,docs.count,store.size,creation.date.string")
                indices_data = indices_response.json() if indices_response and indices_response.status_code == 200 else []
                
                # Calculate totals
                total_docs = sum(int(idx.get('docs.count', 0)) for idx in indices_data if idx.get('docs.count', '0').isdigit())
                total_size = 0
                for idx in indices_data:
                    try:
                        store_size = idx.get('store.size', '0b')
                        if store_size and store_size != '-':
                            # Simple size parsing - just count bytes
                            if store_size.endswith('b'):
                                size_value = float(store_size[:-1])
                                if store_size.endswith('kb'):
                                    total_size += int(size_value * 1024)
                                elif store_size.endswith('mb'):
                                    total_size += int(size_value * 1024 * 1024)
                                elif store_size.endswith('gb'):
                                    total_size += int(size_value * 1024 * 1024 * 1024)
                                else:
                                    total_size += int(size_value)
                    except (ValueError, TypeError):
                        pass
                
                # Build cluster metadata
                cluster_metadata = {
                    "cluster_name": cluster_name,
                    "cluster_version": cluster_info.get('version', {}).get('number', 'unknown'),
                    "node_count": len(nodes_data),
                    "cluster_health": health_info.get('status', 'unknown'),
                    "cluster_owner": workflow_args.get("owner", "Unknown"),
                    "cluster_tags": tags,
                    "total_indices": len(indices_data),
                    "total_documents": total_docs,
                    "total_size_bytes": total_size,
                    "active_indices": len([idx for idx in indices_data if int(idx.get('docs.count', 0)) > 0]),
                    "empty_indices": len([idx for idx in indices_data if int(idx.get('docs.count', 0)) == 0]),
                    "connection_name": connection_name,
                    "connection_description": connection_description,
                    "extraction_timestamp": "2025-09-12T19:00:00Z",  # Fixed timestamp for Temporal determinism
                }
                
                logger.info(f"Extracted cluster metadata: {cluster_metadata['total_indices']} indices, {cluster_metadata['total_documents']} documents")
                
                # Store the metadata in JSON files (following MySQL pattern but with JSON instead of Parquet)
                await self._store_metadata_to_json(
                    metadata=cluster_metadata,
                    workflow_args=workflow_args,
                    output_suffix="raw/cluster",
                    typename="cluster"
                )
                
                return ActivityStatistics(
                    total_record_count=1 + len(indices_data),  # 1 cluster + N indices
                    chunk_count=1,  # Single chunk for cluster metadata
                    typename="cluster"
                )
            else:
                logger.error(f"Failed to get cluster info: {response.status_code if response else 'No response'}")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="cluster"
                )
                
        except Exception as e:
            logger.error(f"Cluster metadata extraction failed: {str(e)}", exc_info=True)
            return ActivityStatistics(
                total_record_count=0,
                chunk_count=0,
                typename="cluster"
            )

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def extract_cluster_info(self, workflow_args: Dict[str, Any]) -> ActivityStatistics:
        """Extract basic cluster information (name, version, health, nodes)."""
        try:
            # Get credentials from workflow args - handle nested structure
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided for cluster info extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="cluster"
                    )
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore for cluster info extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="cluster"
                    )
                
                logger.info("Successfully retrieved credentials from SecretStore for cluster info extraction")
            
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Get basic cluster info
            cluster_info = await client.execute_http_get_request(f"{client.base_url}/")
            cluster_health = await client.execute_http_get_request(f"{client.base_url}/_cluster/health")
            nodes_info = await client.execute_http_get_request(f"{client.base_url}/_cat/nodes?format=json")
            
            if cluster_info and cluster_info.status_code == 200:
                cluster_data = cluster_info.json()
                health_data = cluster_health.json() if cluster_health and cluster_health.status_code == 200 else {}
                nodes_data = nodes_info.json() if nodes_info and nodes_info.status_code == 200 else []
                
                # Extract basic cluster data
                cluster_info_data = {
                    "cluster_name": cluster_data.get("cluster_name", "unknown"),
                    "cluster_version": cluster_data.get("version", {}).get("number", "unknown"),
                    "cluster_health": health_data.get("status", "unknown"),
                    "node_count": len(nodes_data),
                    "cluster_owner": "Unknown",  # Could be extracted from cluster settings
                    "cluster_tags": [],  # Could be extracted from cluster settings
                    "total_size_bytes": 0,  # Will be calculated in extract_indices
                    "extraction_timestamp": datetime.now().isoformat()
                }
                
                # Store cluster info
                await self._store_metadata_to_json(
                    metadata=cluster_info_data,
                    workflow_args=workflow_args,
                    output_suffix="raw/cluster",
                    typename="cluster_info"
                )
                
                logger.info(f"Stored cluster info: {cluster_info_data['cluster_name']}")
                
                return ActivityStatistics(
                    total_record_count=1,
                    chunk_count=1,
                    typename="cluster"
                )
            else:
                logger.error(f"Failed to get cluster info: {cluster_info.status_code if cluster_info else 'No response'}")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="cluster"
                )
                
        except Exception as e:
            logger.error(f"Extract cluster info failed: {e}", exc_info=True)
            return ActivityStatistics(total_record_count=0, chunk_count=0, typename="cluster")

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def extract_indices(self, workflow_args: Dict[str, Any]) -> ActivityStatistics:
        """Extract index information (names, document counts, sizes)."""
        try:
            # Get credentials from workflow args - handle nested structure
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided for indices extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="indices"
                    )
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore for indices extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="indices"
                    )
                
                logger.info("Successfully retrieved credentials from SecretStore for indices extraction")
            
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Get indices info
            indices_response = await client.execute_http_get_request(f"{client.base_url}/_cat/indices?format=json&h=index,docs.count,store.size,creation.date.string")
            
            if indices_response and indices_response.status_code == 200:
                indices_data = indices_response.json()
                logger.info(f"Retrieved {len(indices_data)} indices from Elasticsearch")
                
                # Process indices data
                processed_indices = []
                total_documents = 0
                total_size_bytes = 0
                
                # Get include_system_indices setting from metadata
                metadata_config = workflow_args.get("metadata", {})
                include_system_indices = metadata_config.get("include_system_indices", False)
                logger.info(f"include_system_indices setting: {include_system_indices}")
                
                for idx in indices_data:
                    index_name = idx.get("index", "unknown")
                    logger.info(f"Processing index: {index_name}")
                    
                    # Skip system indices only if include_system_indices is False
                    if not include_system_indices and index_name.startswith("."):
                        logger.info(f"Skipping system index: {index_name}")
                        continue
                    
                    doc_count = int(idx.get("docs.count", 0))
                    size_str = idx.get("store.size", "0b")
                    size_bytes = self._parse_size_to_bytes(size_str)
                    
                    index_data = {
                        "index_name": index_name,
                        "document_count": doc_count,
                        "size_bytes": size_bytes,
                        "creation_date": idx.get("creation.date.string", "unknown"),
                        "extraction_timestamp": datetime.now().isoformat()
                    }
                    processed_indices.append(index_data)
                    logger.info(f"Added index: {index_name} with {doc_count} documents")
                    
                    total_documents += doc_count
                    total_size_bytes += size_bytes
                
                # Store indices data
                await self._store_metadata_to_json(
                    metadata=processed_indices,
                    workflow_args=workflow_args,
                    output_suffix="raw/indices",
                    typename="indices"
                )
                
                logger.info(f"Stored {len(processed_indices)} indices")
                logger.info(f"Total documents: {total_documents}, Total size: {total_size_bytes} bytes")
                
                return ActivityStatistics(
                    total_record_count=len(processed_indices),
                    chunk_count=1,
                    typename="indices"
                )
            else:
                logger.error(f"Failed to get indices info: {indices_response.status_code if indices_response else 'No response'}")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="indices"
                )
                
        except Exception as e:
            logger.error(f"Extract indices failed: {e}", exc_info=True)
            return ActivityStatistics(total_record_count=0, chunk_count=0, typename="indices")

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def extract_index_mappings(self, workflow_args: Dict[str, Any]) -> ActivityStatistics:
        """Extract field mappings for each index."""
        try:
            # Get credentials from workflow args - handle nested structure
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided for mappings extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="mappings"
                    )
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore for mappings extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="mappings"
                    )
                
                logger.info("Successfully retrieved credentials from SecretStore for mappings extraction")
            
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Get indices first
            indices_response = await client.execute_http_get_request(f"{client.base_url}/_cat/indices?format=json&h=index")
            
            if indices_response and indices_response.status_code == 200:
                indices_data = indices_response.json()
                indices = [idx.get("index", "") for idx in indices_data if not idx.get("index", "").startswith(".")]
                
                # Extract mappings for each index
                mappings_data = []
                for index_name in indices[:10]:  # Limit to first 10 indices for performance
                    try:
                        mapping_response = await client.execute_http_get_request(f"{client.base_url}/{index_name}/_mapping")
                        if mapping_response and mapping_response.status_code == 200:
                            mapping = mapping_response.json()
                            if index_name in mapping:
                                index_mapping = mapping[index_name].get("mappings", {})
                                
                                # Extract field information
                                fields = self._extract_fields_from_mapping(index_mapping)
                                
                                mapping_data = {
                                    "index_name": index_name,
                                    "field_count": len(fields),
                                    "fields": fields,
                                    "extraction_timestamp": datetime.now().isoformat()
                                }
                                mappings_data.append(mapping_data)
                                
                    except Exception as e:
                        logger.warning(f"Failed to extract mapping for index {index_name}: {e}")
                        continue
                
                # Store mappings data
                await self._store_metadata_to_json(
                    metadata=mappings_data,
                    workflow_args=workflow_args,
                    output_suffix="raw/mappings",
                    typename="mappings"
                )
                
                logger.info(f"Stored mappings for {len(mappings_data)} indices")
                
                return ActivityStatistics(
                    total_record_count=len(mappings_data),
                    chunk_count=1,
                    typename="mappings"
                )
            else:
                logger.error(f"Failed to get indices info: {indices_response.status_code if indices_response else 'No response'}")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="mappings"
                )
                
        except Exception as e:
            logger.error(f"Extract index mappings failed: {e}", exc_info=True)
            return ActivityStatistics(total_record_count=0, chunk_count=0, typename="mappings")

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def extract_index_settings(self, workflow_args: Dict[str, Any]) -> ActivityStatistics:
        """Extract settings for each index."""
        try:
            # Get credentials from workflow args - handle nested structure
            credentials = workflow_args.get("credentials", {})
            if not credentials and "workflow_args" in workflow_args:
                credentials = workflow_args["workflow_args"].get("credentials", {})
            
            # If no direct credentials, try to get from SecretStore using credential_guid
            if not credentials:
                credential_guid = workflow_args.get("credential_guid")
                if not credential_guid:
                    logger.error("No credentials or credential_guid provided for settings extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="settings"
                    )
                
                credentials = await SecretStore.get_credentials(credential_guid)
                if not credentials:
                    logger.error("Failed to retrieve credentials from SecretStore for settings extraction")
                    return ActivityStatistics(
                        total_record_count=0,
                        chunk_count=0,
                        typename="settings"
                    )
                
                logger.info("Successfully retrieved credentials from SecretStore for settings extraction")
            
            client = ElasticsearchClient()
            await client.load(credentials=credentials)
            
            # Get indices first
            indices_response = await client.execute_http_get_request(f"{client.base_url}/_cat/indices?format=json&h=index")
            
            if indices_response and indices_response.status_code == 200:
                indices_data = indices_response.json()
                indices = [idx.get("index", "") for idx in indices_data if not idx.get("index", "").startswith(".")]
                
                # Extract settings for each index
                settings_data = []
                for index_name in indices[:10]:  # Limit to first 10 indices for performance
                    try:
                        settings_response = await client.execute_http_get_request(f"{client.base_url}/{index_name}/_settings")
                        if settings_response and settings_response.status_code == 200:
                            settings = settings_response.json()
                            if index_name in settings:
                                index_settings = settings[index_name].get("settings", {})
                                
                                settings_data_item = {
                                    "index_name": index_name,
                                    "settings": index_settings,
                                    "extraction_timestamp": datetime.now().isoformat()
                                }
                                settings_data.append(settings_data_item)
                                
                    except Exception as e:
                        logger.warning(f"Failed to extract settings for index {index_name}: {e}")
                        continue
                
                # Store settings data
                await self._store_metadata_to_json(
                    metadata=settings_data,
                    workflow_args=workflow_args,
                    output_suffix="raw/settings",
                    typename="settings"
                )
                
                logger.info(f"Stored settings for {len(settings_data)} indices")
                
                return ActivityStatistics(
                    total_record_count=len(settings_data),
                    chunk_count=1,
                    typename="settings"
                )
            else:
                logger.error(f"Failed to get indices info: {indices_response.status_code if indices_response else 'No response'}")
                return ActivityStatistics(
                    total_record_count=0,
                    chunk_count=0,
                    typename="settings"
                )
                
        except Exception as e:
            logger.error(f"Extract index settings failed: {e}", exc_info=True)
            return ActivityStatistics(total_record_count=0, chunk_count=0, typename="settings")

    def _parse_size_to_bytes(self, size_str: str) -> int:
        """Parse Elasticsearch size string to bytes."""
        if not size_str or size_str == "0b":
            return 0
        
        size_str = size_str.lower()
        if size_str.endswith("kb"):
            return int(float(size_str[:-2]) * 1024)
        elif size_str.endswith("mb"):
            return int(float(size_str[:-2]) * 1024 * 1024)
        elif size_str.endswith("gb"):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024)
        elif size_str.endswith("tb"):
            return int(float(size_str[:-2]) * 1024 * 1024 * 1024 * 1024)
        else:
            return int(float(size_str))

    def _extract_fields_from_mapping(self, mapping: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract field information from Elasticsearch mapping."""
        fields = []
        
        def extract_fields_recursive(properties: Dict[str, Any], prefix: str = "") -> None:
            for field_name, field_config in properties.items():
                full_field_name = f"{prefix}.{field_name}" if prefix else field_name
                
                field_info = {
                    "field_name": full_field_name,
                    "field_type": field_config.get("type", "unknown"),
                    "analyzer": field_config.get("analyzer", "standard"),
                    "indexed": field_config.get("index", True),
                    "stored": field_config.get("store", False),
                    "nested": field_config.get("type") == "nested"
                }
                fields.append(field_info)
                
                # Recursively extract nested fields
                if "properties" in field_config:
                    extract_fields_recursive(field_config["properties"], full_field_name)
        
        if "properties" in mapping:
            extract_fields_recursive(mapping["properties"])
        
        return fields

    async def _store_metadata_to_json(
        self, 
        metadata: Dict[str, Any], 
        workflow_args: Dict[str, Any], 
        output_suffix: str,
        typename: str
    ):
        """Store metadata to JSON files following MySQL pattern.
        
        Args:
            metadata: The metadata to store
            workflow_args: Workflow arguments containing output paths
            output_suffix: Suffix for the output directory
            typename: Type name for the metadata
        """
        import json
        import os
        from datetime import datetime
        
        # Use the framework's built-in path resolution
        # The framework automatically sets up output_path in workflow_args
        output_path = workflow_args.get("output_path", "./output")
        output_dir = os.path.join(output_path, output_suffix)
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().isoformat().replace(":", "-")
        filename = f"{typename}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Write metadata to JSON file
        with open(filepath, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Stored {typename} metadata to: {filepath}")

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    async def get_workflow_args(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process workflow configuration and prepare workflow arguments.
        
        Args:
            workflow_config: The workflow configuration containing connection details.
            
        Returns:
            Dict containing processed workflow arguments with credentials.
        """
        try:
            logger.info(f"Processing workflow config: {workflow_config}")
            
            # If workflow_config is empty, use default values
            if not workflow_config:
                workflow_config = {}
            
            # Extract connection details from workflow config
            workflow_args = workflow_config.get("workflow_args", {})
            
            # Check if credentials are already provided in workflow_args
            if "credentials" in workflow_args:
                # Use the provided credentials directly
                credentials = workflow_args["credentials"]
                logger.info(f"Using provided credentials: {credentials}")
            else:
                # Set up credentials from individual connection details or use defaults
                credentials = {
                    "host": workflow_args.get("host", "localhost"),
                    "port": workflow_args.get("port", 9200),
                    "protocol": workflow_args.get("protocol", "http"),
                    "auth_type": workflow_args.get("auth_type", "none"),
                    "ssl_verify": workflow_args.get("ssl_verify", False),
                    "timeout": workflow_args.get("timeout", 30)
                }
                logger.info(f"Using default credentials: {credentials}")
            
            # Add credentials to workflow args
            workflow_args["credentials"] = credentials
            
            # Add metadata config if present
            if "metadata" in workflow_args:
                workflow_args["metadata"] = workflow_args["metadata"]
            else:
                workflow_args["metadata"] = {
                    "include_system_indices": True,
                    "extract_lineage": False,
                    "extract_quality_metrics": False
                }
            
            logger.info(f"Final workflow args: {workflow_args}")
            
            return workflow_args
            
        except Exception as e:
            logger.error(f"Error processing workflow args: {str(e)}")
            # Return a default configuration
            return {
                "credentials": {
                    "host": "localhost",
                    "port": 9200,
                    "protocol": "http",
                    "auth_type": "none",
                    "ssl_verify": False,
                    "timeout": 30
                },
                "metadata": {
                    "include_system_indices": True,
                    "extract_lineage": False,
                    "extract_quality_metrics": False
                }
            }

    @observability(logger=logger, metrics=metrics, traces=traces)
    @activity.defn
    @auto_heartbeater
    async def transform_data(self, workflow_args: Dict[str, Any]) -> ActivityStatistics:
        """Transform raw Elasticsearch metadata into Atlan entities."""
        try:
            # Get raw data path
            output_path = workflow_args.get("output_path", "./local/tmp/artifacts")
            workflow_id = workflow_args.get("workflow_id", "unknown")
            workflow_run_id = workflow_args.get("workflow_run_id", "unknown")
            
            # Extract connection qualified name from workflow_args
            connection_qualified_name = workflow_args.get("connection", {}).get("qualified_name", "default/atlan-connectors/elasticsearch")
            
            all_transformed_entities = []
            total_entities = 0
            
            # Transform cluster info
            cluster_entities = await self._transform_cluster_data(output_path, connection_qualified_name, workflow_run_id)
            all_transformed_entities.extend(cluster_entities)
            total_entities += len(cluster_entities)
            
            # Transform indices data
            index_entities = await self._transform_indices_data(output_path, connection_qualified_name, workflow_run_id)
            all_transformed_entities.extend(index_entities)
            total_entities += len(index_entities)
            
            # Transform mappings data
            mapping_entities = await self._transform_mappings_data(output_path, connection_qualified_name, workflow_run_id)
            all_transformed_entities.extend(mapping_entities)
            total_entities += len(mapping_entities)
            
            # Save all transformed data
            import os
            transformed_dir = f"{output_path}/transformed"
            os.makedirs(transformed_dir, exist_ok=True)
            
            transformed_file = f"{transformed_dir}/all_transformed_{workflow_run_id}.json"
            with open(transformed_file, 'w') as f:
                json.dump(all_transformed_entities, f, indent=2)
            
            logger.info(f"Transformed {total_entities} entities to {transformed_file}")
            
            return ActivityStatistics(
                total_record_count=total_entities,
                chunk_count=1,
                typename="mixed"
            )
            
        except Exception as e:
            logger.error(f"Transform data failed: {e}", exc_info=True)
            return ActivityStatistics(total_record_count=0, chunk_count=0, typename="mixed")

    async def _transform_cluster_data(self, output_path: str, connection_qualified_name: str, workflow_run_id: str) -> List[Dict[str, Any]]:
        """Transform cluster data into Atlan entities."""
        import glob
        import json
        
        cluster_files = glob.glob(f"{output_path}/**/raw/cluster/*.json", recursive=True)
        entities = []
        
        for file_path in cluster_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Handle both single cluster object and array of cluster objects
                if isinstance(data, list):
                    cluster_data_list = data
                else:
                    cluster_data_list = [data]
                
                for cluster_data in cluster_data_list:
                    entity = {
                        "typeName": "CLUSTER",
                        "attributes": {
                            "name": cluster_data.get("cluster_name", "unknown"),
                            "qualifiedName": f"{connection_qualified_name}/{cluster_data.get('cluster_name', 'unknown')}",
                            "connectionQualifiedName": connection_qualified_name,
                            "clusterName": cluster_data.get("cluster_name", "unknown"),
                            "clusterVersion": cluster_data.get("cluster_version", "unknown"),
                            "clusterStatus": cluster_data.get("cluster_health", "unknown"),
                            "numberOfNodes": cluster_data.get("node_count", 0),
                            "totalIndices": cluster_data.get("total_indices", 0),
                            "totalDocuments": cluster_data.get("total_documents", 0),
                        },
                        "customAttributes": {
                            "cluster_owner": cluster_data.get("cluster_owner", "Unknown"),
                            "cluster_tags": cluster_data.get("cluster_tags", []),
                            "total_size_bytes": cluster_data.get("total_size_bytes", 0),
                        },
                        "status": "ACTIVE"
                    }
                    entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Failed to transform cluster data from {file_path}: {e}")
                continue
        
        return entities

    async def _transform_indices_data(self, output_path: str, connection_qualified_name: str, workflow_run_id: str) -> List[Dict[str, Any]]:
        """Transform indices data into Atlan entities."""
        import glob
        import json
        
        indices_files = glob.glob(f"{output_path}/**/raw/indices/*.json", recursive=True)
        entities = []
        
        for file_path in indices_files:
            try:
                with open(file_path, 'r') as f:
                    indices_data = json.load(f)
                
                for index_data in indices_data:
                    entity = {
                        "typeName": "INDEX",
                        "attributes": {
                            "name": index_data.get("index_name", "unknown"),
                            "qualifiedName": f"{connection_qualified_name}/{index_data.get('index_name', 'unknown')}",
                            "connectionQualifiedName": connection_qualified_name,
                            "indexName": index_data.get("index_name", "unknown"),
                            "documentCount": index_data.get("document_count", 0),
                            "sizeBytes": index_data.get("size_bytes", 0),
                            "creationDate": index_data.get("creation_date", "unknown"),
                        },
                        "customAttributes": {
                            "extraction_timestamp": index_data.get("extraction_timestamp", ""),
                        },
                        "status": "ACTIVE"
                    }
                    entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Failed to transform indices data from {file_path}: {e}")
                continue
        
        return entities

    async def _transform_mappings_data(self, output_path: str, connection_qualified_name: str, workflow_run_id: str) -> List[Dict[str, Any]]:
        """Transform mappings data into Atlan entities."""
        import glob
        import json
        
        mappings_files = glob.glob(f"{output_path}/**/raw/mappings/*.json", recursive=True)
        entities = []
        
        for file_path in mappings_files:
            try:
                with open(file_path, 'r') as f:
                    mappings_data = json.load(f)
                
                for mapping_data in mappings_data:
                    # Create field entities for each field in the mapping
                    for field in mapping_data.get("fields", []):
                        entity = {
                            "typeName": "FIELD",
                            "attributes": {
                                "name": field.get("field_name", "unknown"),
                                "qualifiedName": f"{connection_qualified_name}/{mapping_data.get('index_name', 'unknown')}/{field.get('field_name', 'unknown')}",
                                "connectionQualifiedName": connection_qualified_name,
                                "fieldName": field.get("field_name", "unknown"),
                                "fieldType": field.get("field_type", "unknown"),
                                "analyzer": field.get("analyzer", "standard"),
                                "indexed": field.get("indexed", True),
                                "stored": field.get("stored", False),
                                "nested": field.get("nested", False),
                            },
                            "customAttributes": {
                                "index_name": mapping_data.get("index_name", "unknown"),
                                "field_count": mapping_data.get("field_count", 0),
                                "extraction_timestamp": mapping_data.get("extraction_timestamp", ""),
                            },
                            "status": "ACTIVE"
                        }
                        entities.append(entity)
                    
            except Exception as e:
                logger.warning(f"Failed to transform mappings data from {file_path}: {e}")
                continue
        
        return entities