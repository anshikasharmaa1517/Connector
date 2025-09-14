"""
This file contains the transformer for the Elasticsearch metadata extraction application.
The transformer is responsible for transforming the raw metadata into the Atlan Type.

Following the MySQL pattern: thin transformer extending AtlasTransformer with ES-specific entity mappings.
"""

from typing import Any, Dict

from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.transformers.atlas import AtlasTransformer
from application_sdk.transformers.common.utils import build_atlas_qualified_name

logger = get_logger(__name__)


class ElasticsearchCluster:
    """Represents an Elasticsearch cluster entity in Atlan.

    This class handles the transformation of Elasticsearch cluster metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Elasticsearch cluster metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Elasticsearch cluster metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        attributes = {
            "name": obj.get("cluster_name", ""),
            "qualifiedName": obj.get("connection_qualified_name", ""),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
        }
        
        # Add Elasticsearch-specific custom attributes
        custom_attributes = {
            "elasticsearch_version": obj.get("version", {}).get("number", ""),
            "cluster_uuid": obj.get("cluster_uuid", ""),
            "cluster_status": obj.get("health", {}).get("status", ""),
            "number_of_nodes": str(obj.get("health", {}).get("number_of_nodes", 0)),
            "number_of_data_nodes": str(obj.get("health", {}).get("number_of_data_nodes", 0)),
        }
        
        return {
            "attributes": attributes,
            "custom_attributes": custom_attributes,
        }


class ElasticsearchIndex:
    """Represents an Elasticsearch index entity in Atlan.

    This class handles the transformation of Elasticsearch index metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Elasticsearch index metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Elasticsearch index metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        index_name = obj.get("name", "")
        
        attributes = {
            "name": index_name,
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""), index_name
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
        }
        
        # Add Elasticsearch-specific custom attributes
        custom_attributes = {
            "index_health": obj.get("health", ""),
            "index_status": obj.get("status", ""),
            "document_count": str(obj.get("document_count", 0)),
            "primary_size_bytes": str(obj.get("primary_size_bytes", 0)),
            "total_size_bytes": str(obj.get("total_size_bytes", 0)),
            "primary_shards": str(obj.get("primary_shards", 0)),
            "replica_shards": str(obj.get("replica_shards", 0)),
            "index_uuid": obj.get("uuid", ""),
        }
        
        return {
            "attributes": attributes,
            "custom_attributes": custom_attributes,
        }


class ElasticsearchField:
    """Represents an Elasticsearch field entity in Atlan.

    This class handles the transformation of Elasticsearch field metadata into Atlan entity format.
    """

    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Elasticsearch field metadata into Atlan entity attributes.

        Args:
            obj: Dictionary containing the raw Elasticsearch field metadata.

        Returns:
            Dict[str, Any]: Dictionary containing the transformed attributes and custom attributes.
        """
        field_name = obj.get("field_name", "")
        index_name = obj.get("index_name", "")
        
        attributes = {
            "name": field_name,
            "qualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""), index_name, field_name
            ),
            "connectionQualifiedName": obj.get("connection_qualified_name", ""),
            "indexQualifiedName": build_atlas_qualified_name(
                obj.get("connection_qualified_name", ""), index_name
            ),
        }
        
        # Add Elasticsearch-specific custom attributes
        custom_attributes = {
            "field_type": obj.get("field_type", ""),
            "analyzer": obj.get("analyzer", ""),
            "is_searchable": str(obj.get("index", True)),
            "is_stored": str(obj.get("store", False)),
            "is_nested": str(obj.get("is_nested", False)),
            "has_multi_fields": str(obj.get("has_multi_fields", False)),
        }
        
        return {
            "attributes": attributes,
            "custom_attributes": custom_attributes,
        }


class ElasticsearchAtlasTransformer(AtlasTransformer):
    """Transformer for converting Elasticsearch metadata into Atlas entities.
    
    Following the MySQL pattern: extends AtlasTransformer and provides
    Elasticsearch-specific entity class definitions.
    """

    def __init__(self, connector_name: str, tenant_id: str, **kwargs: Any):
        """Initialize the Elasticsearch Atlas transformer.
        
        Args:
            connector_name: Name of the connector
            tenant_id: Tenant identifier
            **kwargs: Additional arguments passed to parent class
        """
        super().__init__(connector_name, tenant_id, **kwargs)
        
        # Add Elasticsearch-specific entity mappings
        self.entity_class_definitions["CLUSTER"] = ElasticsearchCluster
        self.entity_class_definitions["INDEX"] = ElasticsearchIndex
        self.entity_class_definitions["FIELD"] = ElasticsearchField
