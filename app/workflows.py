"""
This file contains the workflow definition for the Elasticsearch metadata extraction application.

Note:
The workflow extends WorkflowInterface since there's no BaseRESTMetadataExtractionWorkflow yet
"""

import asyncio
from datetime import timedelta
from typing import Any, Callable, Dict, List

from app.activities import ElasticsearchMetadataExtractionActivities
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces
from application_sdk.workflows import WorkflowInterface
from temporalio import workflow
from temporalio.common import RetryPolicy

logger = get_logger(__name__)
workflow.logger = logger
metrics = get_metrics()
traces = get_traces()


@workflow.defn
class ElasticsearchMetadataExtractionWorkflow(WorkflowInterface):
    """
    Workflow for extracting metadata from Elasticsearch clusters.    
    """

    activities_cls = ElasticsearchMetadataExtractionActivities

    @observability(logger=logger, metrics=metrics, traces=traces)
    @workflow.run
    async def run(self, workflow_config: Dict[str, Any]):
        """
        Run the workflow.

        :param workflow_config: The workflow configuration.
        """
        activities_instance = ElasticsearchMetadataExtractionActivities()

        # Let the framework handle workflow args setup (includes output paths)
        workflow_args: Dict[str, Any] = await workflow.execute_activity_method(
            activities_instance.get_workflow_args,
            workflow_config,
            start_to_close_timeout=timedelta(seconds=10),
        )

        retry_policy = RetryPolicy(
            maximum_attempts=6,
            backoff_coefficient=2,
        )

        await workflow.execute_activity_method(
            activities_instance.preflight_check,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )

        await workflow.execute_activity_method(
            activities_instance.credential_extraction_demo_activity,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )

        # Extract cluster information
        cluster_info_stats = await workflow.execute_activity_method(
            activities_instance.extract_cluster_info,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )
        
        # Extract indices information
        indices_stats = await workflow.execute_activity_method(
            activities_instance.extract_indices,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )
        
        # Extract index mappings
        mappings_stats = await workflow.execute_activity_method(
            activities_instance.extract_index_mappings,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )
        
        # Extract index settings
        settings_stats = await workflow.execute_activity_method(
            activities_instance.extract_index_settings,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )
        
        # Transform raw data into Atlan entities
        transform_stats = await workflow.execute_activity_method(
            activities_instance.transform_data,
            workflow_args,
            retry_policy=retry_policy,
            start_to_close_timeout=timedelta(seconds=3600),
            heartbeat_timeout=timedelta(seconds=300),
        )
        
        logger.info(f"Metadata extraction and transformation completed: cluster_info={cluster_info_stats}, indices={indices_stats}, mappings={mappings_stats}, settings={settings_stats}, transform={transform_stats}")

    @staticmethod
    def get_activities(
        activities: ElasticsearchMetadataExtractionActivities,
    ) -> List[Callable[..., Any]]:
        """Get the list of activities for the workflow.

        Args:
            activities: The activities instance containing the workflow activities.

        Returns:
            List[Callable[..., Any]]: A list of activity methods that can be executed by the workflow.
        """
        return [
            activities.get_workflow_args,
            activities.preflight_check,
            activities.credential_extraction_demo_activity,
            activities.extract_cluster_info,
            activities.extract_indices,
            activities.extract_index_mappings,
            activities.extract_index_settings,
            activities.extract_cluster_metadata,  # Keep for backward compatibility
            activities.transform_data,
            activities.test_connection,
        ]