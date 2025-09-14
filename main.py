"""
Main entry point for the Elasticsearch metadata extraction application.

This module initializes and runs the Elasticsearch metadata extraction application,
setting up the workflow, worker, and server components using the Atlan framework.
"""

import asyncio
from typing import Dict, Any

from app.activities import ElasticsearchMetadataExtractionActivities
from app.clients import ElasticsearchClient
from app.handlers import ElasticsearchHandler
from app.transformer import ElasticsearchAtlasTransformer
from app.workflows import ElasticsearchMetadataExtractionWorkflow
from application_sdk.application import BaseApplication
from application_sdk.common.error_codes import ApiError
from application_sdk.constants import APPLICATION_NAME
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces
# Note: FastAPI imports removed since we're using framework's built-in endpoints

logger = get_logger(__name__)
metrics = get_metrics()
traces = get_traces()


@observability(logger=logger, metrics=metrics, traces=traces)
async def main():
    try:
        # Initialize the application using BaseApplication
        application = BaseApplication(
            name=APPLICATION_NAME,
            client_class=ElasticsearchClient,
            handler_class=ElasticsearchHandler,
        )

        await application.setup_workflow(
            workflow_and_activities_classes=[
                (ElasticsearchMetadataExtractionWorkflow, ElasticsearchMetadataExtractionActivities)
            ],
        )

        # Start the worker
        await application.start_worker()

        # Setup the application server
        await application.setup_server(
            workflow_class=ElasticsearchMetadataExtractionWorkflow,
        )

        # Start the application server
        await application.start_server()

    except ApiError:
        logger.error(f"{ApiError.SERVER_START_ERROR}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Application startup failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
