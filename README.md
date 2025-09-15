# 🔍 SourceSense - Elasticsearch Connector for Atlan

<<<<<<< HEAD
An Elasticsearch metadata extraction application built using Atlan's Application SDK.
=======
A Elasticsearch metadata extraction application built using Atlan's Application SDK.

> > > > > > > aa57b6906ea4135747b017fcf2f206d738e19ff0

## 🎯 Overview

SourceSense extracts, transforms, and catalogs metadata from Elasticsearch clusters into Atlan's data governance platform. It provides deep insights into cluster health, index structures, field mappings, and data quality metrics with workflow orchestration.

## ✨ Features

### 🔌 **Universal Elasticsearch Connectivity**

- **Multiple Authentication Methods**: Basic, API Key, Bearer Token
- **Flexible Connection Options**: HTTP/HTTPS with configurable SSL verification
- **Version Agnostic**: Works with Elasticsearch 7.x and 8.x clusters
- **Security First**: Secure credential management through Atlan's SecretStore
- **Docker Support**: Containerized deployment with host networking support

### 📊 **Comprehensive Metadata Extraction**

- **Cluster Metadata**: Health status, node information, version details, cluster statistics
- **Index Intelligence**: Document counts, storage metrics, shard distribution, index patterns
- **Schema Discovery**: Field types, analyzers, nested structures, multi-fields, mappings
- **Settings Extraction**: Index configuration, shard settings, replica configurations
- **Data Transformation**: Converts raw metadata into Atlan-ready entities

### 🎨 **Modern User Experience**

- **3-Step Wizard**: Intuitive connection setup and configuration
- **Real-time Validation**: Instant connection testing and feedback
- **Progress Tracking**: Live workflow monitoring with Temporal integration
- **Responsive Design**: Works seamlessly across devices
- **Advanced Options**: Configurable batch processing and field depth limits

### ⚡ **Enterprise-Grade Architecture**

- **Framework Integration**: Built on Atlan's proven SDK patterns
- **Scalable Processing**: Handles large clusters with parallel processing
- **Error Resilience**: Comprehensive retry policies and graceful degradation
- **Observability**: Full logging, metrics, and tracing support
- **Workflow Orchestration**: Temporal-based distributed task management

## 🏗️ Architecture

SourceSense is built on the **Atlan Application SDK Framework**, which provides enterprise-grade infrastructure for metadata extraction applications. The framework integrates **Dapr** (infrastructure abstraction) and **Temporal** (workflow orchestration) to deliver a robust, scalable platform.

### SDK Framework Integration

```
┌─────────────────────────────────────────────────────────────────┐
│                    Atlan Application SDK                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │    Dapr     │  │  Temporal   │  │  FastAPI    │              │
│  │ (Infra)     │  │ (Workflows) │  │  (Server)   │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SourceSense Application                      │
├─────────────────────────────────────────────────────────────────┤
│  BaseApplication (SDK) → ElasticsearchClient → Elasticsearch    │
│  BaseApplication (SDK) → ElasticsearchHandler → Auth/Validation │
│  BaseApplication (SDK) → Workflow/Activities → Metadata Extract │
└─────────────────────────────────────────────────────────────────┘
```

### Complete System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend UI   │───▶│  FastAPI Server  │───▶│ Temporal Worker │
│  (HTML/JS)      │    │  (SDK Managed)   │    │  (SDK Managed)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Dapr Runtime  │    │  BaseApplication │    │  Workflow       │
│  (Pub/Sub,      │───▶│  (SDK Core)      │───▶│  Orchestration  │
│   State Store)  │    │                  │    │  (Temporal)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │  ElasticsearchClient  │───▶│  Elasticsearch  │
                       │  (BaseClient)    │    │     Cluster     │
                       └──────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │   Data Storage   │
                       │  (JSON Files)    │
                       └──────────────────┘
```

### SDK Framework Components

#### **1. BaseApplication (Core Orchestrator)**

```python
# main.py - SDK Integration
application = BaseApplication(
    name=APPLICATION_NAME,
    client_class=ElasticsearchClient,      # HTTP client for Elasticsearch
    handler_class=ElasticsearchHandler,    # Authentication & validation
)

await application.setup_workflow(
    workflow_and_activities_classes=[
        (ElasticsearchMetadataExtractionWorkflow, ElasticsearchMetadataExtractionActivities)
    ],
)
await application.start_worker()    # Temporal worker
await application.start_server()    # FastAPI server
```

#### **2. Infrastructure Abstraction (Dapr)**

- **Pub/Sub**: Event-driven communication
- **State Store**: Workflow state management
- **Service Discovery**: Component communication
- **Configuration**: Environment management

#### **3. Workflow Orchestration (Temporal)**

- **Workflow Engine**: Distributed task execution
- **Activity Workers**: Metadata extraction tasks
- **Retry Policies**: Fault tolerance
- **Event Sourcing**: Complete audit trail

#### **4. HTTP Server (FastAPI)**

- **API Endpoints**: RESTful interface
- **Authentication**: Connection validation
- **Frontend Serving**: Static assets and templates
- **Workflow Triggers**: HTTP-based workflow initiation

### Core Components

- **`BaseApplication`**: SDK's main orchestrator managing all components
- **`ElasticsearchClient`**: Extends `BaseClient` for HTTP/REST connectivity
- **`ElasticsearchHandler`**: Extends `BaseHandler` for authentication
- **`ElasticsearchMetadataExtractionActivities`**: Implements `ActivitiesInterface`
- **`ElasticsearchMetadataExtractionWorkflow`**: Implements `WorkflowInterface`
- **`ElasticsearchAtlasTransformer`**: Converts metadata to Atlan entities
- **`APIServer`**: SDK-managed FastAPI server with built-in endpoints

### Data Flow Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Input    │───▶│  Connection      │───▶│  Authentication │
│  (Web UI)       │    │  Configuration   │    │  & Validation   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Raw Metadata   │◀───│  Workflow        │───▶│  Elasticsearch  │
│  (JSON Files)   │    │  Orchestration   │    │  REST API       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐    ┌──────────────────┐
│  Atlan Entities │◀───│  Data            │
│  (Transformed)  │    │  Transformation  │
└─────────────────┘    └──────────────────┘
```

### Workflow Orchestration

```
┌─────────────────────────────────────────────────────────────────┐
│                    Temporal Workflow Engine                     │
├─────────────────────────────────────────────────────────────────┤
│  1. Extract Cluster Info    │ 2. Extract Index Mappings        │
│  3. Extract Index Settings  │ 4. Transform to Atlan Entities   │
│  5. Store Raw Data          │ 6. Generate Output Files         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Output Generation                          │
├─────────────────────────────────────────────────────────────────┤
│  Raw Data: ./output/raw/     │ Transformed: ./local/tmp/       │
│  • cluster_info_*.json       │ • all_transformed_*.json        │
│  • mappings_*.json           │                                 │
│  • settings_*.json           │                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- [Dapr CLI](https://docs.dapr.io/getting-started/install-dapr-cli/)
- [Temporal CLI](https://docs.temporal.io/cli)
- Elasticsearch cluster access
- Docker (for containerized deployment)

### Installation

1. **Download required components:**

   ```bash
   uv run poe download-components
   ```

2. **Install dependencies:**

   ```bash
   uv sync --all-extras --all-groups
   ```

3. **Start infrastructure (in separate terminal):**

   ```bash
   uv run poe start-deps
   ```

4. **Run SourceSense:**
   ```bash
   uv run main.py
   ```

### Docker Deployment

```bash
# Build Docker image
docker build -t sourcesense:latest .

# Run container with host networking
docker run -p 8000:8000 --network host sourcesense:latest
```

### Access Points

- **🖥️ Web Interface**: http://localhost:8000
- **📊 Temporal UI**: http://localhost:8233
- **🔧 Dapr Dashboard**: http://localhost:9999

## 📖 Usage Guide

### Step 1: Configure Connection

1. Enter your Elasticsearch cluster details (host, port, protocol)
2. Select authentication method:
   - **Basic**: Username/password authentication
   - **API Key**: Elasticsearch API key authentication
   - **Bearer**: Token-based authentication
3. Test connection to validate credentials

### Step 2: Define Connection

1. Provide a descriptive connection name
2. Add optional description and environment tags
3. Configure metadata extraction scope

### Step 3: Extract Metadata

1. Configure extraction parameters:
   - **Index Pattern**: Target specific indices (e.g., `logs-*`, `app-data-*`)
   - **System Indices**: Include/exclude Elasticsearch system indices
   - **Field Mappings**: Extract detailed schema information
   - **Quality Metrics**: Calculate data completeness and consistency
2. Start extraction and monitor progress in real-time

## 📊 Output & Results

### Generated Files

SourceSense creates comprehensive data files organized by type and timestamp:

**Raw Data Files (JSON format):**

- `./output/raw/cluster/cluster_info_*.json` - Cluster information
- `./output/raw/mappings/mappings_*.json` - Field mappings with detailed schema
- `./output/raw/settings/settings_*.json` - Index configuration settings

**Transformed Data File:**

- `./local/tmp/artifacts/transformed/all_transformed_*.json` - Atlan-ready entities

### Sample Output

**Cluster Metadata:**

```json
{
  "cluster_name": "docker-cluster",
  "cluster_version": "8.16.0",
  "cluster_health": "yellow",
  "node_count": 1,
  "total_size_bytes": 3736
}
```

**Field Metadata:**

```json
{
  "field_name": "@timestamp",
  "field_type": "date",
  "analyzer": "standard",
  "indexed": true,
  "stored": false,
  "nested": false,
  "index_name": "di_full-2025.09.09"
}
```

**Transformed Atlan Entity:**

```json
{
  "typeName": "CLUSTER",
  "attributes": {
    "name": "docker-cluster",
    "qualifiedName": "default/atlan-connectors/elasticsearch/docker-cluster",
    "clusterVersion": "8.16.0",
    "clusterStatus": "yellow",
    "numberOfNodes": 1,
    "totalIndices": 78,
    "totalDocuments": 246612
  }
}
```

## 🛠️ Development

### Project Structure

```
sourcesense/
├── app/                    # Core application logic
│   ├── clients.py         # ElasticsearchClient implementation
│   ├── activities.py      # Metadata extraction activities
│   ├── workflows.py       # Temporal workflow definitions
│   ├── handlers.py        # Authentication and connection handling
│   └── transformer.py     # Atlan entity transformation
├── frontend/              # Web interface
│   ├── static/           # CSS, JS, and assets
│   └── templates/        # HTML templates
├── output/               # Generated data files
│   └── raw/             # Raw metadata files
├── local/               # Temporary artifacts
│   └── tmp/            # Transformed entities
├── tests/                 # Unit and integration tests
├── main.py               # Application entry point
├── pyproject.toml        # Dependencies and configuration
├── DEMO_SCRIPT.md        # Demo presentation guide
└── README.md             # This file
```

### Key Design Decisions

1. **SDK Framework Integration**: Built on `BaseApplication` for enterprise-grade infrastructure
2. **HTTP-First Approach**: Leverages Elasticsearch's REST API for vendor independence
3. **Framework Consistency**: Follows established Atlan SDK patterns for reliability
4. **Infrastructure Abstraction**: Uses Dapr for pub/sub, state management, and service discovery
5. **Workflow Orchestration**: Leverages Temporal for distributed, fault-tolerant task execution
6. **Thin Components**: Each module focuses on configuration, letting the framework handle complexity
7. **Production Ready**: Comprehensive error handling, observability, and security via SDK
8. **JSON Output**: Optimized for metadata governance and human readability

### Extending SourceSense

**Adding New Metadata Types:**

```python
# In transformer.py
class ElasticsearchAlias:
    @classmethod
    def get_attributes(cls, obj: Dict[str, Any]) -> Dict[str, Any]:
        # Define alias entity transformation
        pass
```

**Custom Quality Metrics:**

```python
# In activities.py
async def calculate_custom_metrics(self, workflow_args: Dict[str, Any]):
    # Implement custom quality analysis
    pass
```

## 🚀 Performance & Scalability

### Current Capabilities

- **Tested with**: 78 indices, 1,207 entities, 246,612 documents
- **Execution time**: ~6 seconds for comprehensive extraction
- **Memory efficient**: Streaming processing for large datasets
- **Parallel processing**: Concurrent index and field extraction

### Production Considerations

- **Batch processing**: Configurable batch sizes for large clusters
- **Field depth limits**: Prevents infinite loops in nested structures
- **Error resilience**: Comprehensive retry policies
- **Resource management**: Efficient memory and CPU utilization

## 🧪 Testing

Run the complete test suite:

```bash
uv run coverage run -m pytest --import-mode=importlib --capture=no --log-cli-level=INFO tests/ -v --full-trace --hypothesis-show-statistics
```

### Testing Strategy

- **Unit Tests**: Individual component testing with mocking
- **Integration Tests**: End-to-end workflows with test Elasticsearch clusters
- **Property-Based Tests**: Using Hypothesis for robust edge case coverage
- **Performance Tests**: Large cluster simulation and load testing

## 📊 Monitoring & Observability

### Built-in Observability

- **Structured Logging**: Comprehensive activity and error logging
- **Metrics Collection**: Performance and usage metrics via framework
- **Distributed Tracing**: Request flow tracking across components
- **Health Monitoring**: Real-time extraction progress and status

### Temporal Workflow Monitoring

Access the Temporal UI at http://localhost:8233 to monitor:

- Workflow execution status and history
- Activity performance and retry attempts
- Error details and stack traces
- Workflow timeline and dependencies

## 🔧 Configuration

### Environment Variables

```bash
# Temporal Configuration
TEMPORAL_HOST=localhost
TEMPORAL_PORT=7233

# Application Configuration
APP_HTTP_HOST=localhost
APP_HTTP_PORT=8000

# Observability
LOG_LEVEL=INFO
ENABLE_METRICS=true
ENABLE_TRACING=true
```
