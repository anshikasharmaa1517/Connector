# 🔍 SourceSense - Elasticsearch Connector for Atlan

A Elasticsearch metadata extraction application built using Atlan's Application SDK. 

## 🎯 Overview

SourceSense extracts, transforms, and catalogs metadata from Elasticsearch clusters into Atlan's data governance platform. It provides deep insights into cluster health, index structures, field mappings, and data quality metrics.

## ✨ Features

### 🔌 **Universal Elasticsearch Connectivity**

- **Multiple Authentication Methods**: Basic, API Key, Bearer Token, Certificate-based
- **Flexible Connection Options**: HTTP/HTTPS with configurable SSL verification
- **Version Agnostic**: Works with Elasticsearch 7.x and 8.x clusters
- **Security First**: Secure credential management through Atlan's SecretStore

### 📊 **Comprehensive Metadata Extraction**

- **Cluster Metadata**: Health status, node information, version details
- **Index Intelligence**: Document counts, storage metrics, shard distribution
- **Schema Discovery**: Field types, analyzers, nested structures, multi-fields
- **Quality Metrics**: Data completeness, consistency analysis, pattern detection

### 🎨 **Modern User Experience**

- **3-Step Wizard**: Intuitive connection setup and configuration
- **Real-time Validation**: Instant connection testing and feedback
- **Progress Tracking**: Live workflow monitoring with Temporal integration
- **Responsive Design**: Works seamlessly across devices

### ⚡ **Enterprise-Grade Architecture**

- **Framework Integration**: Built on Atlan's proven SDK patterns
- **Scalable Processing**: Handles large clusters with parallel processing
- **Error Resilience**: Comprehensive retry policies and graceful degradation
- **Observability**: Full logging, metrics, and tracing support

## 🏗️ Architecture

SourceSense follows Atlan's established patterns while pioneering REST/HTTP connectivity:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend UI   │───▶│  FastAPI Server  │───▶│ Temporal Worker │
│  (React-like)   │    │   (App Server)   │    │   (Workflows)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌──────────────────┐    ┌─────────────────┐
                       │  ElasticsearchClient  │───▶│  Elasticsearch  │
                       │   (HTTP/REST)    │    │     Cluster     │
                       └──────────────────┘    └─────────────────┘
```

### Core Components

- **`ElasticsearchClient`**: HTTP-based client following BaseClient patterns
- **`ElasticsearchMetadataExtractionActivities`**: Metadata extraction logic
- **`ElasticsearchMetadataExtractionWorkflow`**: Temporal workflow orchestration
- **`ElasticsearchAtlasTransformer`**: Metadata transformation to Atlan format
- **`Frontend`**: Modern web interface for configuration and monitoring

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) package manager
- [Dapr CLI](https://docs.dapr.io/getting-started/install-dapr-cli/)
- [Temporal CLI](https://docs.temporal.io/cli)
- Elasticsearch cluster access

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

## 🛠️ Development

### Project Structure

```
sourcesense/
├── app/                    # Core application logic
│   ├── clients.py         # ElasticsearchClient implementation
│   ├── activities.py      # Metadata extraction activities
│   ├── workflows.py       # Temporal workflow definitions
│   └── transformer.py     # Atlan entity transformation
├── frontend/              # Web interface
│   ├── static/           # CSS, JS, and assets
│   └── templates/        # HTML templates
├── tests/                 # Unit and integration tests
├── main.py               # Application entry point
├── pyproject.toml        # Dependencies and configuration
└── ARCHITECTURE.md       # Detailed architecture documentation
```

### Key Design Decisions

1. **HTTP-First Approach**: Leverages Elasticsearch's REST API for vendor independence
2. **Framework Consistency**: Follows established Atlan SDK patterns for reliability
3. **Thin Components**: Each module focuses on configuration, letting the framework handle complexity
4. **Production Ready**: Comprehensive error handling, observability, and security

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

### Advanced Configuration

See `pyproject.toml` for dependency management and `ARCHITECTURE.md` for detailed configuration options.

## 🤝 Contributing

We welcome contributions! SourceSense pioneers the REST/HTTP pattern in the Atlan framework:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Follow framework patterns**: Study existing SQL connectors for consistency
4. **Add comprehensive tests**: Unit, integration, and property-based tests
5. **Update documentation**: Keep README and architecture docs current
6. **Submit a pull request**: Detailed description and testing evidence

### Development Guidelines

- Follow Atlan SDK development standards
- Maintain thin component architecture
- Comprehensive error handling and logging
- Production-ready code quality
- Extensive documentation and examples

## 📚 Documentation

- **[Architecture Documentation](ARCHITECTURE.md)**: Detailed technical design and decisions
- **[Atlan SDK Documentation](https://github.com/atlanhq/application-sdk)**: Framework documentation
- **[Sample Apps](https://github.com/atlanhq/atlan-sample-apps)**: Reference implementations

## 🏆 Framework Innovation

SourceSense represents a significant contribution to the Atlan Apps ecosystem:

- **🥇 First REST/HTTP Connector**: Pioneers HTTP-based metadata extraction patterns
- **🔧 Framework Extension**: Demonstrates BaseClient capabilities for non-SQL sources
- **📋 Best Practices**: Establishes patterns for future REST connectors
- **🎯 Production Quality**: Enterprise-grade implementation suitable for critical workloads

