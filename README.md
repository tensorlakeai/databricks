# Databricks + Tensorlake Integration Examples

<p align="center">
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/RAG-8B5CF6?style=for-the-badge&logo=openai&logoColor=white" />
</p>
<p align="center">
  <a href="https://docs.tensorlake.ai"><img src="https://img.shields.io/badge/docs-tensorlake.ai-blue?style=flat-square" /></a>
  <img src="https://img.shields.io/badge/license-MIT-green?style=flat-square" />
</p>

## Transform Unstructured Data into Queryable and AI Ready Data on Databricks

Tensorlake is a serverless platform for building data applications and agents in Python that can ingest and transform unstructured data before landing them in Databricks Data Intelligence Platform. This is an alternative to perform ETL orchestration with SQL expressions and UDF functions. 

Tensorlake's applications automatically behave like durable queues so you wouldn't need to setup Kafka or other queues to manage ingestion. The clusters automatically scale up as data is ingested to process them.

## Table of Contents
- [Example Use Cases](#use-cases)
  - [SEC Filings Analysis Pipeline](#blueprint-sec-filings-analysis-pipeline)
  - [Document Indexing](#blueprint-document-indexing) (*coming soon*)
- [Quick Overview: Tensorlake Applications](#quick-overview-tensorlake-applications)
- [Why This Integration Matters](#why-this-integration-matters)
- [Resources](#resources)

## Use Cases

We present some blueprints for production ready patterns to integrate with Databricks and code that you can deploy under 2 minutes and experience the integration.

### Blueprint: SEC Filings Analysis Pipeline

The Tensorlake Application receives Document URLs over HTTP, uses Vision Language Models to classify pages containing risk factors, calls an LLM for structured extraction from only relevant pages, and then uses Databricks SQL Connector to write structured data into your Databricks SQL Warehouse. Once it's inside Databricks you can run complex analytics to track trends, compare companies, and discover emerging risk patterns.

The Application is written in Python, without any external orchestration engines, so you can build and test it like any other normal application. You can use any document AI API in the Application, or even run open source VLM models on GPUs by annotating functions with GPU enabled hardware resources.

Tensorlake automatically queues requests and scales out the cluster, there is no extra configuration required for handling spiky ingestion.

**Key Features:**
- **Page Classification with VLMs**: Reduces processing from ~200 pages to ~20 relevant pages per document
- **Structured Extraction**: Extracts AI risk categories, descriptions, and severity indicators using Pydantic schemas
- **Parallel Processing**: Uses `.map()` to process multiple documents simultaneously
- **Dual Table Design**: Summary table for aggregations, detailed table for deep analysis
- **Pre-built Queries**: 6 analytics queries for risk distribution, trends, and company comparisons

Try out the [code here](./sec-filings).

### Blueprint: Document Indexing

*Coming soon* - Build a RAG-ready document knowledge base by extracting text chunks with embeddings and storing them in Databricks for semantic search and retrieval.

## Quick Overview: Tensorlake Applications

Tensorlake Applications are Python programs that:
1. Run as serverless applications
2. Can be triggered by HTTP requests, message queues, or scheduled events
3. Can use any Python package or model
4. Can run on CPU or GPU
5. Automatically scale out based on load
6. Have built-in queuing and fault tolerance
7. Support function composition with `.map()` for parallel processing

## Why This Integration Matters

The integration between Tensorlake and Databricks provides several key benefits:

1. **Simplified ETL for Unstructured Data**: Convert documents, images, and other unstructured data into structured formats without complex orchestration like Apache Airflow or Prefect.
2. **Serverless Architecture**: No infrastructure management required - just write Python code and deploy.
3. **Automatic Scaling**: Handle varying loads without manual intervention or cluster configuration.
4. **GPU Support**: Run ML models and VLMs efficiently when needed for document classification or embedding generation.
5. **Databricks Integration**: Leverage Databricks' powerful analytics capabilities, Unity Catalog, and Delta Lake with properly structured data.
6. **Production Ready**: Built-in error handling, retries, and observability for enterprise workloads.

## Architecture

```
Document URLs → Tensorlake Application → Page Classification (VLM)
                                      → Structured Extraction (LLM)
                                      → Databricks SQL Warehouse
                                      → SQL Analytics & Dashboards
```

The architecture separates document processing from querying:
- **Processing Application**: Handles ingestion, classification, extraction, and loading
- **Query Application**: Provides pre-built analytics queries as an API

Both applications are deployed as serverless functions and can be called via HTTP or programmatically.

## Getting Started

### Prerequisites
- Python 3.11+
- [Tensorlake API Key](https://docs.tensorlake.ai/platform/authentication#api-keys)
- Databricks SQL Warehouse credentials (Server Hostname, HTTP Path, Access Token)

### Quick Start

1. **Clone this repository**
   ```bash
   git clone https://github.com/tensorlakeai/databricks
   cd databricks/sec-filings
   ```

2. **Install dependencies**
   ```bash
   pip install --upgrade tensorlake databricks-sql-connector pandas pyarrow
   ```

3. **Set environment variables**
   ```bash
   export TENSORLAKE_API_KEY=your_tensorlake_api_key
   export DATABRICKS_SERVER_HOSTNAME=your_hostname
   export DATABRICKS_HTTP_PATH=your_http_path
   export DATABRICKS_ACCESS_TOKEN=your_access_token
   ```

4. **Test locally**
   ```bash
   python process-sec.py  # Process a test document
   python query-sec.py 0  # Run a query
   ```

5. **Deploy to Tensorlake Cloud**
   ```bash
   # Set secrets
   tensorlake secrets set TENSORLAKE_API_KEY='your_key'
   tensorlake secrets set DATABRICKS_SERVER_HOSTNAME='your_hostname'
   tensorlake secrets set DATABRICKS_HTTP_PATH='your_path'
   tensorlake secrets set DATABRICKS_ACCESS_TOKEN='your_token'
   
   # Deploy applications
   tensorlake deploy process-sec.py
   tensorlake deploy query-sec.py
   ```

6. **Run the full pipeline**
   ```bash
   python process-sec-remote.py  # Process all SEC filings
   python query-sec-remote.py 2  # Query emerging risks
   ```

## Example Queries

The query application provides 6 pre-built analytics queries:

| Query # | Name | Description |
|---------|------|-------------|
| 0 | Risk Distribution | Count risk mentions by category across all companies |
| 1 | Operational Risks | Most detailed operational risk per company |
| 2 | Risk Evolution | All AI risks mentioned in 2025 filings |
| 3 | Risk Timeline | Trends in risk mentions over time |
| 4 | Risk Profiles | Risk category frequency by company |
| 5 | Company Summary | Filing statistics and risk patterns per company |

## Project Structure

```
sec-filings/
├── process-sec.py           # Main processing application (local)
├── query-sec.py             # Query application (local)
├── process-sec-remote.py    # Script to call deployed process app
├── query-sec-remote.py      # Script to call deployed query app
└── README.md                # This file
```

## Resources

- [Tensorlake Documentation](https://docs.tensorlake.ai)
- [Databricks Documentation](https://docs.databricks.com)
- [Tutorial: Query SEC Filings in Databricks](https://docs.tensorlake.ai/examples/tutorials/query-sec-filings-databricks)
- [Integration Guide](https://docs.tensorlake.ai/integrations/databricks)
- [Community Support](https://tlake.link/slack)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://docs.tensorlake.ai)
- 💬 [Community Slack](https://tlake.link/slack)
- 🐛 [Issue Tracker](https://github.com/tensorlakeai/databricks/issues)
- 📧 [Email Support](mailto:support@tensorlake.ai)