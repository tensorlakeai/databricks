
# SEC Filings AI Risk Analysis Example (Databricks Edition)

This example demonstrates how to use Tensorlake Applications to extract and analyze AI-related risk mentions from SEC filings, storing and querying results in Databricks.

## Overview

This folder contains two main Tensorlake applications:

1. **`process-sec.py`** - Extracts AI risk data from SEC filings using Tensorlake DocAI and stores it in Databricks
2. **`query-sec.py`** - Queries the extracted data with 6 pre-defined analysis options from Databricks

The example processes SEC filings to identify and categorize AI-related risks.

## Available Queries

- `0` - Risk category distribution
- `1` - Operational AI risks
- `2` - Emerging risks in 2025
- `3` - Risk timeline analysis
- `4` - Company risk profiles
- `5` - Company summary statistics

## Getting Started

### Prerequisites

- Tensorlake API key
- Databricks SQL Warehouse credentials:
	- Server Hostname
	- HTTP Path
	- Access Token
- Python 3.11+

### Databricks Setup

You need access to a Databricks SQL Warehouse. Find your connection details in the Databricks workspace under SQL Warehouses > Connection Details.

### Local Testing

#### 1. Install Dependencies

```bash
pip install --upgrade tensorlake databricks-sql-connector pandas pyarrow
```

#### 2. Set Environment Variables

```bash
export TENSORLAKE_API_KEY=YOUR_TENSORLAKE_API_KEY
export DATABRICKS_SERVER_HOSTNAME=YOUR_DATABRICKS_SERVER_HOSTNAME
export DATABRICKS_HTTP_PATH=YOUR_DATABRICKS_HTTP_PATH
export DATABRICKS_ACCESS_TOKEN=YOUR_DATABRICKS_ACCESS_TOKEN
export DATABRICKS_SQL_CONNECTOR_VERIFY_SSL=false  # If you have SSL issues
```

#### 3. Process a Test Filing

Run the processing script to extract data from a single test SEC filing:

```bash
python process-sec.py
```

#### 4. Query the Data

Query the extracted data (replace `5` with any query number 0-5):

```bash
python query-sec.py 5
```

### Deploying to Tensorlake Cloud

#### 1. Verify Tensorlake Connection

```bash
tensorlake whoami
```

#### 2. Set Secrets

```bash
tensorlake secrets set DATABRICKS_SERVER_HOSTNAME='YOUR_DATABRICKS_SERVER_HOSTNAME'
tensorlake secrets set DATABRICKS_HTTP_PATH='YOUR_DATABRICKS_HTTP_PATH'
tensorlake secrets set DATABRICKS_ACCESS_TOKEN='YOUR_DATABRICKS_ACCESS_TOKEN'
tensorlake secrets set TENSORLAKE_API_KEY='YOUR_TENSORLAKE_API_KEY'
```

#### 3. Verify Secrets

```bash
tensorlake secrets list
```

#### 4. Deploy Applications

Deploy the processing application:

```bash
tensorlake deploy process-sec.py
```

Deploy the query application:

```bash
tensorlake deploy query-sec.py
```

Once your applications have been deployed, you should be able to see them in your Applications on [cloud.tensorlake.ai](https://cloud.tensorlake.ai).

![A screenshot of the Tensorlake dashboard showing the two deployed applications `document_ingestion` and `query_sec`](./deployed-applications.png)

#### 5. Run the Full Pipeline

Process all SEC filings using the deployed application:

```bash
python process-sec-remote.py
```

To run a specific query using the deployed application:

*Note: specify a command line argument 0-5*
```bash
python query-sec-remote.py 2
```

## Files

- `process-sec.py` - Document processing application
- `query-sec.py` - Data query application
- `process-sec-remote.py` - Script to run the deployed process-sec application
- `query-sec-remote.py` - Script to run the deployed query-sec application
- `README.md` - This file