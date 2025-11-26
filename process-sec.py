import os
import json
from typing import List, Optional, Tuple, Any

from pydantic import BaseModel, Field
from databricks import sql

from tensorlake.applications import Image, application, function, cls
from tensorlake.documentai import (
    DocumentAI, PageClassConfig, StructuredExtractionOptions, ParseResult
)

"""
TENSORLAKE APPLICATIONS PIPELINE OVERVIEW
==========================================

This application demonstrates a complete document processing pipeline using Tensorlake Applications.

Architecture:
1. document_ingestion (entry point with @application())
   └─> classify_pages - Classifies pages in SEC filings
   └─> extract_structured_data.map() - Extracts data from classified pages IN PARALLEL
   └─> initialize_databricks_table - Sets up database schema
   └─> write_to_databricks.map() - Writes results to Databricks IN PARALLEL

Key Tensorlake Concepts Used:
- @application(): Marks the entry point of your application
- @function(): Makes functions distributed and executable in the cloud or locally
- .map(): Enables parallel execution across multiple items
- Image: Defines the Docker container environment with dependencies
- secrets: Securely injects environment variables at runtime

Benefits of .map():
- Automatic parallelization: Each item is processed concurrently
- Fault tolerance: Individual failures don't stop the entire pipeline
- Scalability: Processes scale horizontally based on workload
"""

# TENSORLAKE APPLICATIONS: Define a custom runtime environment
# Image defines the Docker container environment where your functions will run.
# You can specify dependencies, system packages, and environment configuration.
# All @function decorators can reference this image to ensure consistent execution.
image = (
    Image(base_image="python:3.11-slim", name="databricks-sec")
    .run("pip install databricks-sql-connector pandas pyarrow")
)

class AIRiskMention(BaseModel):
    """Individual AI-related risk mention"""
    risk_category: str = Field(
        description="Category: Operational, Regulatory, Competitive, Ethical, Security, Liability"
    )
    risk_description: str = Field(description="Description of the AI risk")
    severity_indicator: Optional[str] = Field(None, description="Severity level if mentioned")
    citation: str = Field(description="Page reference")

class AIRiskExtraction(BaseModel):
    """Complete AI risk data from a filing"""
    company_name: str
    ticker: str
    filing_type: str
    filing_date: str
    fiscal_year: str
    fiscal_quarter: Optional[str] = None
    ai_risk_mentioned: bool
    ai_risk_mentions: List[AIRiskMention] = []
    num_ai_risk_mentions: int = 0
    ai_strategy_mentioned: bool = False
    ai_investment_mentioned: bool = False
    ai_competition_mentioned: bool = False
    regulatory_ai_risk: bool = False

# TENSORLAKE APPLICATIONS: Application Entry Point
# @application() marks this function as the main entry point for your Tensorlake application.
# @function() makes this a distributed function that can run in the cloud or locally.
#
# Key concepts:
# - secrets: List of environment variable names that will be securely injected at runtime
# - image: The runtime environment (Docker container) where this function executes
# - Functions decorated with @function can call other @function decorated functions
# - You can use .map() on @function decorated functions for parallel execution
@application()
@function(
    secrets=[
        "TENSORLAKE_API_KEY"
    ],
    image=image
)
def document_ingestion(document_urls: List[str]) -> None:
    """Main entry point for document processing pipeline"""
    print(f"Starting document ingestion for {len(document_urls)} documents.")

    # Step 1: Classify pages in all documents
    parse_ids = classify_pages(document_urls)
    print(f"Classification complete. Parse IDs: {parse_ids}")

    # Step 2: TENSORLAKE .map() - Parallel execution
    # .map() calls extract_structured_data once for each item in parse_ids.items()
    # Each call runs in parallel, making this very efficient for processing multiple documents
    # Returns a list of results (tuples in this case) from all parallel executions
    results = extract_structured_data.map(parse_ids.items())
    print(f"Extraction complete. Results: {results}")

    # Step 3: Initialize database schema
    initialize_databricks_table()
    print("Databricks table initialized.")

    # Step 4: TENSORLAKE .map() - Parallel writes to Databricks
    # .map() again enables parallel processing - each result tuple is written to Databricks
    # in parallel, significantly speeding up the data ingestion process
    print("Writing results to Databricks.")
    write_to_databricks.map(results)
    print("Document ingestion process completed.")


@function(
    secrets=[
        "TENSORLAKE_API_KEY"
    ], 
    image=image
)
def classify_pages(document_urls: List[str]) -> None:
    """Classify pages in SEC filings to identify AI risk factors"""
    doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
    
    page_classifications = [
        PageClassConfig(
            name="risk_factors",
            description="Pages that contain risk factors related to AI."
        ),
    ]
    parse_ids = {}

    for file_url in document_urls:
        try:
            parse_id = doc_ai.classify(
                file_url=file_url,
                page_classifications=page_classifications
            )
            parse_ids[file_url] = parse_id
            print(f"Successfully classified {file_url}: {parse_id}")
        except Exception as e:
            print(f"Failed to classify document {file_url}: {e}")

    return parse_ids

# TENSORLAKE APPLICATIONS: Distributed Function for Parallel Processing
# This function is designed to be called via .map() for parallel execution.
# When called with .map(), this function runs once for each item in the input list,
# with all executions happening in parallel across multiple workers.
#
# Error Handling: Always wrap .map() functions in try-except to return None on failure.
# This allows the pipeline to continue processing other items even if one fails.
@function(
    image=image,
    secrets=[
        "TENSORLAKE_API_KEY"
    ]
)
def extract_structured_data(url_parse_id_pair: Tuple[str, str]) -> Optional[Tuple[str, str]]:
    """Extract structured data from classified pages

    Args:
        url_parse_id_pair: Tuple of (file_url, parse_id) from the classification step

    Returns:
        Tuple of (extract_result_id, file_url) or None if processing fails
    """
    print(f"Processing: {url_parse_id_pair}")

    try:
        doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
        result = doc_ai.wait_for_completion(parse_id=url_parse_id_pair[1])

        page_numbers = []
        for page_class in result.page_classes:
            if page_class.page_class == "risk_factors":
                page_numbers.extend(page_class.page_numbers)

        if not page_numbers:
            print(f"No risk factor pages found for {url_parse_id_pair[0]}")
            return None

        page_number_str_list = ",".join(str(i) for i in page_numbers)
        print(f"Extracting from pages: {page_number_str_list}")

        extract_result = doc_ai.extract(
            file_url=url_parse_id_pair[0],
            page_range=page_number_str_list,
            structured_extraction_options=[
                StructuredExtractionOptions(
                    schema_name="AIRiskExtraction",
                    json_schema=AIRiskExtraction
                )
            ]
        )
        print(f"Extraction result: {extract_result}")

        return (extract_result, url_parse_id_pair[0])
    except Exception as e:
        print(f"Error processing {url_parse_id_pair[0]}: {e}")
        return None

@function(
    image=image, 
    secrets=[
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN"
    ]
)
def initialize_databricks_table() -> None:
    """Initialize the Databricks table with the required schema"""
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        _tls_no_verify=True,
    )
    cursor = connection.cursor()
    
    create_ai_risk_factors_sql = """
    CREATE TABLE IF NOT EXISTS ai_risk_filings (
        company_name STRING,
        ticker STRING,
        filing_type STRING,
        filing_date STRING,
        fiscal_year STRING,
        fiscal_quarter STRING,
        ai_risk_mentioned BOOLEAN,
        ai_risk_mentions STRING,
        num_ai_risk_mentions INT,
        ai_strategy_mentioned BOOLEAN,
        ai_investment_mentioned BOOLEAN,
        ai_competition_mentioned BOOLEAN,
        regulatory_ai_risk BOOLEAN
    )
    """
    cursor.execute(create_ai_risk_factors_sql)
    create_ai_risk_mentions_sql = """
        CREATE TABLE IF NOT EXISTS ai_risks (
            company_name STRING,
            ticker STRING,
            fiscal_year STRING,
            fiscal_quarter STRING,
            source_file STRING,
            risk_category STRING,
            risk_description STRING,
            severity_indicator STRING,
            citation STRING
        )
    """
    cursor.execute(create_ai_risk_mentions_sql)
    connection.commit()
    connection.close()

# TENSORLAKE APPLICATIONS: Parallel Database Write Function
# This function is called via .map() to write results to Databricks in parallel.
# Each execution processes one result tuple from the extraction step.
#
# Data Flow: extract_structured_data returns tuples -> .map() collects them into a list
#            -> write_to_databricks.map() processes each tuple in parallel
#
# Secrets: Multiple secrets can be specified. Each will be available as an environment
# variable inside the function. Secrets are never logged or exposed in code.
@function(
    image=image,
    secrets=[
        "TENSORLAKE_API_KEY",
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN"
    ]
)
def write_to_databricks(result_tuple: Tuple[Any, str]) -> None:
    """Write structured data to Databricks tables

    Args:
        result_tuple: Tuple of (extract_result_id, file_url) from extract_structured_data
    """
    # Handle None values - functions called via .map() should gracefully skip failed items
    if result_tuple is None:
        return

    extract_result, file_url = result_tuple
    if extract_result is None:
        return

    doc_ai = DocumentAI(api_key=os.getenv("TENSORLAKE_API_KEY"))
    result: ParseResult = doc_ai.wait_for_completion(extract_result)
    if not result.structured_data:
        return
    raw = result.structured_data[0].data
    record = raw if isinstance(raw, dict) else (raw[0] if isinstance(raw, list) and raw else {})
    data = dict(record)
    mentions = data.pop("ai_risk_mentions", []) or []
    
    # Add source file reference
    source_file = os.path.basename(file_url)
    connection = sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        _tls_no_verify=True,
    )
    cursor = connection.cursor()

    # Serialize mentions for STRING column storage
    ai_risk_mentions_json = json.dumps(mentions) if mentions else None
    
    # Insert the single record into ai_risk_filings
    insert_sql = """
    INSERT INTO ai_risk_filings (
        company_name,
        ticker,
        filing_type,
        filing_date,
        fiscal_year,
        fiscal_quarter,
        ai_risk_mentioned,
        ai_risk_mentions,
        num_ai_risk_mentions,
        ai_strategy_mentioned,
        ai_investment_mentioned,
        ai_competition_mentioned,
        regulatory_ai_risk
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Execute the insert with positional parameters
    cursor.execute(insert_sql, (
        data.get('company_name'),
        data.get('ticker'),
        data.get('filing_type'),
        data.get('filing_date'),
        data.get('fiscal_year'),
        data.get('fiscal_quarter'),
        data.get('ai_risk_mentioned', False),
        ai_risk_mentions_json,
        data.get('num_ai_risk_mentions', 0),
        data.get('ai_strategy_mentioned', False),
        data.get('ai_investment_mentioned', False),
        data.get('ai_competition_mentioned', False),
        data.get('regulatory_ai_risk', False)
    ))
    
    # Insert into ai_risks table
    if mentions:
        insert_mentions_sql = """
        INSERT INTO ai_risks (
            company_name,
            ticker,
            fiscal_year,
            fiscal_quarter,
            source_file,
            risk_category,
            risk_description,
            severity_indicator,
            citation
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for mention in mentions:
            cursor.execute(insert_mentions_sql, (
                data.get('company_name'),
                data.get('ticker'),
                data.get('fiscal_year'),
                data.get('fiscal_quarter'),
                source_file,
                mention.get('risk_category'),
                mention.get('risk_description'),
                mention.get('severity_indicator'),
                mention.get('citation')
            ))
    
    connection.commit()
    connection.close()

if __name__ == "__main__":
    from tensorlake.applications import run_local_application

    # TENSORLAKE APPLICATIONS: Local Development
    # run_local_application() executes your application locally for testing and development.
    # Pass the entry point function (decorated with @application()) and its arguments.
    #
    # For production deployment:
    # 1. Use Tensorlake CLI to deploy: `tensorlake deploy`
    # 2. Your application will run in the cloud with automatic scaling
    # 3. All @function decorated functions will execute in their specified container environments
    #
    # Secrets: When running locally, secrets are read from environment variables.
    #          In production, secrets are managed securely through the Tensorlake platform.

    # Example usage with a single document
    test_urls = [
        "https://investors.confluent.io/static-files/95299e90-a988-42c5-b9b5-7da387691f6a"
    ]

    response = run_local_application(
        document_ingestion,
        test_urls
    )

    print(response.output())
