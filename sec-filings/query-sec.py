import os
import json
from typing import List, Optional, Tuple, Any

from pydantic import BaseModel, Field
from databricks import sql

from tensorlake.applications import Image, application, function, cls

image = (
    Image(base_image="python:3.11-slim", name="databricks-sec")
    .run("pip install databricks-sql-connector pandas pyarrow")
)

@application()
@function(
    image=image
)
def query_sec(query_choice: str) -> str:
    # Risk category distribution - Default Query
    query = """
        SELECT 
            risk_category,
            COUNT(*) as total_mentions,
            COUNT(DISTINCT company_name) as companies_mentioning
        FROM ai_risks
        WHERE risk_category IS NOT NULL
        GROUP BY risk_category
        ORDER BY total_mentions DESC
    """
    match query_choice:
        case "operational-risks":
            query = """
                WITH ranked_risks AS (
                    SELECT 
                        company_name,
                        ticker,
                        risk_description,
                        citation,
                        LENGTH(risk_description) as description_length,
                        ROW_NUMBER() OVER (PARTITION BY company_name ORDER BY LENGTH(risk_description) DESC) as rn
                    FROM ai_risks
                    WHERE risk_category = 'Operational'
                )
                SELECT 
                    company_name,
                    ticker,
                    risk_description,
                    citation,
                    description_length
                FROM ranked_risks
                WHERE rn = 1
                ORDER BY company_name
            """
        case "risk-evolution":
            query = """
                SELECT 
                    company_name,
                    ticker,
                    fiscal_year,
                    fiscal_quarter,
                    risk_category,
                    risk_description,
                    citation
                FROM ai_risks
                WHERE fiscal_year = '2025'
                ORDER BY company_name, fiscal_quarter
            """
        case "risk-timeline":
            query = """
                SELECT 
                    fiscal_year,
                    fiscal_quarter,
                    COUNT(DISTINCT company_name) as num_companies,
                    SUM(num_ai_risk_mentions) as total_risk_mentions,
                    AVG(num_ai_risk_mentions) as avg_risk_mentions_per_filing,
                    SUM(CASE WHEN regulatory_ai_risk THEN 1 ELSE 0 END) as filings_with_regulatory_risk
                FROM ai_risk_filings
                GROUP BY fiscal_year, fiscal_quarter
                ORDER BY fiscal_year, fiscal_quarter
            """
        case "risk-profiles":
            query = """
                SELECT 
                    company_name,
                    ticker,
                    risk_category,
                    COUNT(*) as frequency
                FROM ai_risks
                WHERE risk_category IS NOT NULL
                GROUP BY company_name, ticker, risk_category
                ORDER BY company_name, frequency DESC
            """
        case "company-summary":
            query = """
                SELECT 
                    company_name,
                    ticker,
                    COUNT(*) as total_filings,
                    AVG(num_ai_risk_mentions) as avg_risk_mentions,
                    SUM(CASE WHEN regulatory_ai_risk THEN 1 ELSE 0 END) as filings_with_regulatory_risk,
                    SUM(CASE WHEN ai_competition_mentioned THEN 1 ELSE 0 END) as filings_mentioning_competition,
                    SUM(CASE WHEN ai_investment_mentioned THEN 1 ELSE 0 END) as filings_mentioning_investment
                FROM ai_risk_filings
                GROUP BY company_name, ticker
                ORDER BY avg_risk_mentions DESC
            """
    
    return make_query(query)


@function(
    image=image, 
    secrets=[
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN"
    ]
)
def make_query(query: str) -> str:
    import pandas as pd
    from databricks import sql

    try:
        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
            _tls_no_verify=True,
        )
        cursor = connection.cursor()
        cursor.execute(query)
        # Fetch results as pandas DataFrame
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        # Convert to pandas DataFrame
        df = pd.DataFrame(results, columns=columns)
        cursor.close()
        connection.close()
        return df.to_json(orient='records')
    except Exception as e:
        raise e


if __name__ == "__main__":
    from tensorlake.applications import run_local_application
    import sys

    queries = ["risk-distribution", "operational-risks", "risk-evolution", "risk-timeline", "risk-profiles", "company-summary"]
    query = queries[0]

    if len(sys.argv) > 1:
        query = queries[int(sys.argv[1])]

    response = run_local_application(
        query_sec,
        query
    )
    pretty_json = json.loads(response.output())
    print(json.dumps(pretty_json, indent=4))