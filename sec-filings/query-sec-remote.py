from tensorlake.applications import run_remote_application, Request
import json
import sys

## Query SEC Filings
queries = ["risk-distribution", "operational-risks", "risk-evolution", "risk-timeline", "risk-profiles", "company-summary"]

# Choose a query
query = queries[0] # risk-distribution is the default query

if len(sys.argv) > 1:
    query = queries[int(sys.argv[1])]

request: Request = run_remote_application('query_sec', query)
output = request.output()

pretty_json = json.loads(output)
print(json.dumps(pretty_json,indent=4))