import boto3
import os
import time
import logging
import json

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Athena client
athena = boto3.client("athena", region_name="us-east-1")

# Environment variables
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE', 'forex_data_db_new')
SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'forex_api_results')
VIEW_NAME = os.getenv('VIEW_NAME', 'forex_rates_view')
S3_OUTPUT_LOCATION = os.getenv('S3_OUTPUT_LOCATION')  # e.g., s3://your-athena-query-results/
WORKGROUP = os.getenv('WORKGROUP', 'primary')

def run_athena_query(query):
    logger.info(f"Running query: {query.strip()[:100]}...")
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION},
        WorkGroup=WORKGROUP
    )

    query_execution_id = response['QueryExecutionId']

    # Wait for query to complete
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if status != 'SUCCEEDED':
        reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise Exception(f"Athena query failed: {reason}")

    return query_execution_id

def lambda_handler(event, context):
    try:
        # Drop existing view if it exists
        logger.info(f"Dropping view {VIEW_NAME} if it exists...")
        drop_view_query = f"DROP VIEW IF EXISTS {ATHENA_DATABASE}.{VIEW_NAME}"
        run_athena_query(drop_view_query)

        # Create view pointing to the Glue-managed table
        logger.info(f"Creating view {VIEW_NAME} based on table {SOURCE_TABLE}...")
        create_view_query = f"""
        CREATE OR REPLACE VIEW {ATHENA_DATABASE}.{VIEW_NAME} AS
        SELECT * FROM {ATHENA_DATABASE}.{SOURCE_TABLE}
        """
        run_athena_query(create_view_query)

        logger.info(f"View {VIEW_NAME} created successfully.")
        return {
            'statusCode': 200,
            'body': json.dumps(f"Athena view '{VIEW_NAME}' created successfully.")
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error creating view: {str(e)}")
        }
