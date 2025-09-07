import boto3
import json
import time
import os
import logging
import csv
import io

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
athena = boto3.client('athena', region_name='us-east-1')
s3 = boto3.client('s3')

# Config
ATHENA_DATABASE = os.getenv('ATHENA_DATABASE', 'forex_data_db')
ATHENA_TABLE_NAME = os.getenv('ATHENA_TABLE_NAME', 'forex_data')
ATHENA_VIEW_NAME = os.getenv('ATHENA_VIEW_NAME', 'forex_rates_view')
S3_INPUT_LOCATION = os.getenv('S3_INPUT_LOCATION')
S3_OUTPUT_LOCATION = os.getenv('S3_OUTPUT_LOCATION')
WORKGROUP = os.getenv('WORKGROUP', 'primary')

def run_athena_query(query):
    logger.info(f"Running query: {query.strip()[:100]}...")
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION},
        WorkGroup=WORKGROUP
    )
    qid = resp['QueryExecutionId']

    timeout = 60
    elapsed = 0
    while elapsed < timeout:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
        elapsed += 1
    else:
        raise Exception("Query timed out")

    if state != 'SUCCEEDED':
        reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
        raise Exception(f"Query failed: {reason}")

    return qid

def get_csv_headers(s3_uri):
    bucket_key = s3_uri.replace("s3://", "").split("/", 1)
    bucket = bucket_key[0]
    prefix = bucket_key[1] if len(bucket_key) > 1 else ""

    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in objs:
        raise Exception("No files found in S3 path")

    first_file_key = None
    for obj in objs['Contents']:
        if obj['Key'].lower().endswith(".csv"):
            first_file_key = obj['Key']
            break
    if not first_file_key:
        raise Exception("No CSV files found in given S3 location")

    logger.info(f"Reading header from {first_file_key}")

    rsp = s3.get_object(Bucket=bucket, Key=first_file_key, Range='bytes=0-1024')
    first_bytes = rsp['Body'].read()
    first_line = first_bytes.decode('utf-8').splitlines()[0]

    reader = csv.reader(io.StringIO(first_line))
    headers = next(reader)
    logger.info(f"Detected CSV headers: {headers}")
    return headers

def lambda_handler(event, context):
    try:
        logger.info("Dropping old view/table if they exist...")
        run_athena_query(f"DROP VIEW IF EXISTS {ATHENA_DATABASE}.{ATHENA_VIEW_NAME}")
        run_athena_query(f"DROP TABLE IF EXISTS {ATHENA_DATABASE}.{ATHENA_TABLE_NAME}")

        logger.info("Detecting schema from CSV...")
        headers = get_csv_headers(S3_INPUT_LOCATION)

        partition_keys = {'year', 'month'}
        schema_cols = ",\n    ".join([f"`{col.strip()}` string" for col in headers if col.strip().lower() not in partition_keys])
        partition_cols = "year string,\n    month string"

        create_table_query = f"""
        CREATE EXTERNAL TABLE {ATHENA_DATABASE}.{ATHENA_TABLE_NAME} (
            {schema_cols}
        )
        PARTITIONED BY (
            {partition_cols}
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar' = ',',
            'quoteChar' = '\"',
            'escapeChar' = '\\\\'
        )
        LOCATION '{S3_INPUT_LOCATION}'
        TBLPROPERTIES (
            'has_encrypted_data'='false',
            'skip.header.line.count'='1'
        );
        """
        run_athena_query(create_table_query)

        logger.info("Repairing table to load partitions...")
        run_athena_query(f"MSCK REPAIR TABLE {ATHENA_DATABASE}.{ATHENA_TABLE_NAME}")

        logger.info("Creating view from new table...")
        create_view_query = f"""
        CREATE OR REPLACE VIEW {ATHENA_DATABASE}.{ATHENA_VIEW_NAME} AS
        SELECT * FROM {ATHENA_DATABASE}.{ATHENA_TABLE_NAME};
        """
        run_athena_query(create_view_query)

        return {
            'statusCode': 200,
            'body': json.dumps('Partitioned Athena table and view created successfully.')
        }
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': str(e)
        }
