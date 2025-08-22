from dotenv import load_dotenv
load_dotenv()  # loads environment variables from env variables of os.env


import os
import json
import csv
import boto3
import requests
import logging
import io
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError, BotoCoreError

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

def lambda_handler(event, context):
    logger.info("Lambda execution started.")
 
    # Load environment variables
    api_key = os.getenv('API_ACCESS_KEY')
    bucket = os.getenv('BUCKET_NAME')
    prefix = os.getenv('BUCKET_PREFIX', '')
    currency_list = os.getenv('CURRENCY_LIST')
 
    if not all([api_key, bucket, currency_list]):
        msg = "Missing one or more required environment variables: API_ACCESS_KEY, BUCKET_NAME, CURRENCY_LIST"
        logger.error(msg)
        return {'statusCode': 500, 'body': json.dumps(msg)}
 
    url = f"https://api.currencylayer.com/live?access_key={api_key}&currencies={currency_list}&source=USD&format=1"
 
    # API request
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.exception("Failed to retrieve currency data.")
        return {'statusCode': 500, 'body': json.dumps(f"Request failed: {e}")}
 
    if not data.get('success'):
        error_info = data.get('error', {}).get('info', 'Unknown API error')
        logger.error(f"CurrencyLayer API error: {error_info}")
        return {'statusCode': 500, 'body': json.dumps(f"API error: {error_info}")}
 
    # Timestamp conversion and logging
    try:
        raw_timestamp = data['timestamp']
        logger.info(f"Raw timestamp from API (Unix): {raw_timestamp}")
 
        utc_dt = datetime.utcfromtimestamp(raw_timestamp).replace(tzinfo=timezone.utc)
        logger.info(f"Converted UTC time: {utc_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}")
 
        aest_dt = utc_dt.astimezone(ZoneInfo("Australia/Sydney"))
        timestamp_str = aest_dt.strftime('%d/%m/%Y %H:%M %Z')
        logger.info(f"Converted AEST time: {timestamp_str}")
    except Exception as e:
        logger.exception("Failed to convert timestamp.")
        return {'statusCode': 500, 'body': json.dumps("Timestamp conversion failed")}
 
    # Format currency data
    quotes = data.get('quotes', {})
    # Each row: BASE_CURRENCY, COUNTER_CURRENCY, TIMESTAMP, RATE (all as string)
    new_rows = [[pair[:3], pair[3:], timestamp_str, str(rate)] for pair, rate in quotes.items()]
 
    # Prepare file name and key
    filename = f"forex_data_{aest_dt.strftime('%Y_%m')}.csv"
    key = f"{prefix}{filename}" if prefix else filename
    s3 = boto3.client('s3')
 
    # Attempt to load existing data from S3
    try:
        existing_obj = s3.get_object(Bucket=bucket, Key=key)
        existing_content = existing_obj['Body'].read().decode('utf-8')
        existing_reader = list(csv.reader(existing_content.splitlines()))
        header = existing_reader[0]
        existing_rows = existing_reader[1:]  # List preserves order
        existing_set = set(tuple(row) for row in existing_rows)
    except s3.exceptions.NoSuchKey:
        logger.info(f"No existing file found: {key}")
        header = ['BASE_CURRENCY', 'COUNTER_CURRENCY', 'TIMESTAMP', 'RATE']
        existing_rows = []
        existing_set = set()
    except (ClientError, BotoCoreError) as e:
        logger.exception("Error reading from S3.")
        return {'statusCode': 500, 'body': json.dumps("S3 read error")}
 
    # Deduplicate: Only add rows that aren't already in the file
    new_unique_rows = [r for r in new_rows if tuple(r) not in existing_set]
    if not new_unique_rows:
        logger.info("No new data to append.")
        return {'statusCode': 200, 'body': json.dumps("No new data. File unchanged.")}
 
    # Write updated CSV to memory with new rows at the top
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(header)
    writer.writerows(new_unique_rows)    # New data at top
    writer.writerows(existing_rows)      # Existing data below
 
    # Upload to S3
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=output.getvalue())
        logger.info(f"Uploaded updated file to S3: {key}")
    except (ClientError, BotoCoreError) as e:
        logger.exception("Error uploading to S3.")
        return {'statusCode': 500, 'body': json.dumps("S3 upload error")}
 
    return {
        'statusCode': 200,
        'body': json.dumps(f"Updated file uploaded to s3://{bucket}/{key} with {len(new_unique_rows)} new rows (at top).")
    }