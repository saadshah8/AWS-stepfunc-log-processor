import json
import boto3
import io
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    bucket = event['detail']['bucket']['name']
    key = event['detail']['object']['key']
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        metadata = process_log_file(response['Body'])
        
        # Add S3 location to metadata
        metadata['s3_location'] = f"s3://{bucket}/{key}"
        
        # Extract partition keys from S3 key
        metadata['partition_keys'] = extract_partition_keys(key)
        
        logger.info(f"Extracted metadata: {json.dumps(metadata)}")
        
        return {
            'statusCode': 200,
            'metadata': metadata
        }
    except Exception as e:
        logger.error(f"Error processing log file: {str(e)}")
        raise

def extract_partition_keys(key):
    parts = key.split('/')
    partition_keys = []
    for part in parts:
        if '=' in part:
            partition_keys.append(part.split('=')[0])
    return partition_keys

def analyze_log_structure(log_entry, schema):
    for key, value in log_entry.items():
        if key not in schema:
            schema[key] = {'Name': key, 'Type': 'string'}  # Default to string type

def process_log_file(file_obj):
    metadata = {
        'log_count': 0,
        'start_timestamp': None,
        'end_timestamp': None,
        'schema': {},
        'logs': []
    }
    
    for line in io.TextIOWrapper(file_obj, encoding='utf-8'):
        try:
            log_entry = json.loads(line)
            if 'logEvents' in log_entry:
                for event in log_entry['logEvents']:
                    inner_log = json.loads(event['message'])
                    analyze_log_structure(inner_log, metadata['schema'])
                    process_log_entry(inner_log, metadata)
            else:
                analyze_log_structure(log_entry, metadata['schema'])
                process_log_entry(log_entry, metadata)
        except json.JSONDecodeError:
            logger.warning(f"Skipping invalid JSON line: {line}")
    
    metadata['columns'] = list(metadata['schema'].values())
    return metadata
    
    
def process_log_entry(log_entry, metadata):
    metadata['log_count'] += 1
    
    if 'timestamp' in log_entry:
        update_timestamp(metadata, log_entry['timestamp'])
    
    metadata['logs'].append(log_entry)

def update_timestamp(metadata, timestamp):
    try:
        dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        if metadata['start_timestamp'] is None or dt < datetime.strptime(metadata['start_timestamp'], "%Y-%m-%dT%H:%M:%S.%f"):
            metadata['start_timestamp'] = timestamp
        if metadata['end_timestamp'] is None or dt > datetime.strptime(metadata['end_timestamp'], "%Y-%m-%dT%H:%M:%S.%f"):
            metadata['end_timestamp'] = timestamp
    except ValueError:
        logger.warning(f"Invalid timestamp format: {timestamp}")

