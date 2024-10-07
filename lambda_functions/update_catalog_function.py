import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    glue = boto3.client('glue')

    database_name = 'logs_database'
    table_name = 'logs_table'
    
    try:
        metadata = event['metadata']
        logger.info(f"Received metadata: {json.dumps(metadata)}")
        
        columns = metadata['columns']
        partition_keys = [{'Name': key, 'Type': 'string'} for key in metadata['partition_keys']]
        
        logger.info(f"Preparing to create or update table: {table_name} in database: {database_name}")
        
        table_input = {
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': columns,
                'Location': metadata['s3_location'].rsplit('/', 1)[0],
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe',
                    'Parameters': {
                        'ignore.malformed.json': 'true',
                        'dots.in.keys': 'false',
                        'case.insensitive': 'true',
                        'mapping': 'true'
                    }
                },
                'Compressed': False,
                'NumberOfBuckets': -1,
                'StoredAsSubDirectories': False
            },
            'PartitionKeys': partition_keys,
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'json',
                'compressionType': 'none',
                'typeOfData': 'log_data'
            }
        }
        

        try:
            glue.get_table(DatabaseName=database_name, Name=table_name)
            logger.info(f"Table {table_name} exists, updating it.")
            glue.update_table(DatabaseName=database_name, TableInput=table_input)
        except glue.exceptions.EntityNotFoundException:
            logger.info(f"Table {table_name} does not exist, creating it.")
            glue.create_table(DatabaseName=database_name, TableInput=table_input)

        # Extract partition values and register partition
        partition_values = extract_partition_values(metadata['s3_location'], metadata['partition_keys'])
        logger.info(f"Partition values extracted: {partition_values}")
        if partition_values:
            register_partition(glue, database_name, table_name, partition_values, metadata['s3_location'])
        
        logger.info(f"Logs stored at S3 location: {metadata['s3_location']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Data Catalog updated successfully')
        }
    except Exception as e:
        logger.error(f"Error updating Data Catalog: {str(e)}")
        raise

def extract_partition_values(s3_key, partition_keys):
    parts = s3_key.split('/')
    values = []
    for key in partition_keys:
        for part in parts:
            if part.startswith(f"{key}="):
                values.append(part.split('=')[1])
                break
    return values

def register_partition(glue_client, database_name, table_name, partition_values, s3_location):
    try:
        partition_input = {
            'Values': partition_values,
            'StorageDescriptor': {
                'Location': f"{s3_location.rsplit('/', 1)[0]}",
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe',
                    'Parameters': {
                        'ignore.malformed.json': 'true',
                        'dots.in.keys': 'false',
                        'case.insensitive': 'true',
                        'mapping': 'true'
                    }
                },
            }
        }
        
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input
        )
        logger.info(f"Created partition: {partition_values}")
    except glue_client.exceptions.AlreadyExistsException:
        logger.info(f"Partition already exists: {partition_values}")
    except Exception as e:
        logger.error(f"Error creating partition: {str(e)}")
        raise

