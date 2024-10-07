import json
import random
import boto3
import datetime

def generate_log():
    log_types = ["INFO", "WARNING", "ERROR"]
    actions = ["LOGIN", "LOGOUT", "UPDATE", "DELETE", "CREATE"]

    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "log_level": random.choice(log_types),
        "action": random.choice(actions),
        "userid": f"user{random.randint(1, 1000)}",
        "message": f"User performed {random.choice(actions)} action"
    }

def lambda_handler(event, context):
    # Generate 2 logs per invocation
    logs = [generate_log() for _ in range(2)]  

    cloudwatch = boto3.client('logs')
    log_group = "/aws/lambda/generated-logs"
    log_stream = f"stream-{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"

    # Create log group if it doesn't exist
    try:
        cloudwatch.create_log_group(logGroupName=log_group)
        print(f"Created log group: {log_group}")
    except cloudwatch.exceptions.ResourceAlreadyExistsException:
        print(f"Log group already exists: {log_group}")

    # Create log stream
    try:
        cloudwatch.create_log_stream(logGroupName=log_group, logStreamName=log_stream)
        print(f"Created log stream: {log_stream}")
    except cloudwatch.exceptions.ResourceAlreadyExistsException:
        print(f"Log stream already exists: {log_stream}")

    # Put log events into CloudWatch
    try:
        response = cloudwatch.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[{
                "timestamp": int(datetime.datetime.now().timestamp() * 1000), 
                "message": json.dumps(log)
            } for log in logs]
        )
        print(f"Successfully put {len(logs)} log events")
    except Exception as e:
        print(f"Error putting log events: {str(e)}")
        raise

    return {
        'statusCode': 200,
        'body': json.dumps(f'Generated and stored {len(logs)} logs')
    }
