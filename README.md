# AWS Serverless Log Processing with Cost-Effective Step Function Solution

## Project Overview
This project demonstrates a serverless architecture that efficiently generates, processes, and queries logs using AWS services, with a particular focus on a **Step Function-based solution** that replaces the more expensive Glue Crawler. This innovation significantly reduces costs while maintaining functionality, making it a standout solution for log management at scale.

## Key Features
- **Cost Optimization**: Replacing AWS Glue Crawler with a Step Function for metadata extraction and catalog updates, reducing operational costs.
- **Serverless Architecture**: Utilizing Lambda, CloudWatch, Kinesis Firehose, S3, EventBridge, and Athena for a highly scalable log processing workflow.
- **Flexible Log Querying**: Athena queries the logs stored in S3, providing flexibility in data analysis.

## Architecture
![Architecture Diagram](./architecture/architecture_diagram.png)

### Main Components:
1. **Lambda for Log Generation**: Generates logs and sends them to CloudWatch.
2. **CloudWatch Logs**: Stores logs generated by the Lambda function.
3. **Kinesis Firehose**: Streams logs from CloudWatch to S3, where they are stored with structured prefixes.
4. **EventBridge**: Triggers a Step Function when a new file is added to S3.
5. **Step Function for Log Processing**: The heart of the project, replacing the Glue Crawler:
   - **First Lambda Function**: Extracts metadata from the new log files.
   - **Second Lambda Function**: Updates the Glue Data Catalog with the extracted metadata, enabling querying in Athena.
6. **Glue Data Catalog**: Acts as a metadata repository for the logs stored in S3.
7. **Amazon Athena**: Enables querying of the logs via SQL-like queries, with results stored in another S3 bucket.

## Step Function: A Cost-Effective Alternative to Glue Crawler
One of the primary challenges addressed by this project is the **high cost** associated with running AWS Glue Crawlers for metadata extraction. Instead, this project uses a **Step Function** that orchestrates two Lambda functions to extract metadata and update the Glue Data Catalog. This approach achieves the same result while reducing costs significantly, especially in large-scale log processing.

### Why Step Function Over Glue Crawler?
- **Cost Efficiency**: Glue Crawlers charge based on time, and for larger datasets, they can become expensive. By using a Step Function, you're only charged for the execution time of the Lambda functions.
- **Customizable Workflows**: The Step Function allows for greater control and flexibility, making it easier to tailor the metadata extraction and catalog update process to specific use cases.
- **Scalability**: The serverless nature of Lambda functions ensures that the solution can scale dynamically with log volume.

## Workflow
1. **Log Generation**: Logs are generated by a Lambda function and sent to CloudWatch Logs.
2. **Log Streaming**: CloudWatch forwards logs to Kinesis Firehose via a subscription filter, which stores them in an S3 bucket with organized prefixes.
3. **EventBridge Trigger**: When new logs are saved in S3, EventBridge triggers the Step Function.
4. **Step Function Orchestration**:
   - **First Lambda**: Parses the new logs and extracts metadata.
   - **Second Lambda**: Updates the Glue Data Catalog with the extracted metadata.
5. **Athena Queries**: Logs are queried in Athena using the Glue Data Catalog as a reference. Query results are also saved in a separate S3 bucket.

## AWS Services Used
- **Lambda**: For log generation and processing in the Step Function.
- **CloudWatch Logs**: Stores the logs generated by Lambda.
- **Kinesis Firehose**: Streams logs from CloudWatch to S3.
- **S3**: Stores logs and query results.
- **EventBridge**: Detects new log files in S3 and triggers the Step Function.
- **Step Function**: Orchestrates two Lambda functions for metadata extraction and Glue Data Catalog updates.
- **Glue Data Catalog**: Serves as the metadata repository.
- **Athena**: Queries logs stored in S3 using the metadata in the Glue Data Catalog.

## Conclusion
This project showcases a **cost-effective alternative** to the traditional Glue Crawler by using a Step Function for log processing. The approach significantly reduces costs, enhances flexibility, and ensures scalability, making it an ideal solution for large-scale, serverless log processing workflows.
