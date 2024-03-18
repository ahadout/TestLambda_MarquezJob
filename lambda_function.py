import boto3
import requests
import json
import os

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extracting event details
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = "marquez-silver-destination"
    
    # Copy the Parquet file from the source bucket to the destination bucket
    s3_client.copy_object(
        Bucket=destination_bucket,
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Key=source_key
    )
    
    # Assuming the copy operation was successful, log to Marquez
    log_to_marquez(source_bucket, source_key, destination_bucket)
    
    return {
        'statusCode': 200,
        'body': f'File {source_key} transferred from {source_bucket} to {destination_bucket}'
    }

def log_to_marquez(source_bucket, source_key, destination_bucket):
    marquez_url = "http://54.174.220.207:5000/api/v1"  # Update with your Marquez server details
    namespace = "lambdas"  # Update with your namespace
    source_dataset = f"{source_bucket}/{source_key}"
    destination_dataset = f"{destination_bucket}/{source_key}"
    job_name = "MarquezJob-2"  # Update with your Lambda function's name
    
    # Example JSON payloads (You'll need to adjust these according to your Marquez schema)
    job_payload = {
        "type": "BATCH",
        "inputs": [{"namespace": namespace, "name": source_dataset}],
        "outputs": [{"namespace": namespace, "name": destination_dataset}],
        "location": "https://dby2nfcbu6kcff3tfzk6qi5vwa0gjeor.lambda-url.us-east-1.on.aws/",
        "description": "Transfers Parquet files from source to destination S3 bucket"
    }
    
    # Post job to Marquez (simplified example; error handling omitted for brevity)
    job_response = requests.put(f"{marquez_url}/namespaces/{namespace}/jobs/{job_name}", json=job_payload)
    
    # Similarly, post datasets and runs as needed, following the Marquez API documentation
    
    print(f"Logged to Marquez: {job_response.status_code}")
    print(f"arn:aws:lambda:us-east-1:975050176907:function:{job_name}")