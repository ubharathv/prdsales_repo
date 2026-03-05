"""
================================================================================
Script Name: {env}_get_prdsales_file_lambda
================================================================================

Process Overview:
    PrdSales Data Pipeline - File Processing Lambda Function 
    that handles S3 file events and determines processing type
    based on file size. Routes files < 2GB to Lambda processing and files >= 2GB
    to Glue job processing. Records all file events in DynamoDB for tracking.

Created By : DE Team

Modification History:
    ID  Change Date Developer   JIRA/Request    Change Reason
    01  2026-03-05  Bharath U   JIRA-1234       Initial Version

Notes/Dependencies:
    - boto3 (AWS SDK)
    - DynamoDB table: prdssales_file_evnt
    - Step Function: {env}_StepFun_prdsales_process_file
    - IAM permissions for S3, DynamoDB, and Step Functions
================================================================================
"""

import json
import urllib.parse
import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# DynamoDB table name
TABLE_NAME = 'prdssales_file_evnt'


def lambda_handler(event, context):
    """
    Generic Lambda to track file processing events in DynamoDB
    
    Supports two invocation modes:
    1. S3 Event Trigger: Automatically triggered when files are uploaded to S3
    2. Direct Invocation: Called directly with filename and status parameters
    
    Direct invocation format:
    {
        "filename": "PrdSalesRevenue.csv",
        "status": "PROCESSING|SUCCESS|FAILED",
        "event_type": "LAMBDA|GLUE|S3",
        "file_date": "2026-03-03" (optional),
        "record_count": 150000 (optional),
        "output_location": "s3://bucket/path/file.tab" (optional),
        "error_message": "Error details" (optional for FAILED status)
    }
    """
    
    try:
        print("Lambda invoked with S3 file event")
        
        # Check if this is an S3 event or direct invocation
        if 'Records' in event and len(event['Records']) > 0 and 's3' in event['Records'][0]:
            # S3 Event Mode
            return handle_s3_event(event, context)
        else:
            # Direct Invocation Mode
            return handle_direct_invocation(event, context)
            
    except Exception as e:
        print(f'Error in Lambda execution: {e}')
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'message': 'Lambda execution failed'
            }
        }


def handle_s3_event(event, context):
    """
    Handle S3 event-triggered invocation
    Determines processing type based on file size:
    - Files < 2GB: Use Lambda processing (event_type='LAMBDA')
    - Files >= 2GB: Use Glue processing (event_type='GLUE')
    """
    try:
        # Get the object from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        event_name = event['Records'][0]['eventName']
        
        print(f'S3 Event: {event_name}')
        print(f'File received: {key}')
        print(f'Bucket: {bucket}')
        
        # Extract file date from key
        file_date = extract_file_date(key)
        
        # Get current timestamp
        current_datetime = datetime.utcnow().isoformat() + 'Z'
        
        # Get file info from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        content_type = response['ContentType']
        file_size = response['ContentLength']
        
        # Convert file size to GB for easier reading
        file_size_gb = file_size / (1024 * 1024 * 1024)
        
        print(f"Content Type: {content_type}")
        print(f"File Size: {file_size} bytes ({file_size_gb:.2f} GB)")
        
        # Determine processing type based on file size
        size_threshold_bytes = 2 * 1024 * 1024 * 1024  # 2GB in bytes
        
        if file_size < size_threshold_bytes:
            trigger_type = 'Lambda'
            print(f"File size ({file_size_gb:.2f} GB) < 2GB - Recommended processing: LAMBDA")
        else:
            trigger_type = 'Glue'
            print(f"File size ({file_size_gb:.2f} GB) >= 2GB - Recommended processing: GLUE")
        
        # Record file event in DynamoDB with determined processing type
        record_file_event(
            file_date=file_date,
            file_key=key,
            datetime=current_datetime,
            status='RECEIVED',
            event_type='S3',
            bucket=bucket,
            content_type=content_type,
            file_size=file_size
        )
        
        # Trigger Step Function for processing
        step_function_response = trigger_processing_job(bucket, key, trigger_type, file_size_gb)
        
        # Return in the requested format
        return {
            "file_key": key,
            "trigger_type": trigger_type,
            "file_date": file_date,
            "bucket": bucket,
            "content_type": content_type,
            "event_type": trigger_type,
            "file_datetime": current_datetime,
            "file_size_gb": round(file_size_gb, 2),
            "file_status": "Inprogress"
        }
        
    except Exception as e:
        print(f'Error processing S3 event: {e}')
        
        # Record error in DynamoDB if possible
        try:
            if 'key' in locals():
                record_file_event(
                    file_date=extract_file_date(key) if 'key' in locals() else datetime.utcnow().strftime('%Y-%m-%d'),
                    file_key=key if 'key' in locals() else 'unknown',
                    datetime=datetime.utcnow().isoformat() + 'Z',
                    status='ERROR',
                    event_type='S3',
                    error_message=str(e)
                )
        except:
            pass  #For now not failing if Dynamo DB entry is not done
        
        raise e


def handle_direct_invocation(event, context):
    """
    Handle direct Lambda invocation with completion payload
    
    Expected payload format:
    {
        "file_date": "2026-03-03",
        "file_key": "prdsales/web/PrdSalesRevenue.csv",
        "bucket": "prdsales-s3-src-bucket",
        "content_type": "text/csv",
        "event_type": "GLUE",
        "file_datetime": "2026-03-03T10:30:45.123Z",
        "file_size_gb": 1.25,
        "file_status": "COMPLETE"
    }
    """
    try:
        print(f'Direct invocation - Processing payload')
        print(f'Event payload: {json.dumps(event, indent=2)}')
        
        # Extract parameters from payload
        file_date = event.get('file_date')
        file_key = event.get('file_key')
        bucket = event.get('bucket')
        content_type = event.get('content_type')
        event_type = event.get('event_type')
        file_datetime = event.get('file_datetime')
        file_size_gb = event.get('file_size_gb')
        file_status = event.get('file_status')
        error_message = event.get('error_message')

        # Validate required parameters
        if not file_key:
            raise ValueError("Missing required parameter: file_key")
        if not file_status:
            raise ValueError("Missing required parameter: file_status")
        if not event_type:
            raise ValueError("Missing required parameter: event_type")
        if not file_date:
            raise ValueError("Missing required parameter: file_date")
        
        print(f'Processing payload: File={file_key}, Status={file_status}, Type={event_type}')
        
        # Use provided file_datetime or generate current timestamp
        if not file_datetime:
            file_datetime = datetime.utcnow().isoformat() + 'Z'
        
        # Record event in DynamoDB
        record_file_event(
            file_date=file_date,
            file_key=file_key,
            datetime=file_datetime,
            status=file_status,
            event_type=event_type,
            bucket=bucket,
            content_type=content_type,
            file_size_gb=file_size_gb,
            error_message=error_message
        )
        
        # Return in the requested format
        return {
            "file_key": file_key,
            "trigger_type": event_type,
            "file_date": file_date,
            "bucket": bucket if bucket else "prdsales-s3-src-bucket",
            "content_type": content_type if content_type else "text/csv",
            "event_type": event_type,
            "file_datetime": file_datetime,
            "file_size_gb": file_size_gb if file_size_gb else "",
            "file_status": file_status
        }
        
    except Exception as e:
        print(f'Error processing payload: {e}')
        import traceback
        traceback.print_exc()
        raise e


def extract_file_date(file_key):
    """
    Extract date from S3 file key
    Expected format: prdsales/year=YYYY/month=MM/day=DD/filename
    """
    try:
        parts = file_key.split('/')
        year = None
        month = None
        day = None
        
        for part in parts:
            if part.startswith('year='):
                year = part.split('=')[1]
            elif part.startswith('month='):
                month = part.split('=')[1].zfill(2)
            elif part.startswith('day='):
                day = part.split('=')[1].zfill(2)
        
        if year and month and day:
            return f"{year}-{month}-{day}"
        else:
            # Fallback to current date if can't parse
            return datetime.utcnow().strftime('%Y-%m-%d')
            
    except Exception as e:
        print(f"Error extracting date from key {file_key}: {e}")
        # Fallback to current date
        return datetime.utcnow().strftime('%Y-%m-%d')


def record_file_event(file_date, file_key, datetime, status, bucket=None, content_type=None, file_size=None, file_size_gb=None, error_message=None, event_type=None, record_count=None, output_location=None):
    """
    Record file processing event in DynamoDB
    """
    try:
        table = dynamodb.Table(TABLE_NAME)
        
        # Prepare item with exact column names from your table
        item = {
            'file_date': file_date,
            'file_key': file_key,
            'file_datetime': datetime,
            'file_status': status,
            'event_type': event_type if event_type else ''
        }
        
        # Add optional attributes if provided (using additional columns not in your schema)
        if bucket:
            item['bucket'] = bucket
        if content_type:
            item['content_type'] = content_type
        if file_size:
            item['file_size'] = file_size
        if file_size_gb:
            item['file_size_gb'] = file_size_gb
        if error_message:
            item['error_message'] = error_message
        if record_count:
            item['record_count'] = record_count
        if output_location:
            item['output_location'] = output_location
        
        # Write to DynamoDB
        response = table.put_item(Item=item)
        
        print(f"DynamoDB record created: {file_date} / {file_key} / {status}")
        return response
        
    except Exception as e:
        print(f"Error writing to DynamoDB: {e}")
        raise e


def trigger_processing_job(bucket, key, trigger_type, file_size_gb):
    """
    Trigger Step Function for file processing
    """
    try:
        # Initialize Step Functions client
        stepfunctions_client = boto3.client('stepfunctions')
        
        # Get current account ID and region dynamically
        sts_client = boto3.client('sts')
        account_id = sts_client.get_caller_identity()['Account']
        region = boto3.Session().region_name or 'us-east-2'
        
        # Step Function ARN (dynamic with dev_ prefix)
        state_machine_arn = f'arn:aws:states:{region}:{account_id}:stateMachine:dev_StepFun_prdsales_process_file'
        
        # Prepare payload for Step Function with only required arguments
        payload = {
            'file_key': key,
            'trigger_type': trigger_type
        }
        
        # Generate execution name (must be unique)
        execution_name = f"file-processing-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{key.split('/')[-1].replace('.', '-')}"
        
        print(f"Triggering Step Function: {state_machine_arn}")
        print(f"Execution name: {execution_name}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        # Start Step Function execution
        response = stepfunctions_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps(payload)
        )
        
        execution_arn = response['executionArn']
        print(f"Step Function execution started successfully: {execution_arn}")
        
        return {
            'execution_arn': execution_arn,
            'execution_name': execution_name,
            'state_machine_arn': state_machine_arn,
            'status': 'STARTED'
        }
        
    except Exception as e:
        print(f"Error triggering Step Function: {e}")
        import traceback
        traceback.print_exc()
        
        # Return error info but don't raise - this shouldn't fail the main Lambda
        return {
            'error': str(e),
            'status': 'FAILED'
        }