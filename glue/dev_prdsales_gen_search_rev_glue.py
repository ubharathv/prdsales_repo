#!/usr/bin/env python3
"""
AWS Glue PySpark Search Revenue Analyzer
Reads from S3, processes data, and outputs results

Usage:
- Can handle large files (multi-GB)
- Automatically scales with Spark cluster
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, sum as spark_sum, explode, udf, current_date, date_format
from pyspark.sql.types import StringType, FloatType, BooleanType
import urllib.parse
from datetime import datetime
import boto3
import json


def extract_domain(url):
    """Extract domain from URL"""
    if not url:
        return None
    try:
        parsed = urllib.parse.urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    except:
        return None


def extract_search_keyword(referrer_url):
    """Extract search keyword from referrer URL"""
    if not referrer_url:
        return None
    try:
        parsed = urllib.parse.urlparse(referrer_url)
        query_params = urllib.parse.parse_qs(parsed.query)
        
        # Common search parameter names
        search_params = ['q', 'query', 'p', 'search', 'keywords', 'k']
        
        for param in search_params:
            if param in query_params and query_params[param]:
                keyword = query_params[param][0]
                keyword = urllib.parse.unquote_plus(keyword)
                return keyword.strip()
    except:
        pass
    return None


def extract_revenue(product_list):
    """Extract revenue from product_list field"""
    if not product_list:
        return 0.0
    try:
        parts = product_list.split(';')
        if len(parts) >= 4:
            price_str = parts[3].strip()
            if price_str:
                return float(price_str)
    except:
        pass
    return 0.0


def is_search_engine(referrer_url):
    """Check if referrer is a search engine (external with search params)"""
    if not referrer_url:
        return False
    
    try:
        parsed = urllib.parse.urlparse(referrer_url)
        domain = parsed.netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        
        # Must be external domain (not esshopzilla)
        if domain == 'esshopzilla.com':
            return False
        
        # Must have search parameters
        query_params = urllib.parse.parse_qs(parsed.query)
        search_params = ['q', 'query', 'p', 'search', 'keywords', 'k']
        
        for param in search_params:
            if param in query_params and query_params[param]:
                return True
                
    except:
        pass
    return False


def invoke_tracking_lambda(file_key, status='COMPLETE', error_message=None):
    """
    Invoke the tracking Lambda to record completion status
    """
    try:
        print(f"=== LAMBDA INVOCATION START ===")
        print(f"Attempting to invoke tracking Lambda...")
        print(f"File key: {file_key}")
        print(f"Status: {status}")
        
        # Extract file date from file_key
        file_date = extract_file_date(file_key)
        print(f"Extracted file date: {file_date}")
        
        # Prepare status payload in the requested format
        payload = {
            'file_date': file_date,
            'file_key': file_key,
            'bucket': 'prdsales-s3-src-bucket',
            'content_type': 'text/csv',
            'event_type': 'GLUE',
            'file_datetime': datetime.utcnow().isoformat() + 'Z',
            'file_size_gb': '',  # Leave empty as requested
            'file_status': status
        }
        
        # Add error message if provided
        if error_message:
            payload['error_message'] = error_message
        
        print(f"Payload prepared:")
        print(json.dumps(payload, indent=2))
        
        # Initialize Lambda client with explicit region
        print("Initializing Lambda client...")
        lambda_client = boto3.client('lambda', region_name='us-east-2')
        
        # Lambda function name
        lambda_function_name = 'get_prdsales_file_lambda'
        print(f"Target Lambda function: {lambda_function_name}")
        
        # Convert payload to JSON string
        payload_json = json.dumps(payload)
        print(f"Payload JSON length: {len(payload_json)} characters")
        
        print("Invoking Lambda function...")
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=payload_json
        )
        
        print(f"Lambda invocation successful!")
        print(f"Response StatusCode: {response['StatusCode']}")
        print(f"Response Payload: {response.get('Payload', 'No payload')}")
        print(f"=== LAMBDA INVOCATION END ===")
        
        return True
        
    except Exception as e:
        print(f"=== LAMBDA INVOCATION ERROR ===")
        print(f"Error invoking tracking Lambda: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        print(f"=== LAMBDA INVOCATION ERROR END ===")
        return False


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
            return datetime.now().strftime('%Y-%m-%d')
            
    except Exception as e:
        print(f"Error extracting date from key {file_key}: {e}")
        # Fallback to current date
        return datetime.now().strftime('%Y-%m-%d')


def main():
    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    try:
        print("AWS Glue PySpark Search Revenue Analyzer")
        print("=" * 50)
        
        # Get job parameters - expecting file_key as input
        try:
            args = getResolvedOptions(sys.argv, ['file_key'])
            file_key = args['file_key']
            print(f"Received file_key parameter: {file_key}")
        except Exception as e:
            print(f"Error getting job parameters: {e}")
            # Fallback for testing - use hardcoded value
            file_key = "prdsales/web/PrdSalesRevenue.csv"
            print(f"Using fallback file_key: {file_key}")
        
        # Extract filename from file_key for output naming
        filename = file_key.split('/')[-1]
        print(f"Extracted filename: {filename}")
        
        # Generate output filename and path with current date
        now = datetime.now()
        current_date = now.strftime('%Y-%m-%d')
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        
        outfilename = f'{current_date}_SearchKeywordPerformance.tab'
        
        # S3 paths
        input_bucket = 'prdsales-s3-src-bucket'
        input_path = f's3://{input_bucket}/{file_key}'  # Use file_key directly
        output_bucket = 'prdsales-s3-out-bucket'
        output_path = f's3://{output_bucket}/tgt/year={year}/month={month}/date={day}/'
        
        print(f"Input path: {input_path}")
        print(f"Output path: {output_path}")
        
        # 1. Load data from S3
        print(f"Loading data from: {input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .csv(input_path)
        
        record_count = df.count()
        print(f"Loaded {record_count:,} records")
        
        # 2. Register UDFs
        extract_domain_udf = udf(extract_domain, StringType())
        extract_keyword_udf = udf(extract_search_keyword, StringType())
        extract_revenue_udf = udf(extract_revenue, FloatType())
        is_search_udf = udf(is_search_engine, BooleanType())
        
        # 3. Add derived columns
        print("Processing data with Spark transformations...")
        df_enriched = df.withColumn(
            "domain", extract_domain_udf(col("referrer"))
        ).withColumn(
            "search_keyword", extract_keyword_udf(col("referrer"))
        ).withColumn(
            "revenue", extract_revenue_udf(col("product_list"))
        ).withColumn(
            "is_search_referral", is_search_udf(col("referrer"))
        ).withColumn(
            "is_purchase", col("event_list") == "1"
        )
        
        # 4. Find search sessions (users who came from search engines)
        search_sessions = df_enriched.filter(
            col("is_search_referral") == True
        ).select(
            col("ip"),
            col("domain").alias("search_domain"),
            col("search_keyword")
        ).dropDuplicates(["ip"])
        
        search_count = search_sessions.count()
        print(f"Found {search_count:,} search sessions")
        
        # 5. Find purchase sessions (users who made purchases)
        purchase_sessions = df_enriched.filter(
            (col("is_purchase") == True) & (col("revenue") > 0)
        ).groupBy("ip").agg(
            collect_list("revenue").alias("purchase_amounts")
        )
        
        purchase_count = purchase_sessions.count()
        print(f"Found {purchase_count:,} purchase sessions")
        
        # 6. Join search and purchase sessions
        results = search_sessions.join(purchase_sessions, on="ip", how="inner")
        
        # 7. Create final results (one row per purchase)
        final_results = results.select(
            col("search_domain").alias("Search Engine Domain"),
            col("search_keyword").alias("Search Keyword"),
            explode(col("purchase_amounts")).alias("Revenue")
        ).filter(
            col("Search Keyword").isNotNull() & 
            col("Search Engine Domain").isNotNull()
        )
        
        # 8. Show sample results
        print("\nSample Results:")
        print("-" * 50)
        final_results.show(20, truncate=False)
        
        # 9. Summary statistics
        total_records = final_results.count()
        print(f"\nProcessing Summary:")
        print(f"Total Records: {total_records:,}")
        
        if total_records > 0:
            total_revenue = final_results.agg(spark_sum("Revenue")).collect()[0][0]
            unique_domains = final_results.select("Search Engine Domain").distinct().count()
            unique_keywords = final_results.select("Search Keyword").distinct().count()
            
            print(f"Total Revenue: ${total_revenue:,.2f}")
            print(f"Unique Search Domains: {unique_domains}")
            print(f"Unique Keywords: {unique_keywords}")
            
            # 10. Save results to S3 with exact filename and path
            print(f"\nSaving results to: {output_path}{outfilename}")
            
            # Method 1: Save to temp location first, then copy with correct name
            temp_path = f"{output_path}temp/"
            final_results.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", "\t") \
                .csv(temp_path)
            
            # Method 2: Use boto3 to rename the file to exact filename
            import boto3
            s3_client = boto3.client('s3')
            
            # List files in temp directory to find the part file
            temp_bucket = output_bucket
            temp_prefix = f"tgt/year={year}/month={month}/date={day}/temp/"
            
            response = s3_client.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                        # Found the part file, copy it to the correct location with correct name
                        source_key = obj['Key']
                        target_key = f"tgt/year={year}/month={month}/date={day}/{outfilename}"
                        
                        print(f"Copying {source_key} to {target_key}")
                        
                        # Copy the file
                        s3_client.copy_object(
                            Bucket=temp_bucket,
                            CopySource={'Bucket': temp_bucket, 'Key': source_key},
                            Key=target_key
                        )
                        
                        # Delete the temp directory
                        s3_client.delete_object(Bucket=temp_bucket, Key=source_key)
                        break
            
            # Clean up temp directory (delete any remaining files)
            try:
                response = s3_client.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
                if 'Contents' in response:
                    for obj in response['Contents']:
                        s3_client.delete_object(Bucket=temp_bucket, Key=obj['Key'])
            except:
                pass  # Ignore cleanup errors
            
            final_output_location = f"s3://{output_bucket}/tgt/year={year}/month={month}/date={day}/{outfilename}"
            print(f"Results saved successfully!")
            print(f"Final output location: {final_output_location}")
            
        else:
            print("No matching records found.")
            # Create empty output file with correct name
            empty_df = spark.createDataFrame([], final_results.schema)
            temp_path = f"{output_path}temp/"
            empty_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", "\t") \
                .csv(temp_path)
            
            # Rename empty file as well
            import boto3
            s3_client = boto3.client('s3')
            temp_bucket = output_bucket
            temp_prefix = f"tgt/year={year}/month={month}/date={day}/temp/"
            
            response = s3_client.list_objects_v2(Bucket=temp_bucket, Prefix=temp_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                        source_key = obj['Key']
                        target_key = f"tgt/year={year}/month={month}/date={day}/{outfilename}"
                        
                        s3_client.copy_object(
                            Bucket=temp_bucket,
                            CopySource={'Bucket': temp_bucket, 'Key': source_key},
                            Key=target_key
                        )
                        s3_client.delete_object(Bucket=temp_bucket, Key=source_key)
                        break
        
        print("\nGlue job completed successfully!")
        
        # Invoke tracking Lambda to record completion
        invoke_tracking_lambda(file_key, status='COMPLETE')
        
    except Exception as e:
        print(f"Error in Glue job: {e}")
        import traceback
        traceback.print_exc()
        
        # Invoke tracking Lambda to record failure
        try:
            invoke_tracking_lambda(file_key, status='FAILED', error_message=str(e))
        except:
            print("Failed to invoke tracking Lambda for error reporting")
    
    finally:
        # job.commit()  # Uncomment when using as actual Glue job
        spark.stop()


if __name__ == "__main__":
    main()