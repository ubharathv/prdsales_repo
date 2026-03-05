#!/usr/bin/env python3
"""
================================================================================
Script Name: {env}_dev_prdsales_gen_search_rev_glue

PrdSales Data Pipeline - Glue Job for Search Revenue Analysis
================================================================================

Process Overview: 
    Job that processes large sales files (>= 2GB) and
    generates search keyword performance analytics. Reads CSV files from S3,
    performs data transformations, and outputs tab-delimited results.

Created By : DE Team

Modification History:
    ID  Change Date Developer   JIRA/Request    Change Reason
    01  2026-03-05  Bharath U   JIRA-1234       Initial Version

Dependencies:
    - AWS Glue runtime environment
    - PySpark libraries
    - S3 input/output buckets
    - IAM role with S3 and Glue permissions

Usage:
    - Handles large files (multi-GB) with automatic Spark scaling
    - Input: CSV files from S3 source bucket
    - Output: Tab-delimited files to S3 output bucket.

================================================================================
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


class SearchRevenueProcessor:
    """
    Main class for processing search revenue analytics from product sales data.
    Handles large files using PySpark and outputs search keyword performance metrics.
    """
    
    def __init__(self, spark_session, glue_context):
        """
        Initialize the SearchRevenueProcessor with Spark and Glue contexts.
        
        Args:
            spark_session: SparkSession instance
            glue_context: GlueContext instance
        """
        self.spark = spark_session
        self.glue_context = glue_context
        self.site_domain = 'esshopzilla.com'
        self.s3_client = boto3.client('s3', region_name='us-east-2')
        
        # Register UDFs during initialization
        self._register_udfs()
    
    def _register_udfs(self):
        """Register User Defined Functions for Spark processing."""
        # Create static functions to avoid serialization issues
        def extract_domain_static(url):
            return SearchRevenueProcessor.extract_domain_static(url)
        
        def extract_keyword_static(referrer_url):
            return SearchRevenueProcessor.extract_search_keyword_static(referrer_url)
        
        def extract_revenue_static(product_list):
            return SearchRevenueProcessor.extract_revenue_static(product_list)
        
        def is_search_static(referrer_url):
            return SearchRevenueProcessor.is_search_engine_static(referrer_url)
        
        self.extract_domain_udf = udf(extract_domain_static, StringType())
        self.extract_keyword_udf = udf(extract_keyword_static, StringType())
        self.extract_revenue_udf = udf(extract_revenue_static, FloatType())
        self.is_search_udf = udf(is_search_static, BooleanType())
    
    @staticmethod
    def extract_domain_static(url):
        """Static method: Extract domain from URL."""
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
    
    @staticmethod
    def extract_search_keyword_static(referrer_url):
        """Static method: Extract search keyword from referrer URL."""
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
    
    @staticmethod
    def extract_revenue_static(product_list):
        """Static method: Extract revenue from product_list field."""
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
    
    @staticmethod
    def is_search_engine_static(referrer_url):
        """Static method: Check if referrer is a search engine (external with search params)."""
        if not referrer_url:
            return False
        
        try:
            parsed = urllib.parse.urlparse(referrer_url)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Must be external domain (not our site)
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
    
    def extract_domain(self, url):
        """Extract domain from URL."""
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
    
    def extract_search_keyword(self, referrer_url):
        """Extract search keyword from referrer URL."""
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
    
    def extract_revenue(self, product_list):
        """Extract revenue from product_list field."""
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
    
    def is_search_engine(self, referrer_url):
        """Check if referrer is a search engine (external with search params)."""
        if not referrer_url:
            return False
        
        try:
            parsed = urllib.parse.urlparse(referrer_url)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Must be external domain (not our site)
            if domain == self.site_domain:
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
    
    def load_data(self, input_path):
        """
        Load data from S3 path.
        
        Args:
            input_path (str): S3 path to input file
            
        Returns:
            DataFrame: Loaded Spark DataFrame
        """
        print(f"Loading data from: {input_path}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .csv(input_path)
        
        record_count = df.count()
        print(f"Loaded {record_count:,} records")
        return df
    
    def enrich_data(self, df):
        """
        Add derived columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Enriched DataFrame with additional columns
        """
        print("Processing data with Spark transformations...")
        
        df_enriched = df.withColumn(
            "domain", self.extract_domain_udf(col("referrer"))
        ).withColumn(
            "search_keyword", self.extract_keyword_udf(col("referrer"))
        ).withColumn(
            "revenue", self.extract_revenue_udf(col("product_list"))
        ).withColumn(
            "is_search_referral", self.is_search_udf(col("referrer"))
        ).withColumn(
            "is_purchase", col("event_list") == "1"
        )
        
        return df_enriched
    
    def find_search_sessions(self, df_enriched):
        """
        Find users who came from search engines.
        
        Args:
            df_enriched: Enriched DataFrame
            
        Returns:
            DataFrame: Search sessions DataFrame
        """
        search_sessions = df_enriched.filter(
            col("is_search_referral") == True
        ).select(
            col("ip"),
            col("domain").alias("search_domain"),
            col("search_keyword")
        ).dropDuplicates(["ip"])
        
        search_count = search_sessions.count()
        print(f"Found {search_count:,} search sessions")
        return search_sessions
    
    def find_purchase_sessions(self, df_enriched):
        """
        Find users who made purchases.
        
        Args:
            df_enriched: Enriched DataFrame
            
        Returns:
            DataFrame: Purchase sessions DataFrame
        """
        purchase_sessions = df_enriched.filter(
            (col("is_purchase") == True) & (col("revenue") > 0)
        ).groupBy("ip").agg(
            collect_list("revenue").alias("purchase_amounts")
        )
        
        purchase_count = purchase_sessions.count()
        print(f"Found {purchase_count:,} purchase sessions")
        return purchase_sessions
    
    def create_final_results(self, search_sessions, purchase_sessions):
        """
        Join search and purchase sessions to create final results.
        
        Args:
            search_sessions: DataFrame of search sessions
            purchase_sessions: DataFrame of purchase sessions
            
        Returns:
            DataFrame: Final results DataFrame
        """
        # Join search and purchase sessions
        results = search_sessions.join(purchase_sessions, on="ip", how="inner")
        
        # Create final results (one row per purchase)
        final_results = results.select(
            col("search_domain").alias("Search Engine Domain"),
            col("search_keyword").alias("Search Keyword"),
            explode(col("purchase_amounts")).alias("Revenue")
        ).filter(
            col("Search Keyword").isNotNull() & 
            col("Search Engine Domain").isNotNull()
        ).orderBy(col("Revenue").desc())
        
        return final_results
    
    def save_results(self, final_results, output_bucket, output_key):
        """
        Save results to S3 with proper filename.
        
        Args:
            final_results: DataFrame to save
            output_bucket: S3 bucket name
            output_key: S3 key for output file
        """
        print(f"Saving results to: s3://{output_bucket}/{output_key}")
        
        # Extract path components
        path_parts = output_key.rsplit('/', 1)
        output_path = f"s3://{output_bucket}/{path_parts[0]}/"
        filename = path_parts[1]
        
        # Save to temp location first
        temp_path = f"{output_path}temp/"
        final_results.coalesce(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", "\t") \
            .csv(temp_path)
        
        # Rename file to exact filename using S3 operations
        self._rename_output_file(output_bucket, path_parts[0], filename)
        
        print(f"Results saved successfully to: s3://{output_bucket}/{output_key}")
    
    def _rename_output_file(self, bucket, path_prefix, target_filename):
        """
        Rename the Spark output file to the target filename.
        
        Args:
            bucket: S3 bucket name
            path_prefix: Path prefix without filename
            target_filename: Desired filename
        """
        temp_prefix = f"{path_prefix}/temp/"
        
        # List files in temp directory
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv') and 'part-' in obj['Key']:
                    # Found the part file, copy it to correct location
                    source_key = obj['Key']
                    target_key = f"{path_prefix}/{target_filename}"
                    
                    print(f"Copying {source_key} to {target_key}")
                    
                    # Copy the file
                    self.s3_client.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': source_key},
                        Key=target_key
                    )
                    
                    # Delete the temp file
                    self.s3_client.delete_object(Bucket=bucket, Key=source_key)
                    break
        
        # Clean up temp directory
        self._cleanup_temp_directory(bucket, temp_prefix)
    
    def _cleanup_temp_directory(self, bucket, temp_prefix):
        """Clean up temporary directory after file operations."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=temp_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
        except Exception as e:
            print(f"Warning: Could not clean up temp directory: {e}")
    
    def print_summary(self, final_results):
        """
        Print processing summary and statistics.
        
        Args:
            final_results: Final results DataFrame
        """
        print("\nSample Results:")
        print("-" * 50)
        final_results.show(20, truncate=False)
        
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
        else:
            print("No matching records found.")
    
    def invoke_tracking_lambda(self, file_key, status='COMPLETE', error_message=None):
        """
        Invoke the tracking Lambda to record completion status.
        
        Args:
            file_key: S3 file key being processed
            status: Processing status (COMPLETE/FAILED)
            error_message: Error message if status is FAILED
        """
        try:
            print(f"=== LAMBDA INVOCATION START ===")
            print(f"Invoking tracking Lambda for file: {file_key}")
            print(f"Status: {status}")
            
            # Extract file date from file_key
            file_date = self.extract_file_date(file_key)
            
            # Prepare status payload
            payload = {
                'file_date': file_date,
                'file_key': file_key,
                'bucket': 'prdsales-s3-src-bucket',
                'content_type': 'text/csv',
                'event_type': 'Glue',
                'file_datetime': datetime.utcnow().isoformat() + 'Z',
                'file_size_gb': '',
                'file_status': status
            }
            
            if error_message:
                payload['error_message'] = error_message
            
            # Initialize Lambda client
            lambda_client = boto3.client('lambda', region_name='us-east-2')
            
            # Invoke Lambda function
            response = lambda_client.invoke(
                FunctionName='get_prdsales_file_lambda',
                InvocationType='Event',
                Payload=json.dumps(payload)
            )
            
            print(f"Lambda invocation successful! StatusCode: {response['StatusCode']}")
            print(f"=== LAMBDA INVOCATION END ===")
            return True
            
        except Exception as e:
            print(f"=== LAMBDA INVOCATION ERROR ===")
            print(f"Error invoking tracking Lambda: {e}")
            print(f"=== LAMBDA INVOCATION ERROR END ===")
            return False
    
    def extract_file_date(self, file_key):
        """
        Extract date from S3 file key.
        Expected format: prdsales/year=YYYY/month=MM/day=DD/filename
        
        Args:
            file_key: S3 file key
            
        Returns:
            str: Extracted date in YYYY-MM-DD format
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
                return datetime.now().strftime('%Y-%m-%d')
                
        except Exception as e:
            print(f"Error extracting date from key {file_key}: {e}")
            return datetime.now().strftime('%Y-%m-%d')
    
    def process_file(self, file_key):
        """
        Main processing method that orchestrates the entire workflow.
        
        Args:
            file_key: S3 file key to process
            
        Returns:
            bool: True if processing successful, False otherwise
        """
        try:
            print("AWS Glue PySpark Search Revenue Analyzer")
            print("=" * 60)
            print(f"Processing file: {file_key}")
            
            # Generate output paths
            now = datetime.now()
            current_date = now.strftime('%Y-%m-%d')
            year = now.strftime('%Y')
            month = now.strftime('%m')
            day = now.strftime('%d')
            
            filename = file_key.split('/')[-1]
            outfilename = f'{current_date}_SearchKeywordPerformance.tab'
            
            # S3 paths
            input_bucket = 'prdsales-s3-src-bucket'
            input_path = f's3://{input_bucket}/{file_key}'
            output_bucket = 'prdsales-s3-out-bucket'
            output_key = f'tgt/year={year}/month={month}/date={day}/{outfilename}'
            
            print(f"Input path: {input_path}")
            print(f"Output path: s3://{output_bucket}/{output_key}")
            
            # Process data
            df = self.load_data(input_path)
            df_enriched = self.enrich_data(df)
            search_sessions = self.find_search_sessions(df_enriched)
            purchase_sessions = self.find_purchase_sessions(df_enriched)
            final_results = self.create_final_results(search_sessions, purchase_sessions)
            
            # Print summary
            self.print_summary(final_results)
            
            # Save results
            self.save_results(final_results, output_bucket, output_key)
            
            print("\nGlue job completed successfully!")
            
            # Record completion
            self.invoke_tracking_lambda(file_key, status='COMPLETE')
            
            return True
            
        except Exception as e:
            print(f"Error in processing: {e}")
            import traceback
            traceback.print_exc()
            
            # Record failure
            self.invoke_tracking_lambda(file_key, status='FAILED', error_message=str(e))
            
            return False


def main():
    """Main function that initializes Glue context and runs the processor."""
    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    
    try:
        print("=== GLUE JOB INPUT DEBUG ===")
        print(f"sys.argv: {sys.argv}")
        
        # Get job parameters - file_key is required
        args = getResolvedOptions(sys.argv, ['file_key'])
        file_key = args['file_key']
        print(f"Received file_key parameter: {file_key}")
        
        # Create processor instance and run
        processor = SearchRevenueProcessor(spark, glue_context)
        success = processor.process_file(file_key)
        
        if success:
            print("Processing completed successfully!")
        else:
            print("Processing failed!")
            
    except Exception as e:
        print(f"Error in main: {e}")
        import traceback
        traceback.print_exc()
        return False
            
    except Exception as e:
        print(f"Error in main: {e}")
        if "file_key" in str(e) or "getResolvedOptions" in str(e):
            print("CRITICAL ERROR: Required parameter 'file_key' not provided")
            print("Job cannot proceed without file_key parameter")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # job.commit()  # Uncomment when using as actual Glue job
        spark.stop()


if __name__ == "__main__":
    main()