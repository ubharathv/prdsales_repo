#!/usr/bin/env python3
"""
================================================================================
Script Name: {env}_dev_gen_search_revenue_lambda
================================================================================

Process Overview: 
    PrdSales Data Pipeline - Search Revenue Analysis Lambda Function
    that processes small to medium-sized product sales files 
    (< 2GB) and generates search keyword performance analytics. Reads CSV files 
    from S3, analyzes search engine referrals and revenue data, then outputs 
    tab-delimited results for BI.

Created By : DE Team

Modification History:
    ID  Change Date Developer   JIRA/Request    Change Reason
    01  2026-03-05  Bharath U   JIRA-1234       Initial Version

Notes/Dependencies:
    - boto3 (AWS SDK)
    - S3 input bucket: prdsales-s3-src-bucket
    - S3 output bucket: prdsales-s3-out-bucket
    - IAM permissions for S3 read/write operations
    - 15 minute execution limit
    - 10GB /tmp storage limit  
    - 3008MB memory limit
    - Suitable for files up to ~2GB (routed by file size check)
================================================================================
"""

import json
import boto3
import csv
import urllib.parse
from io import StringIO
import tempfile
import os
from datetime import datetime
from typing import Dict, List, Optional


class SearchRevenueAnalyzer:
    """
    Main class for processing search revenue analytics from product sales data.
    Handles small to medium files within Lambda constraints and outputs search 
    keyword performance metrics.
    """
    
    def __init__(self):
        """Initialize the SearchRevenueAnalyzer with AWS clients and configuration."""
        self.s3_client = boto3.client('s3')
        self.site_domain = 'esshopzilla.com'
        self.input_bucket = 'prdsales-s3-src-bucket'
        self.output_bucket = 'prdsales-s3-out-bucket'
    
    def extract_domain(self, url: str) -> Optional[str]:
        """
        Extract domain from URL.
        
        Args:
            url: URL string to extract domain from
            
        Returns:
            Domain string or None if extraction fails
        """
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
    
    def extract_search_keyword(self, referrer_url: str) -> Optional[str]:
        """
        Extract search keyword from referrer URL.
        
        Args:
            referrer_url: Referrer URL to extract keyword from
            
        Returns:
            Search keyword string or None if not found
        """
        if not referrer_url:
            return None
        try:
            parsed = urllib.parse.urlparse(referrer_url)
            query_params = urllib.parse.parse_qs(parsed.query)
            
            search_params = ['q', 'query', 'p', 'search', 'keywords', 'k']
            
            for param in search_params:
                if param in query_params and query_params[param]:
                    keyword = query_params[param][0]
                    keyword = urllib.parse.unquote_plus(keyword)
                    return keyword.strip()
        except:
            pass
        return None
    
    def extract_revenue(self, product_list: str) -> float:
        """
        Extract revenue from product_list field.
        
        Args:
            product_list: Product list string containing revenue data
            
        Returns:
            Revenue amount as float
        """
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
    
    def is_search_engine(self, referrer_url: str) -> bool:
        """
        Check if referrer is a search engine.
        
        Args:
            referrer_url: Referrer URL to check
            
        Returns:
            True if referrer is a search engine, False otherwise
        """
        if not referrer_url:
            return False
        
        domain = self.extract_domain(referrer_url)
        if not domain or domain == self.site_domain:
            return False
        
        keyword = self.extract_search_keyword(referrer_url)
        return keyword is not None
    
    def read_s3_file(self, bucket: str, key: str, max_size_mb: int = 500) -> List[Dict]:
        """
        Read file from S3 with size limits for Lambda.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            max_size_mb: Maximum file size in MB
            
        Returns:
            List of dictionaries representing CSV rows
        """
        try:
            print('Reading S3 file...')
            print(f"Reading S3 file: s3://{bucket}/{key}")
            
            # Read file content
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Parse TSV content
            csv_reader = csv.DictReader(StringIO(content), delimiter='\t')
            rows = list(csv_reader)
            
            print(f"Loaded {len(rows):,} records from S3")
            return rows
            
        except Exception as e:
            print(f"Error reading S3 file: {e}")
            raise
    
    def process_data(self, rows: List[Dict]) -> List[Dict]:
        """
        Read the file data for finding out search keywords
        
        Args:
            rows: List of data rows to process
            
        Returns:
            List of processed results
        """
        user_sessions = {}
        results = []
        
        print("Processing data in streaming mode...")
        
        # Process rows in chunks to manage memory
        chunk_size = 10000
        processed_count = 0
        
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            
            for row in chunk:
                ip = row.get('ip', '')
                referrer = row.get('referrer', '')
                event_list = row.get('event_list', '')
                product_list = row.get('product_list', '')
                
                if ip not in user_sessions:
                    user_sessions[ip] = {
                        'search_referrer': None,
                        'search_keyword': None,
                        'search_domain': None,
                        'purchases': []
                    }
                
                # Check for search engine referral
                if self.is_search_engine(referrer):
                    domain = self.extract_domain(referrer)
                    keyword = self.extract_search_keyword(referrer)
                    if keyword and domain:
                        user_sessions[ip]['search_referrer'] = referrer
                        user_sessions[ip]['search_keyword'] = keyword
                        user_sessions[ip]['search_domain'] = domain
                
                # Check for purchase completion
                if event_list == '1':
                    revenue = self.extract_revenue(product_list)
                    if revenue > 0:
                        user_sessions[ip]['purchases'].append(revenue)
            
            processed_count += len(chunk)
            if processed_count % 50000 == 0:
                print(f"Processed {processed_count:,} records...")
        
        # Generate results from sessions
        for ip, session in user_sessions.items():
            if (session['search_domain'] and 
                session['search_keyword'] and 
                session['purchases']):
                
                for revenue in session['purchases']:
                    results.append({
                        'search_engine_domain': session['search_domain'],
                        'search_keyword': session['search_keyword'],
                        'revenue': revenue
                    })
        
        print(f"Generated {len(results)} final results")
        return results
    
    def save_results_to_s3(self, results: List[Dict], output_bucket: str, output_key: str) -> str:
        """
        Save results back to S3.
        
        Args:
            results: List of result dictionaries
            output_bucket: S3 bucket for output
            output_key: S3 key for output file
            
        Returns:
            S3 URL of saved file
        """
        try:
            print('Saving results to S3...')
            print(f'Results count: {len(results)}')
            print(f'Output bucket: {output_bucket}')
            print(f'Output key: {output_key}')
            
            # Create TSV content
            if not results:
                print('No results found - creating empty file')
                tsv_content = "Search Engine Domain\tSearch Keyword\tRevenue\n"
            else:
                print('Creating TSV content from results')
                
                # Sort results by revenue in descending order (highest first)
                sorted_results = sorted(results, key=lambda x: x['revenue'], reverse=True)
                print(f'Sorted {len(sorted_results)} results by revenue (descending)')
                
                output = StringIO()
                writer = csv.writer(output, delimiter='\t')
                writer.writerow(['Search Engine Domain', 'Search Keyword', 'Revenue'])
                
                for result in sorted_results:
                    writer.writerow([
                        result['search_engine_domain'],
                        result['search_keyword'],
                        f"{result['revenue']:.2f}"
                    ])
                
                tsv_content = output.getvalue()
                print('TSV content length:', len(tsv_content))
                #print('TSV content preview:', tsv_content[:500])  # First 500 chars
            
            print('Uploading to S3...')
            
            # Upload to S3 with detailed error handling
            try:
                response = self.s3_client.put_object(
                    Bucket=output_bucket,
                    Key=output_key,
                    Body=tsv_content.encode('utf-8'),
                    ContentType='text/tab-separated-values'
                )
                print('S3 upload successful!')
                print('S3 put_object response:', response)
                
            except Exception as s3_error:
                print(f"S3 upload failed: {s3_error}")
                print(f"S3 error type: {type(s3_error)}")
                # Check for common S3 errors
                if 'AccessDenied' in str(s3_error):
                    print("ACCESS DENIED: Check Lambda execution role permissions for S3")
                elif 'NoSuchBucket' in str(s3_error):
                    print(f"BUCKET NOT FOUND: {output_bucket} does not exist")
                raise s3_error
            
            output_location = f"s3://{output_bucket}/{output_key}"
            print(f"Results saved to: {output_location}")
            return output_location
            
        except Exception as e:
            print(f"Error saving to S3: {e}")
            print(f"Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def create_summary(self, results: List[Dict]) -> Dict:
        """
        Create summary statistics from results.
        
        Args:
            results: List of result dictionaries
            
        Returns:
            Dictionary containing summary statistics
        """
        print('Creating summary statistics...')
        if not results:
            return {
                'total_records': 0,
                'total_revenue': 0.0,
                'unique_domains': 0,
                'unique_keywords': 0
            }
        
        total_revenue = sum(r['revenue'] for r in results)
        unique_domains = len(set(r['search_engine_domain'] for r in results))
        unique_keywords = len(set(r['search_keyword'] for r in results))
        
        # Top performers
        domain_revenue = {}
        keyword_revenue = {}
        
        for result in results:
            domain = result['search_engine_domain']
            keyword = result['search_keyword']
            revenue = result['revenue']
            
            domain_revenue[domain] = domain_revenue.get(domain, 0) + revenue
            keyword_revenue[keyword] = keyword_revenue.get(keyword, 0) + revenue
        
        top_domains = sorted(domain_revenue.items(), key=lambda x: x[1], reverse=True)[:5]
        top_keywords = sorted(keyword_revenue.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return {
            'total_records': len(results),
            'total_revenue': total_revenue,
            'unique_domains': unique_domains,
            'unique_keywords': unique_keywords,
            'top_domains': top_domains,
            'top_keywords': top_keywords
        }
    
    def extract_file_date(self, file_key: str) -> str:
        """
        Extract date from S3 file key.
        Expected format: prdsales/year=YYYY/month=MM/day=DD/filename
        
        Args:
            file_key: S3 file key
            
        Returns:
            Date string in YYYY-MM-DD format
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
    
    def create_completion_payload(self, file_key: str, status: str = 'COMPLETE', error_message: str = None) -> Dict:
        """
        Create completion status payload.
        
        Args:
            file_key: S3 file key being processed
            status: Processing status (COMPLETE/FAILED)
            error_message: Error message if status is FAILED
            
        Returns:
            Dictionary containing completion payload
        """
        try:
            # Extract file date from file_key
            file_date = self.extract_file_date(file_key)
            
            # Prepare status payload in the requested format
            payload = {
                'file_date': file_date,
                'file_key': file_key,
                'bucket': self.input_bucket,
                'content_type': 'text/csv',
                'event_type': 'Lambda',
                'file_datetime': datetime.utcnow().isoformat() + 'Z',
                'file_size_gb': '',  # Leave empty as requested
                'file_status': status
            }
            
            # Add error message if provided
            if error_message:
                payload['error_message'] = error_message
            
            print(f"Created completion payload: {status}")
            print(f"Payload: {json.dumps(payload, indent=2)}")
            
            return payload
            
        except Exception as e:
            print(f"Error creating payload: {e}")
            return None
    
    def process_file(self, file_key: str, context) -> Dict:
        """
        Main processing method that orchestrates the entire workflow.
        
        Args:
            file_key: S3 file key to process
            context: Lambda context object
            
        Returns:
            Dictionary containing processing results
        """
        try:
            print("Starting Lambda Search Revenue Analysis (Class-based)")
            print("=====================================================")
            print(f"Processing file: {file_key}")
            print(f"Lambda timeout: {context.get_remaining_time_in_millis()} ms")
            
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
            
            # Define paths
            input_key = file_key  # Use the provided file_key directly
            print('Input file path:', input_key)
            output_key = f'tgt/year={year}/month={month}/date={day}/{outfilename}'
            print('Output file path:', output_key)
            max_size_mb = 100  # Size limit for Lambda processing
            
            # Read data from S3
            print(f"Time remaining before read: {context.get_remaining_time_in_millis()} ms")
            rows = self.read_s3_file(self.input_bucket, input_key, max_size_mb)
            
            # Process data
            print(f"Time remaining before processing: {context.get_remaining_time_in_millis()} ms")
            results = self.process_data(rows)
            print(f'Processing complete. Results count: {len(results)}')
            
            # Save results to S3
            print(f"Time remaining before S3 upload: {context.get_remaining_time_in_millis()} ms")
            output_location = self.save_results_to_s3(results, self.output_bucket, output_key)
            print('Output location:', output_location)
            
            # Create summary
            summary = self.create_summary(results)
            print(f"Summary: {summary}")
            
            print("Analysis completed successfully!")
            print(f"Final time remaining: {context.get_remaining_time_in_millis()} ms")
            
            # Create and return completion payload
            completion_payload = self.create_completion_payload(file_key, status='COMPLETE')
            return completion_payload
            
        except Exception as e:
            print(f"Error in processing: {e}")
            import traceback
            traceback.print_exc()
            
            # Create and return failure payload
            failure_payload = self.create_completion_payload(file_key, status='FAILED', error_message=str(e))
            return failure_payload


def lambda_handler(event, context):
    """
    AWS Lambda handler function that uses the SearchRevenueAnalyzer class.
    
    Expected event format:
    {
        "file_key": "prdsales/web/PrdSalesRevenue.csv"
    }
    
    Args:
        event: Lambda event containing file_key
        context: Lambda context object
        
    Returns:
        Dictionary containing processing results
    """
    
    try:
        print("Lambda handler started")
        print(f"Event: {json.dumps(event, indent=2)}")
        
        # Extract file_key from event
        file_key = event.get('file_key')
        if not file_key:
            raise ValueError("Missing required parameter: file_key")
        
        # Create analyzer instance and process file
        analyzer = SearchRevenueAnalyzer()
        result = analyzer.process_file(file_key, context)
        
        return result
        
    except Exception as e:
        print(f"Error in Lambda handler: {e}")
        import traceback
        traceback.print_exc()
        
        # Return basic error info if analyzer creation fails
        return {
            'file_key': event.get('file_key', 'unknown'),
            'file_status': 'FAILED',
            'error_message': str(e)
        }