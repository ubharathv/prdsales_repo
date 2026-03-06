# PrdSales Data Pipeline

## Overview
A serverless AWS data processing pipeline that analyzes product sales data to generate search keyword performance analytics. The system automatically processes CSV files containing web analytics data, extracts search engine referrals, and produces revenue attribution reports.

## Architecture
The pipeline uses a hybrid processing approach:
- **Small files (< 2GB)**: Processed via AWS Lambda for fast execution
- **Large files (≥ 2GB)**: Processed via AWS Glue for scalable distributed processing
- **Orchestration**: AWS Step Functions coordinate the workflow
- **Tracking**: DynamoDB stores processing events and status

## Project Structure

### Documents
Project documentation and sample data files
- **SrcFiles/**: Sample input CSV files for testing
  - `PrdSalesRevenue_original.csv` - Original sales data format
  - `PrdSalesRevenue_quantity.csv` - Sales data with quantity metrics
  - `PrdSalesRevenue_test.csv` - Test dataset for testing ordering 
- **TgtFiles/**: Expected output files in tab-delimited format
  - Various dated output files showing search keyword performance results
- **SalesAnalysis_FrameWork.pptx** - Project framework - Power Point Presentation
- **SalesAnalysis_TestingDoc.pdf** - Testing procedures and validation
- **Project_Artifacts.xlsx** - Details of code base created as part of this Project

### `/glue`
AWS Glue ETL jobs for large-scale data processing
- **`dev_prdsales_gen_search_rev_glue.py`** - Main Glue job for processing large files (≥2GB)
  - Uses PySpark for distributed processing
  - Handles search engine detection and keyword extraction
  - Generates revenue attribution analytics

### `/lambda`
AWS Lambda functions for serverless processing
- **`dev_gen_search_revenue_lambda.py`** - Core analytics processor for small files (<2GB)
  - Analyzes search referrals and revenue data
  - Extracts keywords from various search engines
  - Outputs tab-delimited performance reports
- **`dev_get_prdsales_file_lambda.py`** - File event handler and router
  - Monitors S3 file uploads
  - Routes processing based on file size
  - Tracks events in DynamoDB

### `/stepfunction`
AWS Step Functions workflow definitions
- **`dev_StepFun_prdsales_process_file.json`** - Main orchestration workflow
  - Routes files to appropriate processing engine (Lambda vs Glue)
  - Coordinates end-to-end processing flow

### `/infrastructure`
Infrastructure as Code (CloudFormation)
- **`iam-roles.yaml`** - IAM roles and permissions
  - Lambda execution roles with S3, DynamoDB, and Step Functions access
  - Glue service roles for distributed processing
  - Step Functions execution roles for workflow orchestration
  - S3 event notification permissions

### `/parameters`
Configuration parameters (currently empty - reserved for environment-specific configs)

### `/scripts`
Utility and deployment scripts (currently empty - reserved for automation scripts)

## Data Flow

1. **File Upload**: CSV files uploaded to S3 source bucket trigger Lambda function
2. **Size Check**: System determines processing route based on file size
3. **Processing**: 
   - Small files: Lambda processes directly
   - Large files: Step Function triggers Glue job
4. **Analytics**: Extract search keywords, match with revenue data
5. **Output**: Generate tab-delimited reports in S3 output bucket
6. **Tracking**: Log all events and status in DynamoDB

## Output Format
Tab-delimited files with three columns:
- **Search Engine Domain**: The domain of the search engine (e.g., google.com)
- **Search Keyword**: The search term used (e.g., "Ipod")
- **Revenue**: The purchase amount in dollars (e.g., 290.00)

## AWS Services Used
- **AWS Lambda**: Serverless compute for small file processing
- **AWS Glue**: Managed ETL service for large file processing
- **AWS Step Functions**: Workflow orchestration
- **Amazon S3**: Object storage for input/output files
- **Amazon DynamoDB**: Event tracking and status management
- **AWS IAM**: Security and access management
- **AWS CloudFormation**: Infrastructure as Code

## Environment Support
The pipeline supports multiple environments (dev/test/prod) with configurable:
- Resource naming conventions
- IAM role permissions
- S3 bucket configurations
- Processing parameters

## Getting Started

### Prerequisites
- AWS Account with appropriate permissions
- S3 buckets for source and output data
- DynamoDB table: `prdssales_file_evnt`

### Deployment
1. Deploy IAM roles using CloudFormation template
2. Create Lambda functions from source code
3. Deploy Glue job script
4. Create Step Function workflow
5. Configure S3 event notifications

### Usage
Simply upload CSV files to the configured S3 source bucket. The pipeline will automatically:
- Detect the file upload
- Route to appropriate processing engine
- Generate search keyword performance analytics
- Store results in the output bucket

## Monitoring
- DynamoDB table tracks all file processing events
- CloudWatch logs provide detailed execution information
- Step Functions console shows workflow execution status