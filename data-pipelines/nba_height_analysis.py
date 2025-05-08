"""
NBA Height Analysis Pipeline

Airflow DAG that downloads NBA player height data and stores it in MinIO.
Includes error handling, automatic retries, and data versioning.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import logging

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_request_headers():
    """Return headers to handle HTTP 406 errors"""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,application/octet-stream,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.openintro.org/'
    }

def _download_nba_data(**context):
    """Download NBA data and return content"""
    url = "https://www.openintro.org/data/csv/nba_heights.csv"
    
    try:
        # Get data with proper headers
        response = requests.get(url, headers=get_request_headers())
        response.raise_for_status()
        
        # Verify we got CSV data
        content_type = response.headers.get('content-type', '')
        if not any(t in content_type.lower() for t in ['csv', 'text/plain']):
            raise ValueError(f"Unexpected content type: {content_type}")
        
        # Store content as string in XCom
        content = response.text
        context['task_instance'].xcom_push(key='nba_data', value=content)
        logging.info("Successfully downloaded NBA data")
        
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred: {str(e)}")
        if e.response.status_code == 406:
            logging.error("Received 406 error - check headers configuration")
        raise
    except Exception as e:
        logging.error(f"Error downloading data: {str(e)}")
        raise

def _upload_to_minio(**context):
    """Upload data to MinIO"""
    try:
        # Get content from XCom
        content = context['task_instance'].xcom_pull(task_ids='download_nba_data', key='nba_data')
        if not content:
            raise ValueError("Failed to get data from previous task")
        
        # Generate path with date
        date_str = context['logical_date'].strftime('%Y-%m-%d')
        minio_path = f"nba/heights/nba_heights_{date_str}.csv"
        
        # Using S3Hook with MinIO connection
        s3_hook = S3Hook('minio_s3')
        
        # Get the underlying boto3 client with proper endpoint configuration
        s3_client = s3_hook.get_conn()
        
        # Upload using boto3 client directly
        s3_client.put_object(
            Bucket="workshop-data",
            Key=minio_path,
            Body=content
        )
        
        logging.info(f"Successfully uploaded to MinIO: {minio_path}")
            
    except Exception as e:
        logging.error(f"Error in upload task: {str(e)}")
        raise

# Define the DAG
with DAG(
    'nba_height_analysis',
    default_args=default_args,
    description='Downloads and stores NBA player height data',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Download data
    download_task = PythonOperator(
        task_id='download_nba_data',
        python_callable=_download_nba_data,
    )

    # Task 2: Upload to MinIO
    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=_upload_to_minio,
    )

    # Define task dependencies
    download_task >> upload_task
