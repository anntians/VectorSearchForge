import boto3
import os
import tempfile
from botocore.exceptions import ClientError
from pathlib import Path
from botocore.exceptions import ClientError

def check_s3_object_exists(bucket_name, object_key):
    s3 = boto3.client('s3')
    
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise



def download_s3_file_in_chunks(bucket_name, object_key, chunk_size=1024*1024):  # 1MB chunks
    """
    Download a file from S3 in chunks and save to temp directory
    
    Args:
        bucket_name (str): The S3 bucket name
        object_key (str): The S3 object key (file path)
        chunk_size (int): Size of chunks to download (default 1MB)
        
    Returns:
        str: Path to the downloaded file in temp directory
    """
    s3_client = boto3.client('s3')
    
    try:
        # Get object details
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        file_size = response['ContentLength']
        
        # Create temp file with same extension as original
        file_extension = Path(object_key).suffix
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file_extension)
        temp_file_path = temp_file.name
        
        # Get the object
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        
        downloaded = 0
        with open(temp_file_path, 'wb') as f:
            print(f"Downloading {object_key} to {temp_file_path}")
            
            # Download and write chunks
            for chunk in response['Body'].iter_chunks(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                
                # Calculate and display progress
                progress = (downloaded / file_size) * 100
                print(f"Progress: {progress:.2f}% ({downloaded}/{file_size} bytes)")
        
        print(f"Download completed: {temp_file_path}")
        return temp_file_path
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            print(f"The object {object_key} does not exist in bucket {bucket_name}")
        elif error_code == 'NoSuchBucket':
            print(f"The bucket {bucket_name} does not exist")
        else:
            print(f"Error downloading object: {e}")
            
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise

def cleanup_temp_file(temp_file_path):
    """
    Clean up the temporary file when no longer needed
    
    Args:
        temp_file_path (str): Path to the temporary file
    """
    try:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            print(f"Temporary file removed: {temp_file_path}")
    except Exception as e:
        print(f"Error removing temporary file: {e}")