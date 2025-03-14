import pandas as pd
from io import BytesIO
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional, Union, Dict, Any


def read_json_from_s3(
    s3_client, 
    bucket_name: str, 
    s3_key: str,
    logger: Optional[logging.Logger] = None,
    error_handler: Optional[callable] = None
) -> Union[Dict[str, Any], pd.DataFrame, None]:
    """
    Reads a JSON file from Amazon S3 and returns it as a Python dictionary or DataFrame.
    
    Args:
        s3_client: An initialized boto3 S3 client object
        bucket_name (str): Name of the S3 bucket
        s3_key (str): Path/key to the JSON file in the S3 bucket
        logger (logging.Logger, optional): Logger object for capturing errors
        error_handler (callable, optional): Custom error handling function
            that takes an exception as parameter
    
    Returns:
        Union[Dict[str, Any], pd.DataFrame, None]: 
            - Dictionary containing the JSON data if successful
            - None if an error occurs and no error_handler is provided
    
    Examples:
        >>> s3_client = boto3.client('s3')
        >>> data = read_json_from_s3(s3_client, 'my-bucket', 'path/to/file.json')
        
        With custom error handling:
        >>> import streamlit as st
        >>> data = read_json_from_s3(
        ...     s3_client, 
        ...     'my-bucket', 
        ...     'data.json',
        ...     error_handler=lambda e: st.error(f"Error: {e}")
        ... )
    """
    # Default logger if none provided
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Read the JSON content
        json_content = response['Body'].read().decode('utf-8')
        
        # Convert to Python dictionary
        import json
        return json.loads(json_content)
        
    except (ClientError, NoCredentialsError) as e:
        error_msg = f"Error reading JSON from S3 bucket '{bucket_name}', key '{s3_key}': {str(e)}"
        logger.error(error_msg)
        
        if error_handler:
            error_handler(e)
        return None
    except Exception as e:
        error_msg = f"Unexpected error reading JSON from S3: {str(e)}"
        logger.error(error_msg)
        
        if error_handler:
            error_handler(e)
        return None


def read_parquet_from_s3(
    s3_client, 
    bucket_name: str, 
    s3_key: str,
    logger: Optional[logging.Logger] = None,
    error_handler: Optional[callable] = None
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Reads a Parquet file from Amazon S3 and returns it as a pandas DataFrame.
    
    Args:
        s3_client: An initialized boto3 S3 client object
        bucket_name (str): Name of the S3 bucket
        s3_key (str): Path/key to the Parquet file in the S3 bucket
        logger (logging.Logger, optional): Logger object for capturing errors
        error_handler (callable, optional): Custom error handling function
            that takes an exception as parameter
    
    Returns:
        Union[pd.DataFrame, Dict[str, Any]]: 
            - DataFrame containing the data if successful
            - Empty DataFrame if basic error occurs
            - Dict with error details if detailed_errors=True
    
    Examples:
        >>> s3_client = boto3.client('s3')
        >>> df = read_parquet_from_s3(s3_client, 'my-bucket', 'path/to/file.parquet')
        
        With custom error handling:
        >>> import streamlit as st
        >>> df = read_parquet_from_s3(
        ...     s3_client, 
        ...     'my-bucket', 
        ...     'data.parquet',
        ...     error_handler=lambda e: st.error(f"Error: {e}")
        ... )
    """
    # Default logger if none provided
    if logger is None:
        logger = logging.getLogger(__name__)
    
    result = {
        "success": False,
        "data": pd.DataFrame(),
        "error_type": None,
        "error_message": None
    }
    
    try:
        # Check inputs
        if not isinstance(bucket_name, str) or not bucket_name.strip():
            raise ValueError("bucket_name must be a non-empty string")
        if not isinstance(s3_key, str) or not s3_key.strip():
            raise ValueError("s3_key must be a non-empty string")
        
        # Get object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Read Parquet content
        parquet_data = response['Body'].read()
        parquet_buffer = BytesIO(parquet_data)
        
        # Parse Parquet to DataFrame
        df = pd.read_parquet(parquet_buffer, engine='pyarrow')
        
        # Check if DataFrame is valid
        if df.empty:
            logger.warning(f"File {s3_key} in bucket {bucket_name} resulted in empty DataFrame")
        
        # Success
        result["success"] = True
        result["data"] = df
        return df
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        
        # Handle specific AWS errors
        if error_code == 'NoSuchKey':
            msg = f"File {s3_key} not found in bucket {bucket_name}"
        elif error_code == 'NoSuchBucket':
            msg = f"Bucket {bucket_name} does not exist"
        elif error_code == 'AccessDenied':
            msg = f"Access denied to {s3_key} in bucket {bucket_name}"
        else:
            msg = f"AWS error {error_code}: {error_msg}"
            
        logger.error(msg)
        result["error_type"] = f"AWS:{error_code}"
        result["error_message"] = msg
        
    except NoCredentialsError:
        msg = "AWS credentials not found. Please configure your credentials."
        logger.error(msg)
        result["error_type"] = "Credentials"
        result["error_message"] = msg
        
    except pd.errors.EmptyDataError:
        msg = f"File {s3_key} in bucket {bucket_name} is empty or has no valid data"
        logger.error(msg)
        result["error_type"] = "DataError"
        result["error_message"] = msg
        
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        result["error_type"] = "ValueError"
        result["error_message"] = str(e)
        
    except Exception as e:
        msg = f"Unexpected error reading {s3_key} from {bucket_name}: {str(e)}"
        logger.error(msg)
        result["error_type"] = "Unknown"
        result["error_message"] = msg
    
    # Call custom error handler if provided
    if error_handler and callable(error_handler):
        try:
            error_handler(result["error_message"])
        except Exception as handler_error:
            logger.error(f"Error in custom error handler: {handler_error}")
    
    # For compatibility with existing code, return empty DataFrame by default
    return result["data"]

def save_to_s3(s3_client, df: pd.DataFrame, bucket_name: str, output_key: str, format: str = 'parquet') -> bool:
    """
    Saves processed data to S3.
    
    Args:
        s3_client: The boto3 S3 client
        df (pd.DataFrame): DataFrame to save
        bucket_name (str): S3 bucket name
        output_key (str): Destination key in S3.
        format (str, optional): Output format ('parquet' or 'csv'). Default is 'parquet'.
        
    Returns:
        bool: True if saving is successful, False otherwise.
    """
    if df.empty:
        return False
        
    buffer = BytesIO()
    
    if format.lower() == 'parquet':
        df.to_parquet(buffer, index=False, engine='pyarrow')
    elif format.lower() == 'csv':
        df.to_csv(buffer, index=False)
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'parquet' or 'csv'.")
        
    buffer.seek(0)
    
    try:
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=output_key, 
            Body=buffer.getvalue()
        )
        return True
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return False
