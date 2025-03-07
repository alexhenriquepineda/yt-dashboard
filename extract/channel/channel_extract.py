import json
import logging
from typing import List, Dict, Any, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeDataChannelExtractor:
    """Extracts and stores YouTube channel data in Amazon S3."""

    # API Constants
    YOUTUBE_API_SERVICE = 'youtube'
    YOUTUBE_API_VERSION = 'v3'
    CHANNEL_PARTS = 'id,contentDetails,statistics,status,topicDetails,brandingSettings,snippet'
    MAX_RESULTS_PER_REQUEST = 50

    def __init__(
        self, 
        api_key: str, 
        channel_ids: List[str], 
        bucket_name: str, 
        s3_key: str, 
        auto_execute: bool = True
    ):
        """
        Initializes the extractor with API credentials, channel IDs, and S3 storage details.

        Args:
            api_key: YouTube Data API key.
            channel_ids: List of YouTube channel IDs to extract data for.
            bucket_name: AWS S3 bucket name.
            s3_key: Path where the file will be stored in S3.
            auto_execute: If True, automatically executes extraction and upload.
        """
        # Logging configuration
        self.logger = logging.getLogger(__name__)
        
        # Parameter storage
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        
        # Client initialization
        self.youtube = build(
            self.YOUTUBE_API_SERVICE, 
            self.YOUTUBE_API_VERSION, 
            developerKey=self.api_key,
            cache_discovery=False
        )
        self.s3_client = boto3.client("s3")
        
        # Data storage
        self.data: List[Dict[str, Any]] = []
        
        # Automatic execution if requested
        if auto_execute:
            self.run()

    def run(self) -> List[Dict[str, Any]]:
        """
        Executes the complete process of extraction, processing, and data upload.
        
        Returns:
            List with processed channel data.
        """
        self.get_channels_data()
        self.save_data_to_s3()
        return self.data

    @staticmethod
    def chunk_list(lst: List[Any], size: int) -> List[List[Any]]:
        """
        Divides a list into sublists of specified maximum size.
        
        Args:
            lst: List to be divided.
            size: Maximum size of each sublist.
            
        Returns:
            List of sublists.
        """
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    def get_channels_data(self) -> List[Dict[str, Any]]:
        """
        Retrieves channel data from the YouTube API.
        
        Returns:
            List with channel data.
        """
        if not self.channel_ids:
            self.logger.warning("No channel IDs provided.")
            return self.data
            
        # Split the channel list into chunks to respect API limits
        chunks = self.chunk_list(self.channel_ids, self.MAX_RESULTS_PER_REQUEST)
        
        for chunk in chunks:
            try:
                request = self.youtube.channels().list(
                    part=self.CHANNEL_PARTS,
                    id=','.join(chunk)
                )
                response = request.execute()
                new_items = response.get('items', [])
                self.data.extend(new_items)
                self.logger.info(f"Retrieved data for {len(new_items)} channels.")
            except HttpError as e:
                self.logger.error(f"YouTube API error: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error retrieving channel data: {e}")
                
        self.logger.info(f"Total of {len(self.data)} channels retrieved.")
        return self.data


    def save_data_to_s3(self) -> bool:
        """
        Saves the retrieved channel data as a JSON file to AWS S3.
        
        Returns:
            True if upload is successful, False otherwise.
        """
        if not self.data:
            self.logger.warning("No data to save to S3.")
            return False
            
        json_data = json.dumps(self.data, ensure_ascii=False)

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.s3_key,
                Body=json_data,
                ContentType="application/json"
            )
            self.logger.info(f"Data successfully uploaded to s3://{self.bucket_name}/{self.s3_key}")
            return True
        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"AWS S3 error while uploading: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error saving data: {e}")
            return False
