import json
import logging
import time
import random
from typing import Dict, List, Optional, Set, Tuple
import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime


class YouTubeDataVideoExtractor:
    """A class to harvest YouTube channel and video data through the YouTube Data API."""
    
    def __init__(
        self, 
        api_key: str, 
        channel_ids: List[str], 
        bucket_name: str, 
        s3_key: str, 
        batch_size: int = 50,
        max_retries: int = 5,
        auto_execute: bool = True
    ):
        """
        Initialize the YouTube data harvester.
        
        Args:
            api_key: YouTube Data API v3 key
            channel_ids: List of YouTube channel IDs to process
            bucket_name: AWS S3 bucket name
            s3_key: Path within the S3 bucket to save the file (e.g., 'data/raw/video/videos_data.json')
            batch_size: Number of videos to process in each API batch request
            max_retries: Maximum number of retry attempts for API calls
            auto_execute: Whether to automatically execute the pipeline on initialization
        """
        # Set up logging
        self.logger = self._setup_logger()
        
        # Validate inputs
        if not api_key:
            raise ValueError("API key is required")
        if not channel_ids:
            raise ValueError("At least one channel ID is required")
        if not bucket_name or not s3_key:
            raise ValueError("S3 bucket name and key are required")
            
        # API configuration
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # YouTube API client setup
        self.youtube = build(
            "youtube",
            "v3",
            developerKey=self.api_key,
            cache_discovery=False  # Disable discovery caching for better performance
        )
        
        # AWS S3 client
        self.s3_client = boto3.client("s3")
        
        # State management
        self._channel_playlists: Dict[str, str] = {}
        self._all_video_ids: List[str] = []
        self._video_details: List[dict] = []
        self._processed_video_ids: Set[str] = set()  # Track which videos we've already processed
        self._extraction_date = datetime.now().strftime("%Y-%m-%d")
        
        # Auto-execute if enabled
        if auto_execute:
            self.execute_pipeline()
    
    def _setup_logger(self) -> logging.Logger:
        """
        Set up a logger for the class.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """
        Retry a function with exponential backoff.
        
        Args:
            func: Function to retry
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Result of the function if successful
            
        Raises:
            Last exception encountered if all retries fail
        """
        retries = 0
        last_exception = None
        
        while retries <= self.max_retries:
            try:
                if retries > 0:
                    # Exponential backoff with jitter
                    wait_time = (2 ** retries) + random.uniform(0, 1)
                    self.logger.info(f"Retry attempt {retries}/{self.max_retries}, waiting {wait_time:.2f} seconds")
                    time.sleep(wait_time)
                    
                return func(*args, **kwargs)
                
            except HttpError as e:
                last_exception = e
                status_code = e.resp.status
                
                # Don't retry on client errors (except rate limiting)
                if 400 <= status_code < 500 and status_code != 429:
                    self.logger.error(f"Non-retriable error (HTTP {status_code}): {e}")
                    raise
                    
                self.logger.warning(f"API error (HTTP {status_code}): {e}. Retrying...")
                retries += 1
                
            except Exception as e:
                last_exception = e
                self.logger.warning(f"Unexpected error: {e}. Retrying...")
                retries += 1
        
        # If we get here, all retries failed
        self.logger.error(f"All {self.max_retries} retry attempts failed")
        raise last_exception

    def _get_channel_uploads_playlists(self) -> Dict[str, str]:
        """
        Retrieve uploads playlist IDs for all configured channels.
        
        Returns:
            Dictionary mapping channel IDs to their uploads playlist IDs
        """
        self.logger.info(f"Retrieving upload playlists for {len(self.channel_ids)} channels")
        
        # Process channels in batches of 50 (YouTube API limit)
        channel_batch_size = 50
        result = {}
        
        for i in range(0, len(self.channel_ids), channel_batch_size):
            batch = self.channel_ids[i:i + channel_batch_size]
            
            try:
                # Use the retry mechanism
                response = self._retry_with_backoff(
                    lambda: self.youtube.channels().list(
                        part="contentDetails",
                        id=",".join(batch),
                        maxResults=channel_batch_size
                    ).execute()
                )
                
                # Extract upload playlist IDs
                for item in response.get("items", []):
                    channel_id = item["id"]
                    playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
                    result[channel_id] = playlist_id
                    
            except Exception as e:
                self.logger.error(f"Error fetching channel details even after retries: {e}")
                # Continue with the next batch instead of failing completely
        
        self.logger.info(f"Found {len(result)} valid channels with upload playlists")
        self._channel_playlists = result
        return result

    def _harvest_playlist_videos(self, playlist_id: str) -> List[str]:
        """
        Retrieve all video IDs from a YouTube playlist.
        
        Args:
            playlist_id: ID of the playlist to harvest
            
        Returns:
            List of video IDs found in the playlist
        """
        video_ids = []
        next_page_token = None
        page_count = 0
        
        self.logger.info(f"Harvesting videos from playlist: {playlist_id}")
        
        while True:
            try:
                # Use the retry mechanism
                response = self._retry_with_backoff(
                    lambda: self.youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,  # Maximum allowed by YouTube API
                        pageToken=next_page_token
                    ).execute()
                )
                
                page_count += 1
                
                # Extract video IDs
                batch_video_ids = [
                    item["contentDetails"]["videoId"]
                    for item in response.get("items", [])
                ]
                video_ids.extend(batch_video_ids)
                
                self.logger.debug(f"Retrieved {len(batch_video_ids)} videos (page {page_count})")
                
                # Check for more pages
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching playlist items even after retries: {e}")
                break
        
        self.logger.info(f"Total videos found in playlist: {len(video_ids)}")
        return video_ids

    def _get_video_details_batch(self, video_ids: List[str]) -> List[dict]:
        """
        Retrieve detailed metadata for a batch of video IDs.
        
        Args:
            video_ids: List of YouTube video IDs to process
            
        Returns:
            List of video metadata dictionaries
        """
        if not video_ids:
            return []
            
        self.logger.debug(f"Fetching details for {len(video_ids)} videos")
        
        # Filter out any videos we've already processed
        new_video_ids = [vid for vid in video_ids if vid not in self._processed_video_ids]
        
        if not new_video_ids:
            self.logger.debug("All videos in this batch have already been processed")
            return []
            
        self.logger.debug(f"Fetching details for {len(new_video_ids)} new videos")
        
        try:
            # Use the retry mechanism
            response = self._retry_with_backoff(
                lambda: self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(new_video_ids)
                ).execute()
            )
            
            # Mark these videos as processed
            self._processed_video_ids.update(new_video_ids)
            
            # Add extraction date to each video
            video_details = response.get("items", [])
            for video in video_details:
                video["extraction_date"] = self._extraction_date
                
            return video_details
            
        except Exception as e:
            self.logger.error(f"Error fetching video details even after retries: {e}")
            return []

    def save_data_to_s3(self) -> bool:
        """
        Save the collected video data as a JSON file to AWS S3.
        
        Returns:
            Boolean indicating success or failure
        """
        if not self._video_details:
            self.logger.warning("No video data to save")
            return False

        # Convert the video details to JSON
        json_data = json.dumps(self._video_details, indent=2, ensure_ascii=False)

        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.s3_key,
                Body=json_data,
                ContentType="application/json"
            )
            self.logger.info(f"Data successfully uploaded to s3://{self.bucket_name}/{self.s3_key}")
            return True
            
        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"AWS S3 error during upload: {e}")
            return False

    def execute_pipeline(self) -> bool:
        """
        Execute the complete data harvesting pipeline.
        
        Returns:
            Boolean indicating success or failure
        """
        self.logger.info("Starting YouTube data harvesting pipeline")
        
        # Get channel upload playlists
        playlists = self._get_channel_uploads_playlists()
        if not playlists:
            self.logger.error("No valid channels found, aborting pipeline")
            return False

        # Collect video IDs from all channels
        self._all_video_ids = []  # Reset in case we're retrying
        for channel_id, playlist_id in self._channel_playlists.items():
            self.logger.info(f"Processing channel {channel_id}")
            video_ids = self._harvest_playlist_videos(playlist_id)
            self._all_video_ids.extend(video_ids)

        # Process videos in batches
        total_videos = len(self._all_video_ids)
        self.logger.info(f"Processing {total_videos} videos in batches of {self.batch_size}")
        
        total_batches = (total_videos - 1) // self.batch_size + 1
        
        for i in range(0, total_videos, self.batch_size):
            batch = self._all_video_ids[i:i + self.batch_size]
            details = self._get_video_details_batch(batch)
            self._video_details.extend(details)
            
            batch_num = i // self.batch_size + 1
            self.logger.info(f"Processed batch {batch_num}/{total_batches} ({len(details)} videos)")

        # Save final data to S3
        success = self.save_data_to_s3()
        
        if success:
            self.logger.info(f"Pipeline completed successfully. Saved {len(self._video_details)} videos with extraction date {self._extraction_date}")
        else:
            self.logger.error("Pipeline completed with errors")
            
        return success
    
    def get_statistics(self) -> Dict:
        """
        Get statistics about the harvested data.
        
        Returns:
            Dictionary containing statistics about the harvested data
        """
        return {
            "channels_processed": len(self._channel_playlists),
            "total_videos_found": len(self._all_video_ids),
            "videos_with_details": len(self._video_details),
            "processed_video_ids": len(self._processed_video_ids),
            "extraction_date": self._extraction_date
        }