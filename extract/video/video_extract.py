import json
import logging
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeDataVideoExtractor:
    """A class to harvest YouTube channel and video data through the YouTube Data API."""
    
    def __init__(
        self, 
        api_key: str, 
        channel_ids: List[str], 
        bucket_name: str, 
        s3_key: str, 
        dash_name: str,
        batch_size: int = 50,
        auto_execute: bool = True
    ):
        """
        Initialize the YouTube data harvester.
        
        Args:
            api_key: YouTube Data API v3 key
            channel_ids: List of YouTube channel IDs to process
            bucket_name: AWS S3 bucket name
            s3_key: Path within the S3 bucket to save the file (e.g., 'data/raw/video/videos_data.json')
            dash_name: Dashboard name to tag all records with
            batch_size: Number of videos to process in each API batch request
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
        self.dash_name = dash_name
        self.batch_size = batch_size
        
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
                response = self.youtube.channels().list(
                    part="contentDetails",
                    id=",".join(batch),
                    maxResults=channel_batch_size
                ).execute()
                
                # Extract upload playlist IDs
                for item in response.get("items", []):
                    channel_id = item["id"]
                    playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
                    result[channel_id] = playlist_id
                    
            except HttpError as e:
                self.logger.error(f"YouTube API error fetching channel details: {e}")
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
                request = self.youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,  # Maximum allowed by YouTube API
                    pageToken=next_page_token
                )
                response = request.execute()
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
                    
            except HttpError as e:
                self.logger.error(f"YouTube API error fetching playlist items: {e}")
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
        
        try:
            response = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids)
            ).execute()
            
            return response.get("items", [])
            
        except HttpError as e:
            self.logger.error(f"YouTube API error fetching video details: {e}")
            return []

    def _add_dashboard_tag(self, dash_name: str) -> None:
        """
        Add a dashboard tag to all video records.
        
        Args:
            dash_name: Name of the dashboard to tag records with
        """
        self.logger.info(f"Adding dashboard tag '{dash_name}' to {len(self._video_details)} records")
        
        for record in self._video_details:
            record['dashboard'] = dash_name

    def save_data_to_s3(self) -> bool:
        """
        Save the collected video data as a JSON file to AWS S3.
        
        Returns:
            Boolean indicating success or failure
        """
        if not self._video_details:
            self.logger.warning("No video data to save")
            return False

        # Add the dashboard tag
        self._add_dashboard_tag(self.dash_name)

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
        for channel_id, playlist_id in playlists.items():
            self.logger.info(f"Processing channel {channel_id}")
            video_ids = self._harvest_playlist_videos(playlist_id)
            self._all_video_ids.extend(video_ids)

        # Process videos in batches
        total_videos = len(self._all_video_ids)
        self.logger.info(f"Processing {total_videos} videos in batches of {self.batch_size}")
        
        for i in range(0, total_videos, self.batch_size):
            batch = self._all_video_ids[i:i + self.batch_size]
            details = self._get_video_details_batch(batch)
            self._video_details.extend(details)
            
            batch_num = i // self.batch_size + 1
            total_batches = (total_videos - 1) // self.batch_size + 1
            self.logger.info(f"Processed batch {batch_num}/{total_batches} ({len(details)} videos)")

        # Save data to S3
        success = self.save_data_to_s3()
        
        if success:
            self.logger.info("Pipeline completed successfully")
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
            "dashboard_name": self.dash_name
        }
    