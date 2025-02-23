import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Dict


class YouTubeDataVideoExtractor:
    """A class to harvest YouTube channel and video data through the YouTube Data API."""
    
    def __init__(self, api_key: str, channel_ids: List[str], save_path: str = "./data/raw/videos/videos_data.json"):
        """
        Initialize the YouTube data harvester.
        
        Args:
            api_key: YouTube Data API v3 key
            channel_ids: List of YouTube channel IDs to process
            save_path: Path to save the final JSON output
        """
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.save_path = save_path
        
        # Initialize YouTube API service
        self.youtube = build(
            self.api_service_name,
            self.api_version,
            developerKey=self.api_key
        )
        
        # State management
        self._channel_playlists: Dict[str, str] = {}
        self._all_video_ids: List[str] = []
        self._video_details: List[dict] = []

    def _get_channel_uploads_playlists(self) -> Dict[str, str]:
        """
        Retrieve uploads playlist IDs for all configured channels.
        
        Returns:
            Dictionary mapping channel IDs to their uploads playlist IDs
        """
        if not self.channel_ids:
            raise ValueError("No channel IDs configured")
        
        try:
            response = self.youtube.channels().list(
                part="contentDetails",
                id=",".join(self.channel_ids),
                maxResults=50
            ).execute()
        except HttpError as e:
            print(f"API Error fetching channel details: {e}")
            return {}

        self._channel_playlists = {
            item["id"]: item["contentDetails"]["relatedPlaylists"]["uploads"]
            for item in response.get("items", [])
        }
        return self._channel_playlists

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
        
        while True:
            try:
                request = self.youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
            except HttpError as e:
                print(f"API Error fetching playlist items: {e}")
                break

            video_ids.extend(
                item["contentDetails"]["videoId"]
                for item in response.get("items", [])
            )

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        return video_ids

    def _get_video_details_batch(self, video_ids: List[str]) -> List[dict]:
        """
        Retrieve detailed metadata for a batch of video IDs.
        
        Args:
            video_ids: List of YouTube video IDs to process
            
        Returns:
            List of video metadata dictionaries
        """
        try:
            response = self.youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(video_ids)
            ).execute()
        except HttpError as e:
            print(f"API Error fetching video details: {e}")
            return []

        return response.get("items", [])

    def _save_video_data(self) -> None:
        """Save collected video data to JSON file."""
        if not self._video_details:
            print("No video data to save")
            return

        try:
            with open(self.save_path, "w", encoding="utf-8") as f:
                json.dump(self._video_details, f, indent=2, ensure_ascii=False)
            print(f"Successfully saved data to {self.save_path}")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error saving data: {e}")

    def execute_pipeline(self) -> None:
        """Execute complete data harvesting pipeline."""
        # Step 1: Get uploads playlists for all channels
        playlists = self._get_channel_uploads_playlists()
        if not playlists:
            print("No valid channels found")
            return

        # Step 2: Harvest video IDs from all playlists
        for channel_id, playlist_id in playlists.items():
            print(f"Processing channel {channel_id}")
            video_ids = self._harvest_playlist_videos(playlist_id)
            print(f"Found {len(video_ids)} videos")
            self._all_video_ids.extend(video_ids)

        # Step 3: Get video details in batches
        batch_size = 50
        total_videos = len(self._all_video_ids)
        print(f"Processing {total_videos} videos in batches of {batch_size}")
        
        for i in range(0, total_videos, batch_size):
            batch = self._all_video_ids[i:i + batch_size]
            details = self._get_video_details_batch(batch)
            self._video_details.extend(details)
            print(f"Processed batch {i//batch_size + 1}/{(total_videos-1)//batch_size + 1}")

        # Step 4: Save results
        self._save_video_data()


if __name__ == "__main__":
    # Configuration
    CHANNEL_IDS = [
        "UCGfrC6R2PSMeXv4hdhONB6Q",
        "UCEI44xNfQmAukxMf1kW8d5g",
        # ... rest of channel IDs
    ]
    API_KEY = "AIzaSyBJR_D1gkuwYdTppmmMz6ZIdas78h4wRmE"
    
    # Execute pipeline
    harvester = YouTubeDataHarvester(
        api_key=API_KEY,
        channel_ids=CHANNEL_IDS
    )
    harvester.execute_pipeline()
