import json
import isodate
import pandas as pd
from typing import List, Dict, Any

class Video:
    def __init__(self, data: Dict[str, Any]):
        self.video_id = data.get('id')
        self.title = data.get('snippet', {}).get('title')
        self.channel_id = data.get('snippet', {}).get('channelId')
        self.channel_name = data.get('snippet', {}).get('channelTitle')
        self.published_at = data.get('snippet', {}).get('publishedAt')
        self.duration = data.get('contentDetails', {}).get('duration')
        self.definition = data.get('contentDetails', {}).get('definition')
        self.view_count = int(data.get('statistics', {}).get('viewCount', 0))
        self.like_count = int(data.get('statistics', {}).get('likeCount', 0))
        self.comment_count = int(data.get('statistics', {}).get('commentCount', 0))
        self.thumbnail_url = data.get('snippet', {}).get('thumbnails', {}).get('high', {}).get('url')

    def to_dict(self) -> Dict[str, Any]:
        return {
            'video_id': self.video_id,
            'title': self.title,
            'channel_id': self.channel_id,
            'channel_name': self.channel_name,
            'published_at': self.published_at,
            'duration': self.duration,
            'definition': self.definition,
            'view_count': self.view_count,
            'like_count': self.like_count,
            'comment_count': self.comment_count,
            'thumbnail_url': self.thumbnail_url
        }


class YouTubeVideoTransform:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.videos: List[Video] = []

    def load_data(self):
        with open(self.file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            self.videos = [Video(video) for video in data]

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame([video.to_dict() for video in self.videos])
        df['published_at'] = pd.to_datetime(df['published_at'], format='ISO8601')
        df['duration'] = df['duration'].apply(lambda d: isodate.parse_duration(d).total_seconds() if pd.notna(d) else None)

        return df

    def save_to_csv(self, output_path: str):
        df = self.to_dataframe()
        df.to_csv(output_path, index=False, sep=';')
