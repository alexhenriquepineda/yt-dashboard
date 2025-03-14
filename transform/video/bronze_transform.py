import json
import pandas as pd
import boto3
from typing import List, Dict, Any
from io import BytesIO

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


class VideoTransformBronze:
    def __init__(self, s3_bucket: str, raw_key: str, bronze_key: str):
        self.s3_bucket = s3_bucket
        self.raw_key = raw_key
        self.videos: List[Video] = []
        self.s3_client = boto3.client('s3')
        self.load_data()
        self.save_to_s3(bronze_key)

    def load_data(self):
        # Baixar o arquivo do S3
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.raw_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        self.videos = [Video(video) for video in data]

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame([video.to_dict() for video in self.videos])
        df['published_at'] = pd.to_datetime(df['published_at'], format='ISO8601')

        return df

    def save_to_s3(self, output_key: str):
        df = self.to_dataframe()
        # Usando BytesIO para criar um buffer binário
        parquet_buffer = BytesIO()
        # Salvando o DataFrame como Parquet no buffer
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        # Enviar o arquivo Parquet para o S3
        self.s3_client.put_object(Bucket=self.s3_bucket, Key=output_key, Body=parquet_buffer.getvalue())
