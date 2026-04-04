import json
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List

import boto3
import pandas as pd


class Video:
    def __init__(self, data: Dict[str, Any], source_bucket: str, source_key: str):
        snippet = data.get("snippet") or {}
        statistics = data.get("statistics") or {}
        content_details = data.get("contentDetails") or {}
        thumbnails = snippet.get("thumbnails") or {}
        high_thumbnail = thumbnails.get("high") or {}

        self.video_id = data.get("id")
        self.title = snippet.get("title")
        self.channel_id = snippet.get("channelId")
        self.channel_name = snippet.get("channelTitle")
        self.published_at = snippet.get("publishedAt")
        self.duration = content_details.get("duration")
        self.definition = content_details.get("definition")
        self.view_count = statistics.get("viewCount")
        self.like_count = statistics.get("likeCount")
        self.comment_count = statistics.get("commentCount")
        self.thumbnail_url = high_thumbnail.get("url")
        self.extraction_date = data.get("extraction_date")
        self.previous_view_count = data.get("previous_view_count")
        self.views_gained_since_last_extract = data.get("views_gained_since_last_extract")

        self.has_snippet = bool(snippet)
        self.has_statistics = bool(statistics)
        self.has_content_details = bool(content_details)
        self.has_thumbnail = bool(self.thumbnail_url)
        self.is_valid_video_id = isinstance(self.video_id, str) and bool(self.video_id.strip())

        self.pipeline_run_at = datetime.now(timezone.utc).isoformat()
        self.source_bucket = source_bucket
        self.source_key = source_key
        self.source_layer = "raw"
        self.raw_payload = json.dumps(data, ensure_ascii=False, sort_keys=True)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "video_id": self.video_id,
            "title": self.title,
            "channel_id": self.channel_id,
            "channel_name": self.channel_name,
            "published_at": self.published_at,
            "duration": self.duration,
            "definition": self.definition,
            "view_count": self.view_count,
            "like_count": self.like_count,
            "comment_count": self.comment_count,
            "thumbnail_url": self.thumbnail_url,
            "extraction_date": self.extraction_date,
            "previous_view_count": self.previous_view_count,
            "views_gained_since_last_extract": self.views_gained_since_last_extract,
            "has_snippet": self.has_snippet,
            "has_statistics": self.has_statistics,
            "has_content_details": self.has_content_details,
            "has_thumbnail": self.has_thumbnail,
            "is_valid_video_id": self.is_valid_video_id,
            "pipeline_run_at": self.pipeline_run_at,
            "source_bucket": self.source_bucket,
            "source_key": self.source_key,
            "source_layer": self.source_layer,
            "raw_payload": self.raw_payload,
        }


class VideoTransformBronze:
    EXPECTED_COLUMNS = [
        "video_id",
        "title",
        "channel_id",
        "channel_name",
        "published_at",
        "duration",
        "definition",
        "view_count",
        "like_count",
        "comment_count",
        "thumbnail_url",
        "extraction_date",
        "previous_view_count",
        "views_gained_since_last_extract",
        "has_snippet",
        "has_statistics",
        "has_content_details",
        "has_thumbnail",
        "is_valid_video_id",
        "pipeline_run_at",
        "source_bucket",
        "source_key",
        "source_layer",
        "raw_payload",
    ]

    NUMERIC_COLUMNS = [
        "view_count",
        "like_count",
        "comment_count",
        "previous_view_count",
        "views_gained_since_last_extract",
    ]

    BOOLEAN_COLUMNS = [
        "has_snippet",
        "has_statistics",
        "has_content_details",
        "has_thumbnail",
        "is_valid_video_id",
    ]

    TEXT_COLUMNS = [
        "video_id",
        "title",
        "channel_id",
        "channel_name",
        "duration",
        "definition",
        "thumbnail_url",
        "source_bucket",
        "source_key",
        "source_layer",
        "raw_payload",
    ]

    def __init__(self, s3_bucket: str, raw_key: str, bronze_key: str):
        self.s3_bucket = s3_bucket
        self.raw_key = raw_key
        self.videos: List[Video] = []
        self.s3_client = boto3.client("s3")
        self.load_data()
        self.save_to_s3(bronze_key)

    def load_data(self) -> None:
        obj = self.s3_client.get_object(Bucket=self.s3_bucket, Key=self.raw_key)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        self.videos = [
            Video(video, source_bucket=self.s3_bucket, source_key=self.raw_key)
            for video in data
            if isinstance(video, dict)
        ]

    def _ensure_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in self.EXPECTED_COLUMNS:
            if column not in df.columns:
                df[column] = None
        return df[self.EXPECTED_COLUMNS]

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
        df["extraction_date"] = pd.to_datetime(df["extraction_date"], errors="coerce", utc=True)
        df["pipeline_run_at"] = pd.to_datetime(df["pipeline_run_at"], errors="coerce", utc=True)

        for column in self.NUMERIC_COLUMNS:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).clip(lower=0)
            df[column] = df[column].astype("int64")

        for column in self.BOOLEAN_COLUMNS:
            df[column] = df[column].fillna(False).astype(bool)

        for column in self.TEXT_COLUMNS:
            df[column] = df[column].where(pd.notna(df[column]), None)

        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(
            by=["video_id", "extraction_date", "pipeline_run_at"],
            ascending=[True, False, False],
            na_position="last",
        )
        df = df.drop_duplicates(subset=["video_id"], keep="first")
        return df

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame([video.to_dict() for video in self.videos])
        if df.empty:
            return pd.DataFrame(columns=self.EXPECTED_COLUMNS)

        df = self._ensure_schema(df)
        df = self._coerce_types(df)
        df = df[df["is_valid_video_id"]].copy()
        df = self._deduplicate(df)
        df = df.sort_values(
            by=["channel_id", "published_at", "video_id"],
            ascending=[True, False, True],
            na_position="last",
        ).reset_index(drop=True)
        return df

    def save_to_s3(self, output_key: str) -> None:
        df = self.to_dataframe()
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=output_key,
            Body=parquet_buffer.getvalue(),
        )
