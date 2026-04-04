import boto3
import isodate
import numpy as np
import pandas as pd

from utils.s3_utils import read_parquet_from_s3, save_to_s3


class VideoTransformSilver:
    ANALYTICAL_COLUMNS = [
        "video_id",
        "title",
        "channel_id",
        "channel_name",
        "published_at",
        "published_at_date",
        "duration_iso",
        "duration",
        "duration_seconds",
        "definition",
        "thumbnail_url",
        "extraction_date",
        "pipeline_run_at",
        "view_count",
        "like_count",
        "comment_count",
        "previous_view_count",
        "views_gained_since_last_extract",
        "engagement_rate",
        "like_ratio",
        "comment_ratio",
        "dia_semana",
        "hora_publicacao",
        "mes",
        "ano",
        "dia",
        "semana_do_ano",
        "ano_mes_publish",
        "ano_mes_extraction",
        "has_thumbnail",
        "has_statistics",
        "is_short_candidate",
        "is_recently_published",
        "source_bucket",
        "source_key",
        "source_layer",
    ]

    NUMERIC_COLUMNS = [
        "view_count",
        "like_count",
        "comment_count",
        "previous_view_count",
        "views_gained_since_last_extract",
    ]

    BOOLEAN_COLUMNS = [
        "has_thumbnail",
        "has_statistics",
        "has_content_details",
        "has_snippet",
        "is_valid_video_id",
    ]

    def __init__(self, s3_bucket: str, bronze_key: str, silver_key: str):
        self.s3_client = boto3.client("s3")
        self.s3_bucket = s3_bucket
        self.bronze_key = bronze_key
        self.silver_key = silver_key
        self.df = read_parquet_from_s3(self.s3_client, self.s3_bucket, self.bronze_key)
        self.df = self.transform()
        save_to_s3(self.s3_client, self.df, self.s3_bucket, self.silver_key)

    def _parse_duration_seconds(self, duration_value):
        if pd.isna(duration_value):
            return np.nan
        try:
            return isodate.parse_duration(duration_value).total_seconds()
        except (isodate.ISO8601Error, TypeError, ValueError):
            return np.nan

    def _ensure_columns(self) -> None:
        for column in self.NUMERIC_COLUMNS + self.BOOLEAN_COLUMNS:
            if column not in self.df.columns:
                self.df[column] = 0 if column in self.NUMERIC_COLUMNS else False

        for column in ["extraction_date", "pipeline_run_at", "duration", "published_at"]:
            if column not in self.df.columns:
                self.df[column] = pd.NA

        for column in ["source_bucket", "source_key", "source_layer", "thumbnail_url", "definition"]:
            if column not in self.df.columns:
                self.df[column] = None

    def _coerce_types(self) -> None:
        self.df["published_at"] = pd.to_datetime(self.df["published_at"], errors="coerce", utc=True)
        self.df["extraction_date"] = pd.to_datetime(self.df["extraction_date"], errors="coerce", utc=True)
        self.df["pipeline_run_at"] = pd.to_datetime(self.df["pipeline_run_at"], errors="coerce", utc=True)

        for column in self.NUMERIC_COLUMNS:
            self.df[column] = pd.to_numeric(self.df[column], errors="coerce").fillna(0).clip(lower=0)
            self.df[column] = self.df[column].astype("int64")

        for column in self.BOOLEAN_COLUMNS:
            self.df[column] = self.df[column].fillna(False).astype(bool)

    def transform(self):
        if not isinstance(self.df, pd.DataFrame) or self.df.empty:
            return pd.DataFrame(columns=self.ANALYTICAL_COLUMNS)

        self._ensure_columns()
        self._coerce_types()

        self.df = self.df[self.df["is_valid_video_id"]].copy()

        self.df["duration_iso"] = self.df["duration"]
        self.df["duration_seconds"] = self.df["duration"].apply(self._parse_duration_seconds)
        self.df["duration"] = self.df["duration_seconds"]
        self.df["published_at_date"] = self.df["published_at"].dt.date
        self.df["dia_semana"] = self.df["published_at"].dt.day_name()
        self.df["hora_publicacao"] = self.df["published_at"].dt.hour
        self.df["mes"] = self.df["published_at"].dt.month
        self.df["ano"] = self.df["published_at"].dt.year
        self.df["dia"] = self.df["published_at"].dt.day
        self.df["semana_do_ano"] = self.df["published_at"].dt.isocalendar().week.astype("Int64")
        self.df["ano_mes_publish"] = self.df["published_at"].dt.to_period("M").astype(str)
        self.df["ano_mes_extraction"] = self.df["extraction_date"].dt.to_period("M").astype(str)

        self.df["is_short_candidate"] = self.df["duration_seconds"].le(60).fillna(False)
        self.df["is_recently_published"] = (
            (self.df["extraction_date"] - self.df["published_at"]).dt.days.le(30).fillna(False)
        )

        self.df["engagement_rate"] = np.where(
            self.df["view_count"] > 0,
            ((self.df["like_count"] + self.df["comment_count"]) / self.df["view_count"]) * 100,
            np.nan,
        )
        self.df["like_ratio"] = np.where(
            self.df["view_count"] > 0,
            (self.df["like_count"] / self.df["view_count"]) * 100,
            np.nan,
        )
        self.df["comment_ratio"] = np.where(
            self.df["view_count"] > 0,
            (self.df["comment_count"] / self.df["view_count"]) * 100,
            np.nan,
        )

        self.df = self.df.sort_values(
            by=["channel_id", "published_at", "video_id"],
            ascending=[True, False, True],
            na_position="last",
        ).reset_index(drop=True)

        for column in self.ANALYTICAL_COLUMNS:
            if column not in self.df.columns:
                self.df[column] = pd.NA

        return self.df[self.ANALYTICAL_COLUMNS]
