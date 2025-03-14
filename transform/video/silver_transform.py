import boto3
import isodate
import pandas as pd
import numpy as np
from utils.s3_utils import read_parquet_from_s3, save_to_s3


class VideoTransformSilver:
    def __init__(self, s3_bucket: str, bronze_key: str, silver_key: str):
        """
        Initialize the VideoSilverTransform class.
        
        Args:
            s3_bucket (str): S3 bucket name
            bronze_key (str): S3 key for the bronze layer data
            silver_key (str): S3 key for the silver layer data output
            s3_client (boto3.client): S3 client
            read_parquet_from_s3 (callable): Function to read Parquet from S3
            save_to_s3 (callable): Function to save DataFrame to S3
        """
        self.s3_client = boto3.client("s3")
        self.s3_bucket = s3_bucket
        self.bronze_key = bronze_key
        self.silver_key = silver_key
        self.df = read_parquet_from_s3(self.s3_client, self.s3_bucket, self.bronze_key)
        self.df = self.transform()
        save_to_s3(self.s3_client, self.df, self.s3_bucket, self.silver_key)

    def transform(self):
        """
        Transform the data from bronze to silver layer by adding derived columns
        and performing necessary data transformations.
        
        Returns:
            pd.DataFrame: Transformed DataFrame with additional columns
        """
        if 'published_at' in self.df.columns:
            self.df['published_at'] = pd.to_datetime(self.df['published_at'])
            self.df['duration'] = self.df['duration'].apply(lambda d: isodate.parse_duration(d).total_seconds() if pd.notna(d) else None)
            self.df['dia_semana'] = self.df['published_at'].dt.day_name()
            self.df['hora_publicacao'] = self.df['published_at'].dt.hour
            self.df['mes'] = self.df['published_at'].dt.month
            self.df['ano'] = self.df['published_at'].dt.year
            self.df['dia'] = self.df['published_at'].dt.day
            self.df['semana_do_ano'] = self.df['published_at'].dt.isocalendar().week
            self.df["ano_mes_publish"] = self.df['published_at'].apply(lambda x: f"{x.year}-{x.month:02}")
            
            # Add engagement metrics
            self.df['engagement_rate'] = np.where(
                self.df['view_count'] > 0,
                (self.df['like_count'] + self.df['comment_count']) / self.df['view_count'],
                np.nan
            ) * 100
            
            self.df['like_ratio'] = np.where(
                self.df['view_count'] > 0,
                (self.df['like_count'] / self.df['view_count']) * 100,
                np.nan
            )
            
            self.df['comment_ratio'] = np.where(
                self.df['view_count'] > 0,
                (self.df['comment_count'] / self.df['view_count']) * 100,
                np.nan
            )
        
        return self.df

