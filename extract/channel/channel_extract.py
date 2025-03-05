import json
import boto3
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeDataChannelExtractor:
    def __init__(self, api_key, channel_ids, bucket_name, s3_key, dash_name):
        """
        Initializes the extractor with API credentials, channel IDs, and S3 storage details.

        Args:
            api_key (str): Your YouTube Data API key.
            channel_ids (list): List of YouTube channel IDs to extract data for.
            bucket_name (str): Name of the AWS S3 bucket.
            s3_key (str): Path where the file will be stored in S3.
        """
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.data = []
        self.s3_client = boto3.client("s3")
        self.get_channels_data()
        self.add_dashboard_column(dash_name)
        self.save_data_to_s3()

    def chunk_list(self, lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    def get_channels_data(self):
        chunks = self.chunk_list(self.channel_ids, 50)
        for chunk in chunks:
            try:
                request = self.youtube.channels().list(
                    part='id,contentDetails,statistics,status,topicDetails,localizations,brandingSettings,snippet',
                    id=','.join(chunk)
                )
                response = request.execute()
                self.data.extend(response.get('items', []))
            except HttpError as e:
                print(f"An error occurred: {e}")
        return self.data

    def add_dashboard_column(self, dash_name):
        """
        Adds a new column called 'dashboard' with the value 'value' to all records.
        """
        for record in self.data:
            record['dashboard'] = dash_name

    def save_data_to_s3(self):
        """
        Saves the retrieved channel data as a JSON file to AWS S3.
        """
        json_data = json.dumps(self.data, indent=4)

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.s3_key,
                Body=json_data,
                ContentType="application/json"
            )
            print(f"Data successfully uploaded to s3://{self.bucket_name}/{self.s3_key}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")

    def run(self):
        self.get_channels_data()
        self.add_dashboard_column()
        self.save_data_to_s3()
