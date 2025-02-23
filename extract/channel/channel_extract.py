import json
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class YouTubeDataChannelExtractor:
    def __init__(self, api_key, channel_ids, output_file='./data/raw/channel/channels_data.json'):
        """
        Initializes the extractor with API credentials, channel IDs, and an output file name.

        Args:
            api_key (str): Your YouTube Data API key.
            channel_ids (list): List of YouTube channel IDs to extract data for.
            output_file (str): Filename where the data will be saved.
        """
        self.api_key = api_key
        self.channel_ids = channel_ids
        self.output_file = output_file
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        self.data = []

    def chunk_list(self, lst, size):
        """
        Splits a list into smaller chunks of a specified size.

        Args:
            lst (list): The list to be chunked.
            size (int): The maximum size of each chunk.

        Returns:
            list: A list of chunks.
        """
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    def get_channels_data(self):
        """
        Retrieves YouTube channel data for all provided channel IDs in chunks.

        Returns:
            list: Aggregated list of channel data dictionaries.
        """
        chunks = self.chunk_list(self.channel_ids, 50)
        for chunk in chunks:
            try:
                request = (
                    self.youtube.channels()
                    .list(
                        part='id,contentDetails,statistics,status,topicDetails,localizations,brandingSettings,snippet',
                        id=','.join(chunk)
                        )
                        )
                response = request.execute()
                self.data.extend(response.get('items', []))
            except HttpError as e:
                print(f"An error occurred: {e}")
        return self.data

    def save_data(self):
        """
        Saves the retrieved channel data to a JSON file.
        """
        with open(self.output_file, 'w') as f:
            json.dump(self.data, f, indent=4)
        print(f"Data saved to {self.output_file}")

    def run(self):
        """
        Runs the complete process: retrieving the data and saving it to a file.
        """
        self.get_channels_data()
        self.save_data()
