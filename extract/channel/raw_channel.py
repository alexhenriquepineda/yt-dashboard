from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json


API_KEY = "AIzaSyBJR_D1gkuwYdTppmmMz6ZIdas78h4wRmE"
CHANNELS_IDS = ["UCGfrC6R2PSMeXv4hdhONB6Q",
                "UCEI44xNfQmAukxMf1kW8d5g",
                "UCd4g7oX49jtbX7rN-wclkJg",
                "UCmK5h2-a4CquS4nIxDN6j7g",
                "UCU_q46MCMEu5l2QdshV0hGQ",
                "UCPlemwX82_QEWRDC6yYnOCg",
                "UC6gJAnIhDn2Fy_jZfICmgSA",
                "UCNEUXTXD8IhDqxR7sosYOow",
                "UCy-R3H4s0z1HubB_uSiGsHA",
                "UC0EPH87G38158HBGzDSSZ1Q",
                "UCjbknPNDsL9fd860cDInPGw",
                "UCBtOMJf4ZB6yXZQx98vIN7w",
                "UCOOLLUHhjJ-I6dXTQ-5cq5g",
                "UC7RaQvO8fqoyd62RmIg_yGA",
                "UCA0eYfmteLgN7bFZaMDXGUw",
                "UCdRhxTB5X5XoJvamINrrijQ",
                "UCTmAy9D7NR8sHdn3Oup1wlQ",
                "UCaZLOsun_sY6BonjwR8Zo4A",
                "UC5BW1WJ3ioahNzFUBdJ7cLQ",
                "UCfvg8A2uKlOmC1xWnUk7tpQ",
                "UCbgmJsnqdzu9aHTEvQGupfg",
                "UCUOsr03iLj627hJm55cmIPw",
                "UCLfCo17TCjx7qf-JMhQioLQ",
                "UCPX0gLduKAfgr-HJENa7CFw"]


class YouTubeDataExtractor:
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
                request = request = self.youtube.channels().list(
                                            part='id,contentDetails,statistics,status,topicDetails,localizations,brandingSettings,snippet',
                                            id=','.join(chunk)
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


if __name__ == '__main__':
    extractor = YouTubeDataExtractor(API_KEY, CHANNELS_IDS)
    extractor.run()
