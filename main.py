from config.api_key import API_KEY
from config.channel_id import CHANNELS_IDS
from extract.channel.channel_extract import YouTubeDataChannelExtractor
from extract.video.videos_extract import YouTubeDataVideoExtractor

if __name__ == '__main__':
    channel_extractor = YouTubeDataChannelExtractor(API_KEY, CHANNELS_IDS)
    channel_extractor.run()

    video_extractor = YouTubeDataVideoExtractor(API_KEY, CHANNELS_IDS)
    video_extractor.execute_pipeline()
