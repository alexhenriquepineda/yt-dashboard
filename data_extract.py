from config.api_key import API_KEY
from config.channel_id import CHANNELS_IDS
from config.aws import BUCKET_NAME, RAW_DATA
from extract.channel.channel_extract import YouTubeDataChannelExtractor
from extract.video.video_extract import YouTubeDataVideoExtractor

if __name__ == '__main__':
    YouTubeDataChannelExtractor(API_KEY, CHANNELS_IDS, BUCKET_NAME, RAW_DATA + "/channel/channel_data.json", "FITNESS")
    

    extractor = YouTubeDataVideoExtractor(API_KEY, CHANNELS_IDS, BUCKET_NAME, RAW_DATA + "/video/video_data.json", "FITNESS")

    stats = extractor.get_statistics()
    print(stats)
    
