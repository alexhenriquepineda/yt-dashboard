from config.api_key import YT_API_KEY
from config.channel_id import CHANNELS_IDS
from config.aws import BUCKET_NAME, RAW_DATA
from extract.channel.channel_extract import YouTubeDataChannelExtractor
from extract.video.video_extract import YouTubeDataVideoExtractor

if __name__ == '__main__':
    channel_output_path = f"{RAW_DATA}/channel/channel_data.json"
    YouTubeDataChannelExtractor(
        YT_API_KEY,
        CHANNELS_IDS,
        BUCKET_NAME,
        channel_output_path
    )
    
    video_output_path = f"{RAW_DATA}/video/video_data.json"
    extractor = YouTubeDataVideoExtractor(
        YT_API_KEY,
        CHANNELS_IDS,
        BUCKET_NAME,
        video_output_path
    )
    print(extractor.get_statistics())
    