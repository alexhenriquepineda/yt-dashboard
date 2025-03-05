from config.aws import BRONZE_DATA, RAW_DATA, BUCKET_NAME
from transform.channel.channel_transform import YouTubeChannelTransform
from transform.video.video_transform import YouTubeVideoTransform

if __name__ == '__main__':

    #YouTubeChannelTransform(BUCKET_NAME, RAW_DATA + "/channel/channel_data.json", BRONZE_DATA + "/channel/channel_data.parquet")
    YouTubeVideoTransform(BUCKET_NAME, RAW_DATA + "/video/video_data.json", BRONZE_DATA + "/video/video_data.parquet")
    


