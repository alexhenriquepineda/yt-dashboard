from config.aws import BRONZE_DATA, RAW_DATA, BUCKET_NAME, SILVER_DATA
from transform.channel.channel_transform import YouTubeChannelTransform
from transform.video.bronze_transform import VideoTransformBronze
from transform.video.silver_transform import VideoTransformSilver


if __name__ == '__main__':
    # RAW TO BRONZE - Channel data
    channel_raw_path = f"{RAW_DATA}/channel/channel_data.json"
    channel_bronze_path = f"{BRONZE_DATA}/channel/channel_data.parquet"
    YouTubeChannelTransform(BUCKET_NAME, channel_raw_path, channel_bronze_path)

    # RAW TO BRONZE - Video data
    video_raw_path = f"{RAW_DATA}/video/video_data.json"
    video_bronze_path = f"{BRONZE_DATA}/video/video_data.parquet"
    VideoTransformBronze(BUCKET_NAME, video_raw_path, video_bronze_path)

    # BRONZE TO SILVER - Video data
    video_silver_path = f"{SILVER_DATA}/video/video_data.parquet"
    VideoTransformSilver(BUCKET_NAME, video_bronze_path, video_silver_path)
    


