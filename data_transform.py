from config.path import RAW_DATA, BRONZE_DATA
from transform.channel.channel_transform import YouTubeChannelTransform
from transform.video.video_transform import YouTubeVideoTransform

if __name__ == '__main__':

    yt_data = YouTubeChannelTransform(RAW_DATA + "/channel/channels_data.json")
    yt_data.run(BRONZE_DATA + "/channel/channels_data.csv")

    yt_video_transform = YouTubeVideoTransform(RAW_DATA + "/video/videos_data.json")
    yt_video_transform.load_data()
    yt_video_transform.save_to_csv(BRONZE_DATA + "/video/video_data.csv")
