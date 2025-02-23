import json
from googleapiclient.discovery import build

# Replace with your YouTube Data API key
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"
API_KEY = "AIzaSyBJR_D1gkuwYdTppmmMz6ZIdas78h4wRmE"
# Build the YouTube service object
youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)


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


def get_channels_uploads_playlist_ids(channel_ids):
    """
    Retrieve the uploads playlist IDs for multiple channels in a single request.
    Returns a dictionary mapping channel ID to its uploads playlist ID.
    """
    channel_playlists = {}
    # The API allows up to 50 channel IDs per request.
    request = youtube.channels().list(
        part="contentDetails",
        id=",".join(channel_ids)
    )
    response = request.execute()
    
    for item in response.get("items", []):
        channel_id = item["id"]
        uploads_playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]
        channel_playlists[channel_id] = uploads_playlist_id
    return channel_playlists

def get_videos_from_playlist(playlist_id):
    """
    Retrieve all video IDs from the given uploads playlist.
    """
    video_ids = []
    next_page_token = None
    
    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return video_ids

def get_video_details_in_batches(video_ids, batch_size=50):
    """
    Retrieve video details for a list of video IDs in batches.
    Returns a list of video detail dictionaries.
    """
    all_video_details = []
    # Process video IDs in batches (max 50 IDs per request)
    for i in range(0, len(video_ids), batch_size):
        batch_ids = video_ids[i:i + batch_size]
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch_ids)
        )
        response = request.execute()
        all_video_details.extend(response.get("items", []))
    return all_video_details

def main():
    all_video_ids = []
    
    # Get the uploads playlist IDs for all channels
    channel_playlists = get_channels_uploads_playlist_ids(CHANNELS_IDS)
    if not channel_playlists:
        print("No valid channels found.")
        return

    # For each channel, get all video IDs from its uploads playlist
    for channel_id, playlist_id in channel_playlists.items():
        print(f"Processing channel {channel_id} with uploads playlist {playlist_id}")
        video_ids = get_videos_from_playlist(playlist_id)
        print(f"  Found {len(video_ids)} videos.")
        all_video_ids.extend(video_ids)
    
    print(f"Total videos to process: {len(all_video_ids)}")
    
    # Get video details in batches
    all_videos_info = get_video_details_in_batches(all_video_ids)
    
    # Save the gathered video details to a JSON file
    with open("./data/raw/videos/videos_data.json", "w", encoding="utf-8") as f:
        json.dump(all_videos_info, f, indent=2, ensure_ascii=False)
    
    print("All video information saved to videos.json")


if __name__ == "__main__":
    main()
