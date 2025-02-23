import json
from googleapiclient.discovery import build

# Replace with your YouTube Data API key
API_KEY = "AIzaSyBJR_D1gkuwYdTppmmMz6ZIdas78h4wRmE"
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# Build the YouTube service object
youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

CHANNELS_IDS = ["UCmK5h2-a4CquS4nIxDN6j7g", "UC0EPH87G38158HBGzDSSZ1Q"]


def get_uploads_playlist_id(channel_id):
    """
    Retrieve the uploads playlist ID for the given channel.
    Each YouTube channel has an 'uploads' playlist containing all its videos.
    """
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    
    items = response.get("items", [])
    if not items:
        print(f"No channel found with ID: {channel_id}")
        return None
    
    uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]
    return uploads_playlist_id

def get_videos_from_playlist(playlist_id):
    """
    Retrieve all videos (as playlist items) from the given uploads playlist.
    """
    videos = []
    next_page_token = None
    
    while True:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,  # maximum allowed per request
            pageToken=next_page_token
        )
        response = request.execute()
        videos.extend(response.get("items", []))
        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return videos

def get_video_details(video_id):
    """
    Retrieve detailed information for a given video ID.
    """
    request = youtube.videos().list(
        part="snippet,statistics,contentDetails",
        id=video_id
    )
    response = request.execute()
    items = response.get("items", [])
    return items[0] if items else None

def main():
    all_videos_info = []
    
    for channel_id in CHANNELS_IDS:
        print(f"Processing channel: {channel_id}")
        uploads_playlist_id = get_uploads_playlist_id(channel_id)
        if not uploads_playlist_id:
            continue
        
        print(f"  Found uploads playlist: {uploads_playlist_id}")
        playlist_videos = get_videos_from_playlist(uploads_playlist_id)
        print(f"  Total videos found: {len(playlist_videos)}")
        
        for item in playlist_videos:
            video_id = item["contentDetails"]["videoId"]
            video_info = get_video_details(video_id)
            if video_info:
                all_videos_info.append(video_info)
    
    # Save the gathered video details to a JSON file
    with open("./data/raw/videos/video_data.json", "w", encoding="utf-8") as f:
        json.dump(all_videos_info, f, indent=2, ensure_ascii=False)
    
    print("All video information saved to videos.json")

if __name__ == "__main__":
    main()
