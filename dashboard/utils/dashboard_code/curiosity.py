import streamlit as st
from utils.dashboard_code.texts import (
    TITLE_CURIOSITIES, 
    METRIC_CHANNEL_VIDEOS, 
    METRIC_CHANNEL_AVG, 
    METRIC_FIRST_VIDEO_CHANNEL, 
    METRIC_FIRST_VIDEO_DATE, 
    TITLE_VIDEOS_ENGAGEMENT, 
    TITLE_VIDEO_MOST_VIEWS, 
    TITLE_VIDEO_MOST_LIKES, 
    TITLE_VIDEO_MOST_COMMENTS, 
    TITLE_VIDEO_HIGHEST_ENGAGEMENT, 
    TITLE_CONTENT_FEATURES, 
    TITLE_LONGEST_VIDEO, 
    TITLE_LONG_VIDEO_MOST_VIEWS
)

def display_additional_info(df, df_videos_longos):
    st.title(TITLE_CURIOSITIES)
    
    canal_mais_videos = df['channel_name'].value_counts().idxmax()
    total_videos = df['channel_name'].value_counts().max()
    titulo_primeiro_video = df.loc[df['published_at'].idxmin(), 'title']
    canal_primeiro_video = df.loc[df['published_at'].idxmin(), 'channel_name']
    data_primeiro_video = df['published_at'].min().strftime('%d/%m/%Y')
    media_videos_mes = df.groupby('channel_name').size() / df['ano_mes_publish'].nunique()
    canal_mais_videos_mes = media_videos_mes.idxmax()
    media_max_videos = media_videos_mes.max()
    more_views = df.loc[df['view_count'].idxmax(), 'title']
    channel_more_views = df.loc[df['view_count'].idxmax(), 'channel_name']
    views = df.loc[df['view_count'].idxmax(), 'view_count']
    long_video_title = df_videos_longos.loc[df_videos_longos['view_count'].idxmax(), 'title']
    long_video_channel = df_videos_longos.loc[df_videos_longos['view_count'].idxmax(), 'channel_name']
    long_video_views = df_videos_longos.loc[df_videos_longos['view_count'].idxmax(), 'view_count']
    most_likes_title = df.loc[df['like_count'].idxmax(), 'title']
    most_likes_channel = df.loc[df['like_count'].idxmax(), 'channel_name']
    most_likes_count = df.loc[df['like_count'].idxmax(), 'like_count']
    most_comments_title = df.loc[df['comment_count'].idxmax(), 'title']
    most_comments_channel = df.loc[df['comment_count'].idxmax(), 'channel_name']
    most_comments_count = df.loc[df['comment_count'].idxmax(), 'comment_count']
    longest_video_title = df.loc[df['duration'].idxmax(), 'title']
    longest_video_channel = df.loc[df['duration'].idxmax(), 'channel_name']
    longest_duration = df.loc[df['duration'].idxmax(), 'duration']
    hours, remainder = divmod(longest_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    longest_duration_formatted = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    highest_engagement_title = df.loc[df['engagement_rate'].idxmax(), 'title']
    highest_engagement_channel = df.loc[df['engagement_rate'].idxmax(), 'channel_name']
    highest_engagement_rate = df.loc[df['engagement_rate'].idxmax(), 'engagement_rate']
    
    st.subheader("📈 Estatísticas de Canais")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**{METRIC_CHANNEL_VIDEOS}:** {canal_mais_videos} ({total_videos} vídeos)")
        st.markdown(f"**{METRIC_CHANNEL_AVG}:** {canal_mais_videos_mes} ({media_max_videos:.2f} vídeos/mês)")
    with col2:
        st.markdown(f"**{METRIC_FIRST_VIDEO_CHANNEL}:** {canal_primeiro_video} - {titulo_primeiro_video}")
        st.markdown(f"**{METRIC_FIRST_VIDEO_DATE}:** {data_primeiro_video}")
    
    st.subheader(TITLE_VIDEOS_ENGAGEMENT)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**{TITLE_VIDEO_MOST_VIEWS}:** {more_views}")
        st.markdown(f"**Canal:** {channel_more_views}")
        st.markdown(f"**Visualizações:** {views:,}")
        st.markdown("---")
        st.markdown(f"**{TITLE_VIDEO_MOST_LIKES}:** {most_likes_title}")
        st.markdown(f"**Canal:** {most_likes_channel}")
        st.markdown(f"**Likes:** {most_likes_count:,}")
    with col2:
        st.markdown(f"**{TITLE_VIDEO_MOST_COMMENTS}:** {most_comments_title}")
        st.markdown(f"**Canal:** {most_comments_channel}")
        st.markdown(f"**Comentários:** {most_comments_count:,}")
        st.markdown("---")
        st.markdown(f"**{TITLE_VIDEO_HIGHEST_ENGAGEMENT}:** {highest_engagement_title}")
        st.markdown(f"**Canal:** {highest_engagement_channel}")
        st.markdown(f"**Taxa de engajamento:** {highest_engagement_rate:.2f}%")
    
    st.subheader(TITLE_CONTENT_FEATURES)
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**{TITLE_LONGEST_VIDEO}:** {longest_video_title}")
        st.markdown(f"**Canal:** {longest_video_channel}")
        st.markdown(f"**Duração:** {longest_duration_formatted}")
    with col2:
        st.markdown(f"**{TITLE_LONG_VIDEO_MOST_VIEWS}:** {long_video_title}")
        st.markdown(f"**Canal:** {long_video_channel}")
        st.markdown(f"**Visualizações:** {long_video_views:,}")