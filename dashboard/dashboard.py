import streamlit as st
import pandas as pd
import altair as alt
import datetime
import numpy as np
import plotly.express as px
from pathlib import Path


st.set_page_config(
    page_title="YouTube Dashboard",
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")

path = Path(__file__).parents[1] / 'data/bronze/video/video_data.csv'
df = pd.read_csv(path, sep = ";")
df['published_at'] = pd.to_datetime(df['published_at'], format='ISO8601')
df["ano_mes_publish"] = df['published_at'].apply(lambda x: f"{x.year}-{x.month:02}")

with st.sidebar:
    st.title('🏂 YouTube Dashboard')
    st.write('This dashboard is a tool to analyze the YouTube data')
    channel_list = ["Todos"] + list(df["channel_name"].unique())[::-1]
    selected_channels = st.multiselect('Select channels', channel_list, default="Todos")

if "Todos" in selected_channels:
    df = df
else:
    df = df[df["channel_name"].isin(selected_channels)]


df_videos_longos = df[df['duration'] > 90]
df_videos_curtos = df[df['duration'] <= 90]
st.title( 'OVERVIEW' )
c1, c2 = st.columns( (1,1) )
c1.header('Número de vídeos longos publicados')
c2.header('Número de visualizações de vídeos longos')
c1.metric('Total', df_videos_longos.shape[0])
c2.metric('Total', f"{df_videos_longos['view_count'].sum():,}".replace(",", "."))

aux = (
df_videos_longos[["ano_mes_publish", "video_id"]]
.groupby(["ano_mes_publish"]).agg(qtd_videos = ("video_id", "count"))
.reset_index()
)


fig = px.line(aux, x='ano_mes_publish', y='qtd_videos', labels={'ano_mes_publish': 'Mês', 'qtd_videos': 'Quantidade de Vídeos'})
c1.plotly_chart(fig, use_container_width=True)

aux = (
df_videos_longos[["ano_mes_publish", "view_count"]]
.groupby(["ano_mes_publish"]).agg(qtd_views = ("view_count", "sum"))
.reset_index()
)

fig = px.line(aux, x='ano_mes_publish', y='qtd_views', labels={'ano_mes_publish': 'Mês', 'qtd_videos': 'Quantidade de Vídeos'})
c2.plotly_chart(fig, use_container_width=True)






c1, c2 = st.columns( (1,1) )
c1.header('Número de vídeos curtos publicados')
c2.header('Número de visualizações de vídeos curtos')
c1.metric('Total', df_videos_curtos.shape[0])
c2.metric('Total', f"{df_videos_curtos['view_count'].sum():,}".replace(",", "."))


aux = (
df_videos_curtos[["ano_mes_publish", "video_id"]]
.groupby(["ano_mes_publish"]).agg(qtd_videos = ("video_id", "count"))
.reset_index()
)


fig = px.line(aux, x='ano_mes_publish', y='qtd_videos', labels={'ano_mes_publish': 'Mês', 'qtd_videos': 'Quantidade de Vídeos'})
c1.plotly_chart(fig, use_container_width=True)

aux = (
df_videos_curtos[["ano_mes_publish", "view_count"]]
.groupby(["ano_mes_publish"]).agg(qtd_views = ("view_count", "sum"))
.reset_index()
)

fig = px.line(aux, x='ano_mes_publish', y='qtd_views', labels={'ano_mes_publish': 'Mês', 'qtd_videos': 'Quantidade de Vídeos'})
c2.plotly_chart(fig, use_container_width=True)


# Calculate engagement rate
df['engagement_rate'] = np.where(
    df['view_count'] > 0,
    (df['like_count'] + df['comment_count']) / df['view_count'],
    np.nan
) * 100  # Convert to percentage

# Calculate individual component ratios
df['like_ratio'] = (df['like_count'] / df['view_count']) * 100
df['comment_ratio'] = (df['comment_count'] / df['view_count'])


# Filter data for January 2024 and January 2025
df_jan_2024 = df[(df['published_at'].dt.year == 2024) & (df['published_at'].dt.month == 1)]
st.dataframe(df_jan_2024)
df_jan_2025 = df[(df['published_at'].dt.year == 2025) & (df['published_at'].dt.month == 1)]



# Aggregate data for January 2024
agg_jan_2024 = df_jan_2024.groupby('channel_name').agg(
    total_views_2024=('view_count', 'sum'),
    total_likes_2024=('like_count', 'sum'),
    total_comments_2024=('comment_count', 'sum'),
    avg_engagement_rate_2024=('engagement_rate', 'mean'),
    avg_like_ratio_2024=('like_ratio', 'mean'),
    avg_comment_ratio_2024=('comment_ratio', 'mean')
).reset_index()
st.dataframe(agg_jan_2024, use_container_width=True)

# Aggregate data for January 2025
agg_jan_2025 = df_jan_2025.groupby('channel_name').agg(
    total_views_2025=('view_count', 'sum'),
    total_likes_2025=('like_count', 'sum'),
    total_comments_2025=('comment_count', 'sum'),
    avg_engagement_rate_2025=('engagement_rate', 'mean'),
    avg_like_ratio_2025=('like_ratio', 'mean'),
    avg_comment_ratio_2025=('comment_ratio', 'mean')
).reset_index()

# Merge the dataframes
comparison_df = pd.merge(agg_jan_2024, agg_jan_2025, on='channel_name', how='outer').fillna(0)

# Calculate the differences
comparison_df['view_diff'] = comparison_df['total_views_2025'] - comparison_df['total_views_2024']
comparison_df['like_diff'] = comparison_df['total_likes_2025'] - comparison_df['total_likes_2024']
comparison_df['comment_diff'] = comparison_df['total_comments_2025'] - comparison_df['total_comments_2024']

st.title('Comparativo de Janeiro 2025 vs Janeiro 2024')

st.write('Comparativo do número de visualizações, likes e comentários de Janeiro 2025 com Janeiro 2024 de todos os canais')

st.dataframe(comparison_df, use_container_width=True)

