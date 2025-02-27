import os
import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import plotly.express as px
from pathlib import Path
import plotly.graph_objects as go

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

#with st.sidebar:
#    st.title('🏂 YouTube Dashboard')
#    st.write('This dashboard is a tool to analyze the YouTube data')
#    channel_list = ["Todos"] + list(df["channel_name"].unique())[::-1]
#    selected_channels = st.multiselect('Select channels', channel_list, default="Todos")

#if "Todos" in selected_channels:
#    df = df
#else:
#    df = df[df["channel_name"].isin(selected_channels)]
# Calculate engagement rate
df['engagement_rate'] = np.where(
    df['view_count'] > 0,
    (df['like_count'] + df['comment_count']) / df['view_count'],
    np.nan
) * 100  # Convert to percentage

# Calculate individual component ratios
df['like_ratio'] = (df['like_count'] / df['view_count']) * 100
df['comment_ratio'] = (df['comment_count'] / df['view_count'])

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
c1.header('Número de vídeos curtos publicados (<= 90 segundos)')
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




# Filter data for January 2024 and January 2025
df_jan_2024 = df_videos_longos[(df_videos_longos['published_at'].dt.year == 2024) & (df_videos_longos['published_at'].dt.month == 1)]

df_jan_2025 = df_videos_longos[(df_videos_longos['published_at'].dt.year == 2025) & (df_videos_longos['published_at'].dt.month == 1)]

st.title( 'Análise Individual dos canais' )
st.markdown("""
### 📌 Indicadores de desempenho:
Para analisar o desempenho dos canais no youtube, separamos quatro indicadores: 
- 📊 **Número de Views, Comentários e Likes no mês.**
- 📈 **Taxa de engajamento que corresponde ao => (Número delikes + número de comentários) / Total de visualizações no mes.**       
""")

st.markdown("<br>", unsafe_allow_html=True)

st.markdown("""
### VÍDEOS LONGOS
""")
# Lista de meses de 2024
meses_2024 = range(1, 13)  # Janeiro a Dezembro

for mes in meses_2024:
    # Filtrando os dados para o mês de 2024
    df_mes_2024 = df_videos_longos[(df_videos_longos['published_at'].dt.year == 2024) & (df_videos_longos['published_at'].dt.month == mes)]
    
    # Verificando se há dados para o mês de 2025
    df_mes_2025 = df_videos_longos[(df_videos_longos['published_at'].dt.year == 2025) & (df_videos_longos['published_at'].dt.month == mes)]
    
    # Se houver dados para 2024
    if not df_mes_2024.empty:
        # Agregação dos dados de 2024
        agg_mes_2024 = df_mes_2024.groupby('channel_name').agg(
            total_views_2024=('view_count', 'sum'),
            total_likes_2024=('like_count', 'sum'),
            total_comments_2024=('comment_count', 'sum'),
            avg_engagement_rate_2024=('engagement_rate', 'mean'),
            avg_like_ratio_2024=('like_ratio', 'mean'),
            avg_comment_ratio_2024=('comment_ratio', 'mean')
        ).reset_index()

    # Se houver dados para 2025
    if not df_mes_2025.empty:
        # Agregação dos dados de 2025
        agg_mes_2025 = df_mes_2025.groupby('channel_name').agg(
            total_views_2025=('view_count', 'sum'),
            total_likes_2025=('like_count', 'sum'),
            total_comments_2025=('comment_count', 'sum'),
            avg_engagement_rate_2025=('engagement_rate', 'mean'),
            avg_like_ratio_2025=('like_ratio', 'mean'),
            avg_comment_ratio_2025=('comment_ratio', 'mean')
        ).reset_index()

        # Mesclar os dados de 2024 e 2025 pelos canais, preenchendo valores ausentes
        agg_mes_comparativo = pd.merge(agg_mes_2024, agg_mes_2025, on='channel_name', how='outer')

        # Criando o gráfico
        fig = go.Figure()

        # Gráfico de barras para número de views de 2024
        fig.add_trace(go.Bar(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['total_views_2024'],
            name=f'Views {mes} 2024',
            marker_color='lightskyblue'
        ))

        # Gráfico de barras para número de views de 2025
        fig.add_trace(go.Bar(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['total_views_2025'],
            name=f'Views {mes} 2025',
            marker_color='lightcoral'
        ))

        # Gráfico de linha para a taxa de engajamento de 2024
        fig.add_trace(go.Scatter(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['avg_engagement_rate_2024'],
            name=f'Engagement Rate {mes} 2024',
            mode='lines+markers',
            marker_color='darkorange',
            yaxis='y2'
        ))

        # Gráfico de linha para a taxa de engajamento de 2025
        fig.add_trace(go.Scatter(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['avg_engagement_rate_2025'],
            name=f'Engagement Rate {mes} 2025',
            mode='lines+markers',
            marker_color='darkred',
            yaxis='y2'
        ))

        # Atualizando o layout para incluir dois eixos y
        fig.update_layout(
            title=f"Número de views em vídeos longos por canal e taxa de engajamento em {pd.to_datetime(str(mes), format='%m').strftime('%B')} de 2024 e 2025",
            xaxis=dict(title="Channel Name"),
            yaxis=dict(
                title="Number of Views",
                side="left"
            ),
            yaxis2=dict(
                title="Engagement Rate",
                overlaying="y",
                side="right",
                tickformat=".%",
            ),
            legend=dict(x=0.01, y=0.95),
            bargap=0.2
        )
        # Exibindo o gráfico (usando streamlit para exibição)
        st.plotly_chart(fig, use_container_width=True)


st.markdown("<br>", unsafe_allow_html=True)

st.markdown("""
### VÍDEOS CURTOS
""")

for mes in meses_2024:
    # Filtrando os dados para o mês de 2024
    df_mes_2024 = df_videos_curtos[(df_videos_curtos['published_at'].dt.year == 2024) & (df_videos_curtos['published_at'].dt.month == mes)]
    
    # Verificando se há dados para o mês de 2025
    df_mes_2025 = df_videos_curtos[(df_videos_curtos['published_at'].dt.year == 2025) & (df_videos_curtos['published_at'].dt.month == mes)]
    
    # Se houver dados para 2024
    if not df_mes_2024.empty:
        # Agregação dos dados de 2024
        agg_mes_2024 = df_mes_2024.groupby('channel_name').agg(
            total_views_2024=('view_count', 'sum'),
            total_likes_2024=('like_count', 'sum'),
            total_comments_2024=('comment_count', 'sum'),
            avg_engagement_rate_2024=('engagement_rate', 'mean'),
            avg_like_ratio_2024=('like_ratio', 'mean'),
            avg_comment_ratio_2024=('comment_ratio', 'mean')
        ).reset_index()

    # Se houver dados para 2025
    if not df_mes_2025.empty:
        # Agregação dos dados de 2025
        agg_mes_2025 = df_mes_2025.groupby('channel_name').agg(
            total_views_2025=('view_count', 'sum'),
            total_likes_2025=('like_count', 'sum'),
            total_comments_2025=('comment_count', 'sum'),
            avg_engagement_rate_2025=('engagement_rate', 'mean'),
            avg_like_ratio_2025=('like_ratio', 'mean'),
            avg_comment_ratio_2025=('comment_ratio', 'mean')
        ).reset_index()

        # Mesclar os dados de 2024 e 2025 pelos canais, preenchendo valores ausentes
        agg_mes_comparativo = pd.merge(agg_mes_2024, agg_mes_2025, on='channel_name', how='outer')

        # Criando o gráfico
        fig = go.Figure()

        # Gráfico de barras para número de views de 2024
        fig.add_trace(go.Bar(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['total_views_2024'],
            name=f'Views {mes} 2024',
            marker_color='lightskyblue'
        ))

        # Gráfico de barras para número de views de 2025
        fig.add_trace(go.Bar(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['total_views_2025'],
            name=f'Views {mes} 2025',
            marker_color='lightcoral'
        ))

        # Gráfico de linha para a taxa de engajamento de 2024
        fig.add_trace(go.Scatter(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['avg_engagement_rate_2024'],
            name=f'Engagement Rate {mes} 2024',
            mode='lines+markers',
            marker_color='darkorange',
            yaxis='y2'
        ))

        # Gráfico de linha para a taxa de engajamento de 2025
        fig.add_trace(go.Scatter(
            x=agg_mes_comparativo['channel_name'],
            y=agg_mes_comparativo['avg_engagement_rate_2025'],
            name=f'Engagement Rate {mes} 2025',
            mode='lines+markers',
            marker_color='darkred',
            yaxis='y2'
        ))

        # Atualizando o layout para incluir dois eixos y
        fig.update_layout(
            title=f"Número de views em vídeos longos por canal e taxa de engajamento em {pd.to_datetime(str(mes), format='%m').strftime('%B')} de 2024 e 2025",
            xaxis=dict(title="Channel Name"),
            yaxis=dict(
                title="Number of Views",
                side="left"
            ),
            yaxis2=dict(
                title="Engagement Rate",
                overlaying="y",
                side="right",
                tickformat=".%",
            ),
            legend=dict(x=0.01, y=0.95),
            bargap=0.2
        )
        # Exibindo o gráfico (usando streamlit para exibição)
        st.plotly_chart(fig, use_container_width=True)