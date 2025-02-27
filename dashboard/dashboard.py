import os
import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

class YouTubeDashboard:
    def __init__(self):
        self.df = self.load_data()
        self.df = self.process_data(self.df)
        self.df_videos_longos = self.df[self.df['duration'] > 90]
        self.df_videos_curtos = self.df[self.df['duration'] <= 90]
        self.setup_page()
        

    def setup_page(self):
        st.set_page_config(
            page_title="YouTube Dashboard",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        alt.themes.enable("dark")
        self.show_overview()
        self.show_channel_analysis()
        self.display_additional_info()

    def load_data(self):
        path = Path(__file__).parents[1] / 'data/bronze/video/video_data.csv'
        return pd.read_csv(path, sep=";")

    def process_data(self, df):
        df['published_at'] = pd.to_datetime(df['published_at'], format='ISO8601')
        df["ano_mes_publish"] = df['published_at'].apply(lambda x: f"{x.year}-{x.month:02}")
        df['engagement_rate'] = np.where(
            df['view_count'] > 0,
            (df['like_count'] + df['comment_count']) / df['view_count'],
            np.nan
        ) * 100
        df['like_ratio'] = (df['like_count'] / df['view_count']) * 100
        df['comment_ratio'] = df['comment_count'] / df['view_count']
        return df

    def show_overview(self):
        st.title('OVERVIEW')
        self.show_video_metrics(self.df_videos_longos, 'Número de vídeos longos publicados', 'Número de visualizações de vídeos longos')
        self.show_video_metrics(self.df_videos_curtos, 'Número de vídeos curtos publicados (<= 90 segundos)', 'Número de visualizações de vídeos curtos')

    def show_video_metrics(self, df, title1, title2):
        c1, c2 = st.columns((1, 1))
        c1.header(title1)
        c2.header(title2)
        c1.metric('Total', df.shape[0])
        c2.metric('Total', f"{df['view_count'].sum():,}".replace(",", "."))
        self.plot_time_series(df, 'video_id', 'Quantidade de Vídeos', c1)
        self.plot_time_series(df, 'view_count', 'Quantidade de Visualizações', c2)

    def plot_time_series(self, df, column, ylabel, container):
        aux = (
            df[["ano_mes_publish", column]]
            .groupby("ano_mes_publish").agg(qtd=(column, "count" if column == "video_id" else "sum"))
            .reset_index()
        )
        fig = px.line(aux, x='ano_mes_publish', y='qtd', labels={'ano_mes_publish': 'Mês', 'qtd': ylabel})
        container.plotly_chart(fig, use_container_width=True)

    def show_channel_analysis(self):
        st.title('Análise Individual dos canais')
        st.markdown("""
        ### 📌 Indicadores de desempenho:
        - 📊 **Número de Views, Comentários e Likes no mês.**
        - 📈 **Taxa de engajamento => (Número de likes + número de comentários) / Total de visualizações no mês.**
        """)
        st.markdown("<br>", unsafe_allow_html=True)
        self.analyze_videos(self.df_videos_longos, "VÍDEOS LONGOS")
        self.analyze_videos(self.df_videos_curtos, "VÍDEOS CURTOS")

    def analyze_videos(self, df, title):
        st.markdown(f"### {title}")
        for mes in range(1, 13):
            df_mes_2024 = df[(df['published_at'].dt.year == 2024) & (df['published_at'].dt.month == mes)]
            df_mes_2025 = df[(df['published_at'].dt.year == 2025) & (df['published_at'].dt.month == mes)]
            if not df_mes_2024.empty or not df_mes_2025.empty:
                self.plot_comparative_analysis(df_mes_2024, df_mes_2025, mes)

    def plot_comparative_analysis(self, df_2024, df_2025, mes):
        agg_2024 = self.aggregate_data(df_2024, "2024")
        agg_2025 = self.aggregate_data(df_2025, "2025")
        agg_comparativo = pd.merge(agg_2024, agg_2025, on='channel_name', how='outer')
        fig = go.Figure()
        
        fig.add_trace(go.Bar(x=agg_comparativo['channel_name'], y=agg_comparativo['total_views_2024'], name=f'Views {mes} 2024', marker_color='lightskyblue'))
        fig.add_trace(go.Bar(x=agg_comparativo['channel_name'], y=agg_comparativo['total_views_2025'], name=f'Views {mes} 2025', marker_color='lightcoral'))
        fig.add_trace(go.Scatter(x=agg_comparativo['channel_name'], y=agg_comparativo['avg_engagement_rate_2024'], name=f'Engagement Rate {mes} 2024', mode='lines+markers', marker_color='darkorange', yaxis='y2'))
        fig.add_trace(go.Scatter(x=agg_comparativo['channel_name'], y=agg_comparativo['avg_engagement_rate_2025'], name=f'Engagement Rate {mes} 2025', mode='lines+markers', marker_color='darkred', yaxis='y2'))

        fig.update_layout(
            title=f"Número de views por canal e taxa de engajamento em {pd.to_datetime(str(mes), format='%m').strftime('%B')} de 2024 e 2025",
            xaxis=dict(title="Channel Name"),
            yaxis=dict(title="Number of Views", side="left"),
            yaxis2=dict(title="Engagement Rate", overlaying="y", side="right", tickformat=". %"),
            legend=dict(x=0.01, y=0.95),
            bargap=0.2
        )
        st.plotly_chart(fig, use_container_width=True)

    def aggregate_data(self, df, year):
        return df.groupby('channel_name').agg(
            **{f'total_views_{year}': ('view_count', 'sum'),
               f'total_likes_{year}': ('like_count', 'sum'),
               f'total_comments_{year}': ('comment_count', 'sum'),
               f'avg_engagement_rate_{year}': ('engagement_rate', 'mean')}
        ).reset_index()
    
    def display_additional_info(self):
        st.title("📊 Curiosidades")
        
        canal_mais_videos = self.df['channel_name'].value_counts().idxmax()
        total_videos = self.df['channel_name'].value_counts().max()
        
        titulo_primeiro_video = self.df.loc[self.df['published_at'].idxmin(), 'title']
        canal_primeiro_video = self.df.loc[self.df['published_at'].idxmin(), 'channel_name']
        
        # Canal com maior média de vídeos por mês
        media_videos_mes = self.df.groupby('channel_name').size() / self.df['ano_mes_publish'].nunique()
        canal_mais_videos_mes = media_videos_mes.idxmax()
        media_max_videos = media_videos_mes.max()
        
        st.markdown(f"**Canal com mais vídeos publicados:** {canal_mais_videos} ({total_videos} vídeos)")
        st.markdown(f"**Canal que postou o primeiro vídeo:** {canal_primeiro_video} - {titulo_primeiro_video}")
        st.markdown(f"**Canal com maior média de vídeos por mês:** {canal_mais_videos_mes} ({media_max_videos:.2f} vídeos/mês)")



if __name__ == "__main__":
    YouTubeDashboard()