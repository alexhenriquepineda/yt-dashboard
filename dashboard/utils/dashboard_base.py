import os
import boto3
from io import BytesIO
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from utils.channel_id import FITNESS_CHANNELS_IDS, FINANCAS_CHANNEL_ID
from scipy.stats import f_oneway
from dashboard.utils.texts import (
    PAGE_CONFIG, TITLE_OVERVIEW, TITLE_CHANNEL_ANALYSIS, DESC_CHANNEL_ANALYSIS,
    TITLE_CURIOSITIES, TITLE_WEEKDAY_CORRELATION, TITLE_SUGGEST_CHANNEL,
    METRIC_CHANNEL_VIDEOS, METRIC_CHANNEL_AVG, METRIC_FIRST_VIDEO_CHANNEL,
    METRIC_FIRST_VIDEO_DATE, TITLE_VIDEOS_ENGAGEMENT, TITLE_VIDEO_MOST_VIEWS,
    TITLE_VIDEO_MOST_LIKES, TITLE_VIDEO_MOST_COMMENTS, TITLE_VIDEO_HIGHEST_ENGAGEMENT,
    TITLE_CONTENT_FEATURES, TITLE_LONGEST_VIDEO, TITLE_LONG_VIDEO_MOST_VIEWS,
    TITLE_CURIOUS_PATTERNS, TEXT_POPULAR_DAY, TEXT_LEAST_POPULAR_DAY,
    TEXT_AVG_TITLE_LENGTH, TITLE_CORRELATION_GRAPH, TEXT_CORRELATION,
    TITLE_STATISTICAL_SUMMARY, TEXT_BEST_DAY, TEXT_WORST_DAY, TEXT_SIGNIFICANCE_ANALYSIS,
    INPUT_SUGGEST_CHANNEL, BTN_SEND_SUGGESTION, MSG_SUGGESTION_SUCCESS, MSG_SUGGESTION_EMPTY
)

Base = declarative_base()


class ChannelSugestion(Base):
    __tablename__ = 'channel_suggestion'
    id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String(50))
    dashboard = Column(String(50))


class BaseDashboard:
    """
    Classe base para todos os dashboards de nicho.
    Contém toda a lógica comum que será compartilhada entre diferentes dashboards.
    """
    def __init__(self, niche):
        """
        Inicializa o dashboard para um nicho específico.
        
        Args:
            niche (str): Nome do nicho para filtrar os dados (ex: "fitness", "gaming", "tech")
        """
        self.niche = niche
        self.s3_client = boto3.client("s3")
        self.bucket_name = "yt-dashboard-datalake"
        self.s3_key = "bronze/video/video_data.parquet"
        
        self.df = self.read_parquet_from_s3()
     
        self.df = self.filter_by_niche(self.df, niche)

        if not self.df.empty:
            self.df = self.process_data(self.df)
            self.df_videos_longos = self.df[self.df['duration'] > 90]
            self.df_videos_curtos = self.df[self.df['duration'] <= 90]
        else:
            st.error(f"Nenhum dado encontrado para o nicho '{niche}'.")
            self.df_videos_longos = pd.DataFrame()
            self.df_videos_curtos = pd.DataFrame()
    
    def filter_by_niche(self, df, niche):
        """
        Filtra o DataFrame para incluir apenas canais do nicho especificado.
        
        Args:
            df (pd.DataFrame): DataFrame completo com todos os dados
            niche (str): Nome do nicho para filtrar
            
        Returns:
            pd.DataFrame: DataFrame filtrado
        """
        if niche == "Fitness":
            return df[df['channel_id'].isin(FITNESS_CHANNELS_IDS)]
        
        if niche == "Financas":
            return df[df['channel_id'].isin(FINANCAS_CHANNEL_ID)]
        
        else:
            return pd.DataFrame()

    def read_parquet_from_s3(self) -> pd.DataFrame:
        try:
            
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            parquet_data = response['Body'].read()
            
            
            parquet_buffer = BytesIO(parquet_data)
            df = pd.read_parquet(parquet_buffer, engine='pyarrow')
            return df
        except Exception as e:
            st.error(f"Erro ao ler o arquivo Parquet do S3: {e}")
            return pd.DataFrame()

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
        st.title(TITLE_OVERVIEW)
        self.show_video_metrics(self.df_videos_longos, 'Número de vídeos longos publicados', 'Número de visualizações de vídeos longos')
        self.show_video_metrics(self.df_videos_curtos, 'Número de vídeos curtos publicados (<= 90 segundos)', 'Número de visualizações de vídeos curtos')

    def show_video_metrics(self, df, title1, title2):
        if df.empty:
            st.warning(f"Não há dados disponíveis para {title1}")
            return
            
        c1, c2 = st.columns((1, 1))
        c1.header(title1)
        c2.header(title2)
        c1.metric('Total', df.shape[0])
        c2.metric('Total', f"{df['view_count'].sum():,}".replace(",", "."))
        self.plot_time_series(df, 'video_id', 'Quantidade de Vídeos', c1)
        self.plot_time_series(df, 'view_count', 'Quantidade de Visualizações', c2)

    def plot_time_series(self, df, column, ylabel, container):
        if df.empty:
            container.warning("Sem dados para exibir")
            return
            
        aux = (
            df[["ano_mes_publish", column]]
            .groupby("ano_mes_publish").agg(qtd=(column, "count" if column == "video_id" else "sum"))
            .reset_index()
        )
        fig = px.line(aux, x='ano_mes_publish', y='qtd', labels={'ano_mes_publish': 'Mês', 'qtd': ylabel})
        container.plotly_chart(fig, use_container_width=True)

    def show_channel_analysis(self):
        st.title(f'Análise Individual dos canais - {self.niche.capitalize()}')
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
        if df.empty:
            st.warning(f"Não há dados disponíveis para {title}")
            return
            
        for mes in range(1, 13):
            df_mes_2023 = df[(df['published_at'].dt.year == 2023) & (df['published_at'].dt.month == mes)]
            df_mes_2024 = df[(df['published_at'].dt.year == 2024) & (df['published_at'].dt.month == mes)]
            df_mes_2025 = df[(df['published_at'].dt.year == 2025) & (df['published_at'].dt.month == mes)]
            if not df_mes_2023.empty or not df_mes_2024.empty or not df_mes_2025.empty:
                self.plot_comparative_analysis(df_mes_2023, df_mes_2024, df_mes_2025, mes)

    def plot_comparative_analysis(self, df_2023, df_2024, df_2025, mes):
        agg_2023 = self.aggregate_data(df_2023, "2023")
        agg_2024 = self.aggregate_data(df_2024, "2024")
        agg_2025 = self.aggregate_data(df_2025, "2025")
        agg_comparativo = pd.merge(agg_2023, agg_2024, on='channel_name', how='outer')
        agg_comparativo = pd.merge(agg_comparativo, agg_2025, on='channel_name', how='outer')

        fig = go.Figure()
        
        fig.add_trace(go.Bar(x=agg_comparativo['channel_name'], y=agg_comparativo['total_views_2023'], name=f'Views {mes} 2023', marker_color='blueviolet'))
        fig.add_trace(go.Bar(x=agg_comparativo['channel_name'], y=agg_comparativo['total_views_2024'], name=f'Views {mes} 2024', marker_color='lightskyblue'))
        fig.add_trace(go.Bar(x=agg_comparativo['channel_name'], y=agg_comparativo['total_views_2025'], name=f'Views {mes} 2025', marker_color='lightcoral'))
        fig.add_trace(go.Scatter(x=agg_comparativo['channel_name'], y=agg_comparativo['avg_engagement_rate_2023'], name=f'Engagement Rate {mes} 2023', mode='lines+markers', marker_color='darkgreen', yaxis='y2'))
        fig.add_trace(go.Scatter(x=agg_comparativo['channel_name'], y=agg_comparativo['avg_engagement_rate_2024'], name=f'Engagement Rate {mes} 2024', mode='lines+markers', marker_color='darkorange', yaxis='y2'))
        fig.add_trace(go.Scatter(x=agg_comparativo['channel_name'], y=agg_comparativo['avg_engagement_rate_2025'], name=f'Engagement Rate {mes} 2025', mode='lines+markers', marker_color='darkred', yaxis='y2'))

        fig.update_layout(
            title=f"Número de views por canal e taxa de engajamento em {pd.to_datetime(str(mes), format='%m').strftime('%B')} de 2023, 2024 e 2025",
            xaxis=dict(title="Channel Name"),
            yaxis=dict(title="Number of Views", side="left"),
            yaxis2=dict(title="Engagement Rate", overlaying="y", side="right", tickformat=". %"),
            legend=dict(x=0.01, y=0.99),
            bargap=0.2
        )
        st.plotly_chart(fig, use_container_width=True)

    def aggregate_data(self, df, year):
        if df.empty:
            # Retorna um DataFrame vazio com as colunas necessárias
            return pd.DataFrame(columns=[
                'channel_name', 
                f'total_views_{year}', 
                f'total_likes_{year}', 
                f'total_comments_{year}', 
                f'avg_engagement_rate_{year}'
            ])
            
        return df.groupby('channel_name').agg(
            **{f'total_views_{year}': ('view_count', 'sum'),
               f'total_likes_{year}': ('like_count', 'sum'),
               f'total_comments_{year}': ('comment_count', 'sum'),
               f'avg_engagement_rate_{year}': ('engagement_rate', 'mean')}
        ).reset_index()
    
    def display_additional_info(self):
        st.title(TITLE_CURIOSITIES)
        
        # Estatísticas básicas
        canal_mais_videos = self.df['channel_name'].value_counts().idxmax()
        total_videos = self.df['channel_name'].value_counts().max()
        titulo_primeiro_video = self.df.loc[self.df['published_at'].idxmin(), 'title']
        canal_primeiro_video = self.df.loc[self.df['published_at'].idxmin(), 'channel_name']
        data_primeiro_video = self.df['published_at'].min().strftime('%d/%m/%Y')
        media_videos_mes = self.df.groupby('channel_name').size() / self.df['ano_mes_publish'].nunique()
        canal_mais_videos_mes = media_videos_mes.idxmax()
        media_max_videos = media_videos_mes.max()
        more_views = self.df.loc[self.df['view_count'].idxmax(), 'title']
        channel_more_views = self.df.loc[self.df['view_count'].idxmax(), 'channel_name']
        views = self.df.loc[self.df['view_count'].idxmax(), 'view_count']
        long_video_title = self.df_videos_longos.loc[self.df_videos_longos['view_count'].idxmax(), 'title']
        long_video_channel = self.df_videos_longos.loc[self.df_videos_longos['view_count'].idxmax(), 'channel_name']
        long_video_views = self.df_videos_longos.loc[self.df_videos_longos['view_count'].idxmax(), 'view_count']
        most_likes_title = self.df.loc[self.df['like_count'].idxmax(), 'title']
        most_likes_channel = self.df.loc[self.df['like_count'].idxmax(), 'channel_name']
        most_likes_count = self.df.loc[self.df['like_count'].idxmax(), 'like_count']
        most_comments_title = self.df.loc[self.df['comment_count'].idxmax(), 'title']
        most_comments_channel = self.df.loc[self.df['comment_count'].idxmax(), 'channel_name']
        most_comments_count = self.df.loc[self.df['comment_count'].idxmax(), 'comment_count']
        longest_video_title = self.df.loc[self.df['duration'].idxmax(), 'title']
        longest_video_channel = self.df.loc[self.df['duration'].idxmax(), 'channel_name']
        longest_duration = self.df.loc[self.df['duration'].idxmax(), 'duration']
        hours, remainder = divmod(longest_duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        longest_duration_formatted = f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        highest_engagement_title = self.df.loc[self.df['engagement_rate'].idxmax(), 'title']
        highest_engagement_channel = self.df.loc[self.df['engagement_rate'].idxmax(), 'channel_name']
        highest_engagement_rate = self.df.loc[self.df['engagement_rate'].idxmax(), 'engagement_rate']
        
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

    def display_statiscal_analysis(self):
        st.subheader(TITLE_CURIOUS_PATTERNS)
        if 'published_at' in self.df.columns:
            self.df['day_of_week'] = self.df['published_at'].dt.day_name()
            day_counts = self.df['day_of_week'].value_counts()
            most_popular_day = day_counts.idxmax()
            least_popular_day = day_counts.idxmin()
            st.markdown(f"**{TEXT_POPULAR_DAY}:** {most_popular_day} ({day_counts[most_popular_day]} vídeos)")
            st.markdown(f"**{TEXT_LEAST_POPULAR_DAY}:** {least_popular_day} ({day_counts[least_popular_day]} vídeos)")
        
        avg_title_length = self.df['title'].str.len().mean()
        st.markdown(f"**{TEXT_AVG_TITLE_LENGTH}:** {avg_title_length:.1f} caracteres")
        
        if all(col in self.df_videos_longos.columns for col in ['view_count', 'duration']):
            corr = self.df_videos_longos['view_count'].corr(self.df_videos_longos['duration'])
            correlation_description = "positiva" if corr > 0.3 else "negativa" if corr < -0.3 else "pouca"
            st.markdown(f"**Correlação entre duração e visualizações:** {correlation_description} (coeficiente: {corr:.2f})")
            
            self.df_videos_longos['duration_minutes'] = self.df_videos_longos['duration'] / 60
            
            st.subheader(TITLE_CORRELATION_GRAPH)
            plot_df = self.df_videos_longos[
                (self.df_videos_longos['duration_minutes'] < self.df_videos_longos['duration_minutes'].quantile(0.99)) & 
                (self.df_videos_longos['view_count'] < self.df_videos_longos['view_count'].quantile(0.99))
            ]
            
            fig = px.scatter(
                plot_df,
                x='duration_minutes',
                y='view_count',
                hover_data=['title', 'channel_name'],
                opacity=0.7,
                labels={
                    'duration_minutes': 'Duração do Vídeo (minutos)',
                    'view_count': 'Número de Visualizações',
                    'title': 'Título',
                    'channel_name': 'Canal'
                },
                title=TEXT_CORRELATION
            )
            
            x_range = np.linspace(plot_df['duration_minutes'].min(), plot_df['duration_minutes'].max(), 100)
            y_range = np.polyval(np.polyfit(plot_df['duration_minutes'], plot_df['view_count'], 1), x_range)
            
            fig.add_trace(
                go.Scatter(
                    x=x_range,
                    y=y_range,
                    mode='lines',
                    name='Linha de Tendência',
                    line=dict(color='red', width=2)
                )
            )
            
            fig.update_layout(
                xaxis_title='Duração do Vídeo (minutos)',
                yaxis_title='Número de Visualizações',
                hovermode='closest',
                height=600,
                annotations=[
                    dict(
                        x=0.99,
                        y=0.98,
                        xref='paper',
                        yref='paper',
                        text=f'Coeficiente de Correlação: {corr:.2f}',
                        showarrow=False,
                        bgcolor='black',
                        bordercolor='black',
                        borderwidth=1,
                        borderpad=4,
                        font=dict(size=14)
                    )
                ]
            )
            
            fig.update_xaxes(gridcolor='lightgray', showgrid=True, zeroline=False)
            fig.update_yaxes(gridcolor='lightgray', showgrid=True, zeroline=False, tickformat=',')
            st.plotly_chart(fig, use_container_width=True)

    def analyze_weekday_correlation(self):
        st.title(TITLE_WEEKDAY_CORRELATION)
        
        if 'day_of_week' not in self.df.columns:
            self.df['day_of_week'] = self.df['published_at'].dt.day_name()
        
        dias_semana_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dias_semana_pt = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        
        self.df['day_number'] = self.df['day_of_week'].map({day: i for i, day in enumerate(dias_semana_ordem)})
        
        weekday_stats = self.df.groupby('day_of_week').agg(
            media_views=('view_count', 'mean'),
            total_views=('view_count', 'sum'),
            count_videos=('video_id', 'count'),
            media_engagement=('engagement_rate', 'mean')
        ).reset_index()
        
        weekday_stats['day_number'] = weekday_stats['day_of_week'].map({day: i for i, day in enumerate(dias_semana_ordem)})
        weekday_stats = weekday_stats.sort_values('day_number')
        weekday_stats['day_of_week_pt'] = weekday_stats['day_of_week'].map(dict(zip(dias_semana_ordem, dias_semana_pt)))
        
        st.subheader("Média de visualizações por dia da semana")
        fig1 = px.bar(
            weekday_stats, 
            x='day_of_week_pt', 
            y='media_views',
            title='Média de visualizações por dia da semana',
            labels={'day_of_week_pt': 'Dia da Semana', 'media_views': 'Média de Visualizações'},
            text_auto='.0f'
        )
        fig1.update_traces(marker_color='lightskyblue', textposition='outside')
        fig1.update_layout(xaxis_title='Dia da Semana', yaxis_title='Média de Visualizações')
        st.plotly_chart(fig1, use_container_width=True)
        
        st.subheader("Quantidade de vídeos publicados por dia da semana")
        fig2 = px.bar(
            weekday_stats, 
            x='day_of_week_pt', 
            y='count_videos',
            title='Quantidade de vídeos publicados por dia da semana',
            labels={'day_of_week_pt': 'Dia da Semana', 'count_videos': 'Quantidade de Vídeos'},
            text_auto='.0f'
        )
        fig2.update_traces(marker_color='lightgreen', textposition='outside')
        st.plotly_chart(fig2, use_container_width=True)
        
        st.subheader("Comparação: Visualizações vs Engajamento por dia da semana")
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=weekday_stats['day_of_week_pt'],
            y=weekday_stats['media_views'],
            name='Média de Visualizações',
            marker_color='royalblue'
        ))
        fig3.add_trace(go.Scatter(
            x=weekday_stats['day_of_week_pt'],
            y=weekday_stats['media_engagement'],
            name='Taxa de Engajamento (%)',
            marker_color='firebrick',
            mode='lines+markers',
            yaxis='y2'
        ))
        fig3.update_layout(
            title='Visualizações vs Engajamento por Dia da Semana',
            xaxis=dict(title='Dia da Semana'),
            yaxis=dict(title='Média de Visualizações', side='left'),
            yaxis2=dict(
                title='Taxa de Engajamento (%)',
                overlaying='y',
                side='right',
                tickformat='.2f'
            ),
            legend=dict(x=0.01, y=0.99)
        )
        st.plotly_chart(fig3, use_container_width=True)
        
        st.subheader(TITLE_STATISTICAL_SUMMARY)
        best_day = weekday_stats.loc[weekday_stats['media_views'].idxmax()]
        worst_day = weekday_stats.loc[weekday_stats['media_views'].idxmin()]
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric(TEXT_BEST_DAY, f"{best_day['day_of_week_pt']}", f"{best_day['media_views']:.0f} views")
        with col2:
            st.metric(TEXT_WORST_DAY, f"{worst_day['day_of_week_pt']}", f"{worst_day['media_views']:.0f} views")
        
        diff_pct = (best_day['media_views'] - worst_day['media_views']) / worst_day['media_views'] * 100
        
        st.markdown(f"""
        ### {TEXT_SIGNIFICANCE_ANALYSIS}
        
        - **Diferença percentual** entre o melhor e o pior dia: **{diff_pct:.2f}%**
        - Isso indica que vídeos publicados na **{best_day['day_of_week_pt']}** tendem a ter em média **{diff_pct:.2f}%** mais visualizações do que os publicados na **{worst_day['day_of_week_pt']}**.
        """)
 
        st.info(f"""
        **Recomendação**: Com base nos dados históricos, **{best_day['day_of_week_pt']}** parece ser o melhor dia para publicar novos vídeos para maximizar visualizações.
        Entretanto, é importante considerar outros fatores como o engajamento do público-alvo em diferentes horários e dias da semana.
        """)
        
        try:
            groups = [self.df[self.df['day_of_week'] == day]['view_count'].values for day in dias_semana_ordem if day in self.df['day_of_week'].unique()]
            if len(groups) > 1 and all(len(g) > 0 for g in groups):
                f_stat, p_value = f_oneway(*groups)
                
                st.subheader("Análise Estatística (ANOVA)")
                st.markdown(f"""
                    - **Estatística F:** {f_stat:.4f}
                    - **Valor p:** {p_value:.10f}
                            
            {'**Resultado**: Há uma diferença estatisticamente significativa nas visualizações entre dias da semana (p < 0.05).' 
            if p_value < 0.05 else 
            '**Resultado**: Não há evidência estatística suficiente para afirmar que o dia da semana afeta significativamente as visualizações (p > 0.05).'}
                            """)
        except ImportError:
            st.warning("Análise estatística avançada não disponível. Instale scipy para habilitar esta funcionalidade.")
        except Exception as e:
            st.warning(f"Não foi possível realizar a análise estatística: {e}")


    def suggest_channel(self):
        st.title(TITLE_SUGGEST_CHANNEL)
        channel_suggestion = st.text_input(INPUT_SUGGEST_CHANNEL)
        if st.button(BTN_SEND_SUGGESTION):
            if channel_suggestion:
                self.save_suggestion_to_db(channel_suggestion, dashboard="fitness")
                st.success(MSG_SUGGESTION_SUCCESS.format(channel_suggestion))
            else:
                st.warning(MSG_SUGGESTION_EMPTY)
    
    def save_suggestion_to_db(self, channel_sugestion, dashboard):
        load_dotenv()
        DB_HOST = os.getenv("DB_HOST")
        DB_DATABASE = os.getenv("DB_DATABASE")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}"
        
        def save_data_postgres(session, dados):
            try:
                novo_dado = ChannelSugestion(
                    channel_name=dados["channel_name"],
                    dashboard=dados["dashboard"],
                )
                session.add(novo_dado)
                session.commit()
            except SQLAlchemyError as e:
                st.error(f"Erro ao salvar os dados no banco de dados: {e}")
                session.rollback()
        try:
            engine = create_engine(DATABASE_URL)
            Session = sessionmaker(bind=engine)
            session = Session()
            suggestion = {
                "channel_name": channel_sugestion, "dashboard": dashboard
            }
            save_data_postgres(session, suggestion)
            session.close()
        except Exception as e:
            st.error(f"Erro ao salvar a sugestão no banco de dados: {e}")

    def run_dashboard(self):
        """Executa o dashboard completo"""
        st.title(f"Dashboard de {self.niche.capitalize()}")
        
        # Adiciona informações na barra lateral
        st.sidebar.title(f"Sobre o Dashboard de {self.niche.capitalize()}")
        st.sidebar.info(f"""
        Este dashboard analisa canais do YouTube no nicho de {self.niche.capitalize()}.
        
        Os dados são atualizados regularmente e incluem métricas de desempenho e engajamento.
        
        Use as seções abaixo para explorar os dados.
        """)
        
        # Configuração das tabs principais
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔍 Análise por Canal", "💡 Curiosidades", "📈 Análise Estatística"])
        
        with tab1:
            self.show_overview()
            
        with tab2:
            self.show_channel_analysis()
            
        with tab3:
            self.display_additional_info()

        with tab4:
            self.display_statiscal_analysis()
            self.analyze_weekday_correlation()

        
            
        # Seção de sugestão de canais
        st.markdown("---")
        self.suggest_channel()