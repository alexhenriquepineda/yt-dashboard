import os
import re
import boto3
from collections import Counter
from io import BytesIO
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from utils.dashboard_code.channel_id import FITNESS_CHANNELS_IDS, FINANCAS_CHANNEL_ID, PODCAST_CHANNEL_ID
from scipy.stats import f_oneway, pearsonr, spearmanr, ttest_ind
from utils.dashboard_code.texts import (
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
from utils.dashboard_code.load_data import read_parquet_from_s3
from utils.dashboard_code.overview import show_overview
from utils.dashboard_code.channel_analysis import show_channel_analysis
from utils.dashboard_code.curiosity import display_additional_info

from wordcloud import WordCloud
from matplotlib import pyplot as plt

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
        self.s3_key = "silver/video/video_data.parquet"
        
        self.df = read_parquet_from_s3(self.s3_client, self.bucket_name, self.s3_key)
     
        self.df = self.filter_by_niche(self.df, niche)
        if not self.df.empty:
            if niche == "Podcast":
                self.df_videos_longos = self.df[self.df['duration'] > 3600]
                self.df_videos_curtos = self.df[self.df['duration'] <= 3600]
            else:
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
        if niche == "Podcast":
            return df[df['channel_id'].isin(PODCAST_CHANNEL_ID)]
        else:
            return pd.DataFrame()


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


    def analyze_sazonalidade(self):
        """Análise de Sazonalidade Completa"""
        st.title("📅 Análise de Sazonalidade")
        
        # Criando um dataframe de análise mensal
        monthly_data = self.df.groupby(['ano', 'mes']).agg(
            num_videos=('video_id', 'count'),
            media_views=('view_count', 'mean'),
            total_views=('view_count', 'sum'),
            media_engagement=('engagement_rate', 'mean'),
            media_likes=('like_count', 'mean')
        ).reset_index()
        
        # Criando coluna de data para visualização temporal
        monthly_data['date'] = pd.to_datetime(monthly_data['ano'].astype(str) + '-' + monthly_data['mes'].astype(str) + '-01')
        monthly_data = monthly_data.sort_values('date')
        
        # Seletor de métricas
        st.subheader("Tendência Temporal de Métricas")
        metric_options = ["num_videos", "media_views", "total_views", "media_engagement", "media_likes"]
        metric_names = ["Número de Vídeos", "Média de Visualizações", "Total de Visualizações", 
                        "Taxa de Engajamento Média (%)", "Média de Likes"]
        
        metric_dict = dict(zip(metric_options, metric_names))
        selected_metrics = st.multiselect(
            "Selecione as métricas para visualizar a tendência temporal:",
            options=metric_options,
            default=["total_views", "num_videos"],
            format_func=lambda x: metric_dict[x]
        )
        
        if selected_metrics:
            # Criando subplots - um para cada métrica
            fig = make_subplots(rows=len(selected_metrics), cols=1, 
                            shared_xaxes=True,
                            vertical_spacing=0.05,
                            subplot_titles=[metric_dict[m] for m in selected_metrics])
            
            # Adicionando uma linha para cada métrica em seu próprio subplot
            for i, metric in enumerate(selected_metrics, 1):
                fig.add_trace(
                    go.Scatter(
                        x=monthly_data['date'], 
                        y=monthly_data[metric],
                        mode='lines+markers',
                        name=metric_dict[metric],
                        line=dict(width=2)
                    ),
                    row=i, col=1
                )
                
                # Adicionando linha de tendência (opcional)
                try:
                    z = np.polyfit(range(len(monthly_data)), monthly_data[metric], 1)
                    p = np.poly1d(z)
                    fig.add_trace(
                        go.Scatter(
                            x=monthly_data['date'],
                            y=p(range(len(monthly_data))),
                            mode='lines',
                            name=f'Tendência {metric_dict[metric]}',
                            line=dict(dash='dash', width=1)
                        ),
                        row=i, col=1
                    )
                except:
                    pass  # Ignora se não conseguir calcular a tendência
            
            # Ajustando layout
            height_per_plot = 250  # altura para cada subplot
            fig.update_layout(
                height=height_per_plot * len(selected_metrics) + 100,  # altura total
                title="Tendência Temporal por Métrica",
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="x unified"
            )
            
            # Configurando eixos y
            for i in range(1, len(selected_metrics) + 1):
                fig.update_yaxes(title_text="Valor", row=i, col=1)
            
            # Configurando o eixo x apenas para o último subplot
            fig.update_xaxes(title_text="Data", row=len(selected_metrics), col=1)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Adicionar explicação
            st.info("📊 **Visualização separada**: Cada métrica é exibida em seu próprio gráfico com escala apropriada, facilitando a identificação de padrões sazonais.")
                

    def analyze_comparative_performance(self):
        """Análise Comparativa entre Videos Longos e Curtos"""
        st.title("🔄 Análise Comparativa: Vídeos Longos x Shorts")
        
        # Estatísticas básicas para cada tipo
        long_stats = {
            'count': len(self.df_videos_longos),
            'avg_views': self.df_videos_longos['view_count'].mean(),
            'avg_likes': self.df_videos_longos['like_count'].mean(),
            'avg_comments': self.df_videos_longos['comment_count'].mean(),
            'avg_engagement': self.df_videos_longos['engagement_rate'].mean(),
            'max_views': self.df_videos_longos['view_count'].max(),
            'max_engagement': self.df_videos_longos['engagement_rate'].max(),
        }
        
        short_stats = {
            'count': len(self.df_videos_curtos),
            'avg_views': self.df_videos_curtos['view_count'].mean(),
            'avg_likes': self.df_videos_curtos['like_count'].mean(),
            'avg_comments': self.df_videos_curtos['comment_count'].mean(),
            'avg_engagement': self.df_videos_curtos['engagement_rate'].mean(),
            'max_views': self.df_videos_curtos['view_count'].max(),
            'max_engagement': self.df_videos_curtos['engagement_rate'].max(),
        }
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Vídeos Longos")
            st.metric("Quantidade", f"{long_stats['count']} vídeos")
            st.metric("Média de Visualizações", f"{long_stats['avg_views']:,.0f}")
            st.metric("Média de Likes", f"{long_stats['avg_likes']:,.0f}")
            st.metric("Taxa de Engajamento Média", f"{long_stats['avg_engagement']:.2f}%")
        
        with col2:
            st.subheader("Shorts")
            st.metric("Quantidade", f"{short_stats['count']} vídeos")
            st.metric("Média de Visualizações", f"{short_stats['avg_views']:,.0f}")
            st.metric("Média de Likes", f"{short_stats['avg_likes']:,.0f}")
            st.metric("Taxa de Engajamento Média", f"{short_stats['avg_engagement']:.2f}%")
        
        st.subheader("Comparação de Métricas")
        
        metrics_to_compare = [
            'avg_views', 'avg_likes', 'avg_comments', 'avg_engagement'
        ]
        
        metric_labels = {
            'avg_views': 'Média de Visualizações',
            'avg_likes': 'Média de Likes',
            'avg_comments': 'Média de Comentários', 
            'avg_engagement': 'Taxa de Engajamento (%)'
        }
        
        selected_metrics = st.multiselect(
            "Selecione as métricas para comparar:",
            options=metrics_to_compare,
            default=metrics_to_compare,
            format_func=lambda x: metric_labels[x]
        )
        
        if selected_metrics:
            comparison_data = []
            
            for metric in selected_metrics:
                if metric == 'avg_engagement':
                    comparison_data.append({
                        'Métrica': metric_labels[metric],
                        'Vídeos Longos': long_stats[metric],
                        'Shorts': short_stats[metric]
                    })
                else:
                    long_value = long_stats[metric]
                    short_value = short_stats[metric]
                    
                    comparison_data.append({
                        'Métrica': metric_labels[metric],
                        'Vídeos Longos': long_value,
                        'Shorts': short_value
                    })
            
            comparison_df = pd.DataFrame(comparison_data)
            
            fig = px.bar(
                comparison_df, 
                x='Métrica', 
                y=['Vídeos Longos', 'Shorts'],
                barmode='group',
                title="Comparação de Métricas entre Vídeos Longos e Shorts",
                labels={'value': 'Valor', 'variable': 'Tipo de Vídeo'},
                text_auto='.1f'
            )
            
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Teste estatístico para comparar os grupos
            st.subheader("Análise Estatística Comparativa")
            
            metric_for_test = st.selectbox(
                "Selecione uma métrica para teste estatístico:",
                options=metrics_to_compare,
                format_func=lambda x: metric_labels[x]
            )
            
            # Executando teste t para duas amostras independentes
            try:
                if metric_for_test == 'avg_views':
                    stat, p_value = ttest_ind(
                        self.df_videos_longos['view_count'].dropna(),
                        self.df_videos_curtos['view_count'].dropna(),
                        equal_var=False  # Welch's t-test não assume variâncias iguais
                    )
                    metric_label = 'visualizações'
                elif metric_for_test == 'avg_likes':
                    stat, p_value = ttest_ind(
                        self.df_videos_longos['like_count'].dropna(),
                        self.df_videos_curtos['like_count'].dropna(), 
                        equal_var=False
                    )
                    metric_label = 'likes'
                elif metric_for_test == 'avg_comments':
                    stat, p_value = ttest_ind(
                        self.df_videos_longos['comment_count'].dropna(),
                        self.df_videos_curtos['comment_count'].dropna(),
                        equal_var=False
                    )
                    metric_label = 'comentários'
                else:  # 'avg_engagement'
                    stat, p_value = ttest_ind(
                        self.df_videos_longos['engagement_rate'].dropna(),
                        self.df_videos_curtos['engagement_rate'].dropna(),
                        equal_var=False
                    )
                    metric_label = 'taxa de engajamento'
                
                st.markdown(f"""
                #### Resultado do Teste-t para {metric_labels[metric_for_test]}
                - **Estatística t**: {stat:.4f}
                - **Valor p**: {p_value:.10f}
                
                {'**Conclusão**: Há uma diferença estatisticamente significativa na ' + metric_label + ' entre vídeos longos e shorts (p < 0.05).' 
                if p_value < 0.05 else 
                '**Conclusão**: Não há evidência estatística suficiente para afirmar que existe diferença na ' + metric_label + ' entre vídeos longos e shorts (p > 0.05).'}
                """)
            except Exception as e:
                st.warning(f"Não foi possível realizar o teste estatístico: {e}")
            


            # Abordagem 3: Box plots para comparar as distribuições
            st.subheader("Comparação de Visualizações (Box Plot)")

            fig3 = go.Figure()

            fig3.add_trace(go.Box(
                y=self.df_videos_longos['view_count'],
                name='Vídeos Longos',
                boxpoints='outliers',
                marker_color='blue',
                line_color='blue'
            ))

            fig3.add_trace(go.Box(
                y=self.df_videos_curtos['view_count'],
                name='Shorts',
                boxpoints='outliers',
                marker_color='red',
                line_color='red'
            ))

            fig3.update_layout(
                title="Comparação de Visualizações por Tipo de Vídeo",
                yaxis_title="Visualizações",
                height=500
            )

            # Usando escala logarítmica no eixo Y para melhor visualização
            fig3.update_yaxes(type="log")

            st.plotly_chart(fig3, use_container_width=True)

            # Abordagem 4: Gráfico de densidade (KDE) para visualizar a distribuição
            st.subheader("Densidade de Visualizações")

            # Preparando os dados para KDE - usando log para melhor visualização
            long_views_log = np.log10(self.df_videos_longos['view_count'].replace(0, 1))
            short_views_log = np.log10(self.df_videos_curtos['view_count'].replace(0, 1))

            fig4 = go.Figure()

            # KDE para vídeos longos
            fig4.add_trace(go.Violin(
                y=long_views_log,
                name='Vídeos Longos',
                box_visible=True,
                line_color='blue',
                fillcolor='blue',
                opacity=0.6
            ))

            # KDE para shorts
            fig4.add_trace(go.Violin(
                y=short_views_log,
                name='Shorts',
                box_visible=True,
                line_color='red',
                fillcolor='red',
                opacity=0.6
            ))

            fig4.update_layout(
                title="Densidade de Visualizações (escala logarítmica)",
                yaxis_title="Log10(Visualizações)",
                height=500
            )

            st.plotly_chart(fig4, use_container_width=True)

            
            # Conclusões gerais
            st.subheader("Conclusões da Análise Comparativa")
            
            # Calculando razões para insights
            view_ratio = short_stats['avg_views'] / long_stats['avg_views']
            engagement_ratio = short_stats['avg_engagement'] / long_stats['avg_engagement']
            
            st.markdown(f"""
            ### Principais insights:
            
            - Shorts recebem em média **{view_ratio:.2f}x** mais visualizações que vídeos longos
            - A taxa de engajamento dos shorts é **{engagement_ratio:.2f}x** a dos vídeos longos
            
            **Recomendações:**
            
            {
            "- Considerando que os shorts têm desempenho significativamente melhor em termos de visualizações e engajamento, seria recomendável aumentar a produção deste formato" 
            if view_ratio > 1.5 and engagement_ratio > 1.2 else
            "- Apesar dos shorts terem um desempenho diferente, os vídeos longos continuam sendo relevantes para a estratégia do canal"
            }
            
            - A estratégia ideal seria uma combinação de ambos os formatos, com shorts servindo como porta de entrada para atrair novos espectadores e vídeos longos para aprofundar o relacionamento
            """)

    def analyze_title_performance(self):
        """Análise do impacto do título no desempenho dos vídeos"""
        st.title("🔤 Análise de Títulos e Desempenho")
        
        # Análise de comprimento do título
        st.subheader("Relação entre Comprimento do Título e Desempenho")
        
        # Criar coluna de comprimento do título
        self.df['title_length'] = self.df['title'].str.len()
        
        # Agrupando por comprimento de título
        title_length_bins = [0, 20, 30, 40, 50, 60, 70, 100]
        title_length_labels = ['< 20', '20-30', '30-40', '40-50', '50-60', '60-70', '> 70']
        
        self.df['title_length_cat'] = pd.cut(
            self.df['title_length'], 
            bins=title_length_bins, 
            labels=title_length_labels
        )
        
        title_length_perf = self.df.groupby('title_length_cat').agg(
            avg_views=('view_count', 'mean'),
            avg_likes=('like_count', 'mean'),
            avg_engagement=('engagement_rate', 'mean'),
            count=('video_id', 'count')
        ).reset_index()
        
        # Gráfico de desempenho por comprimento de título
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=("Visualizações por Comprimento do Título", "Engajamento por Comprimento do Título"),
            specs=[[{"type": "bar"}, {"type": "bar"}]]
        )
        
        fig.add_trace(
            go.Bar(
                x=title_length_perf['title_length_cat'], 
                y=title_length_perf['avg_views'],
                name="Média de Visualizações",
                marker_color='royalblue',
                text=title_length_perf['avg_views'].round(0),
                textposition='outside'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                x=title_length_perf['title_length_cat'], 
                y=title_length_perf['avg_engagement'],
                name="Taxa de Engajamento (%)",
                marker_color='firebrick',
                text=title_length_perf['avg_engagement'].round(2),
                textposition='outside'
            ),
            row=1, col=2
        )
        
        fig.update_layout(
            title_text="Desempenho por Comprimento do Título",
            height=500,
            showlegend=False
        )
        
        fig.update_xaxes(title_text="Caracteres no Título", row=1, col=1)
        fig.update_xaxes(title_text="Caracteres no Título", row=1, col=2)
        fig.update_yaxes(title_text="Média de Visualizações", row=1, col=1)
        fig.update_yaxes(title_text="Taxa de Engajamento (%)", row=1, col=2)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Identificando o comprimento de título ideal
        best_views_length = title_length_perf.loc[title_length_perf['avg_views'].idxmax(), 'title_length_cat']
        best_engagement_length = title_length_perf.loc[title_length_perf['avg_engagement'].idxmax(), 'title_length_cat']
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Melhor comprimento para visualizações", f"{best_views_length} caracteres")
        with col2:
            st.metric("Melhor comprimento para engajamento", f"{best_engagement_length} caracteres")
        
        # Análise de correlação
        corr_title_views, p_value_views = pearsonr(self.df['title_length'], self.df['view_count'])
        corr_title_engagement, p_value_engagement = pearsonr(self.df['title_length'], self.df['engagement_rate'])
        
        st.markdown(f"""
        ### Correlação entre Comprimento do Título e Desempenho
        
        - **Correlação com visualizações**: {corr_title_views:.3f} (p-value: {p_value_views:.3f})
        - **Correlação com engajamento**: {corr_title_engagement:.3f} (p-value: {p_value_engagement:.3f})
        
        {'O comprimento do título tem uma correlação estatisticamente significativa com as visualizações.' if p_value_views < 0.05 else 'Não há correlação estatisticamente significativa entre o comprimento do título e as visualizações.'}
        
        {'O comprimento do título tem uma correlação estatisticamente significativa com o engajamento.' if p_value_engagement < 0.05 else 'Não há correlação estatisticamente significativa entre o comprimento do título e o engajamento.'}
        """)
        
        # Análise de palavras comuns em títulos populares
        st.subheader("Análise de Palavras-Chave em Títulos Populares")
        
        # Definindo o que são vídeos populares (superior a 75% em visualizações)
        view_threshold = self.df['view_count'].quantile(0.75)
        popular_videos = self.df[self.df['view_count'] >= view_threshold]
        
        # Preparando texto para nuvem de palavras
        try:
            # Se a biblioteca WordCloud estiver disponível
            all_titles = " ".join(popular_videos['title'])
            
            # Lista de stopwords em português
            stopwords = ["a", "o", "e", "de", "da", "do", "em", "para", "com", "um", "uma", "os", "as", "que", "como", "-", "por", "seu", "sua", "seus", "suas", "na", "no", "nos", "nas", "ao", "aos", "à", "às", "pelo", "pela", "pelos", "pelas", "num", "numa", "nuns", "numas", "dum", "duma", "duns", "dumas"]
            
            # Criando a nuvem de palavras
            wordcloud = WordCloud(
                width=800, 
                height=400, 
                background_color='white',
                stopwords=stopwords,
                min_font_size=10,
                max_font_size=100,
                colormap='viridis'
            ).generate(all_titles)
            
            # Exibindo a nuvem de palavras
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.tight_layout(pad=0)
            
            st.pyplot(plt)
            

            
            # Função para extrair palavras das strings
            def extract_words(text):
                return re.findall(r'\b[a-zA-ZÀ-ÿ]{3,}\b', text.lower())
            
            # Extrair todas as palavras dos títulos
            all_words = []
            for title in popular_videos['title']:
                words = extract_words(title)
                all_words.extend([w for w in words if w not in stopwords])
            
            # Contando as palavras mais frequentes
            word_counts = Counter(all_words)
            top_words = word_counts.most_common(100)
            
            # Criando dataframe para visualização
            top_words_df = pd.DataFrame(top_words, columns=['Palavra', 'Frequência'])
            
            # Gráfico de barras para as palavras mais comuns
            fig = px.bar(
                top_words_df, 
                x='Palavra', 
                y='Frequência',
                title="100 Palavras Mais Comuns em Vídeos Populares",
                text_auto=True
            )
            
            fig.update_traces(marker_color='teal', textposition='outside')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Análise de impacto das palavras-chave
            st.subheader("Impacto das Palavras-Chave no Desempenho")
            
            # Selecionar uma palavra-chave para análise
            palavra_chave = st.selectbox(
                "Selecione uma palavra-chave para analisar seu impacto:",
                options=[word for word, _ in top_words]
            )
            
            if palavra_chave:
                # Identificar vídeos com a palavra-chave no título
                self.df['contains_keyword'] = self.df['title'].str.lower().str.contains(palavra_chave.lower())
                
                # Calcular estatísticas para vídeos com e sem a palavra-chave
                with_keyword = self.df[self.df['contains_keyword']]
                without_keyword = self.df[~self.df['contains_keyword']]
                
                keyword_stats = pd.DataFrame([
                    {
                        'Grupo': f'Com "{palavra_chave}"',
                        'Número de Vídeos': len(with_keyword),
                        'Média de Visualizações': with_keyword['view_count'].mean(),
                        'Média de Likes': with_keyword['like_count'].mean(),
                        'Taxa de Engajamento (%)': with_keyword['engagement_rate'].mean()
                    },
                    {
                        'Grupo': f'Sem "{palavra_chave}"',
                        'Número de Vídeos': len(without_keyword),
                        'Média de Visualizações': without_keyword['view_count'].mean(),
                        'Média de Likes': without_keyword['like_count'].mean(),
                        'Taxa de Engajamento (%)': without_keyword['engagement_rate'].mean()
                    }
                ])
                
                # Visualizar comparação
                st.dataframe(keyword_stats.round(2))
                
                try:
                    stat, p_value = ttest_ind(
                        with_keyword['view_count'].dropna(),
                        without_keyword['view_count'].dropna(),
                        equal_var=False
                    )
                    
                    # Interpretação do teste estatístico
                    significance = "estatisticamente significativa" if p_value < 0.05 else "não estatisticamente significativa"
                    
                    st.markdown(f"""
                    ### Análise Estatística
                    
                    A diferença na média de visualizações entre vídeos com e sem a palavra "{palavra_chave}" é {significance} (p-value: {p_value:.10f}).
                    
                    {'Isso sugere que esta palavra-chave tem um impacto real no desempenho dos vídeos.' if p_value < 0.05 else 'Isso sugere que esta palavra-chave não tem um impacto estatisticamente comprovado no desempenho dos vídeos.'}
                    """)
                    
                    # Visualizar comparação em gráfico
                    fig = px.bar(
                        keyword_stats, 
                        x='Grupo', 
                        y=['Média de Visualizações', 'Média de Likes', 'Taxa de Engajamento (%)'],
                        barmode='group',
                        title=f'Impacto da Palavra "{palavra_chave}" no Desempenho',
                        text_auto=True
                    )
                    
                    fig.update_layout(height=500)
                    st.plotly_chart(fig, use_container_width=True)
                    
                except Exception as e:
                    st.warning(f"Não foi possível realizar o teste estatístico: {e}")
        
        except ImportError:
            st.warning("A biblioteca WordCloud não está disponível. Instale-a para visualizar a nuvem de palavras.")
            # Ainda podemos mostrar as palavras mais comuns sem a nuvem
            
        # Análise de emojis no título
        st.subheader("Impacto de Emojis nos Títulos")
        
        # Função para detectar emojis
        def contains_emoji(text):
            emoji_pattern = re.compile(
                "["
                "\U0001F600-\U0001F64F"  # emoticons
                "\U0001F300-\U0001F5FF"  # symbols & pictographs
                "\U0001F680-\U0001F6FF"  # transport & map symbols
                "\U0001F700-\U0001F77F"  # alchemical symbols
                "\U0001F780-\U0001F7FF"  # Geometric Shapes
                "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                "\U0001FA00-\U0001FA6F"  # Chess Symbols
                "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                "\U00002702-\U000027B0"  # Dingbats
                "\U000024C2-\U0001F251" 
                "]+"
            )
            return bool(emoji_pattern.search(text))
        
        # Identificar vídeos com emoji no título
        self.df['has_emoji'] = self.df['title'].apply(contains_emoji)
        
        # Comparar desempenho
        emoji_stats = self.df.groupby('has_emoji').agg(
            avg_views=('view_count', 'mean'),
            avg_likes=('like_count', 'mean'),
            avg_engagement=('engagement_rate', 'mean'),
            count=('video_id', 'count')
        ).reset_index()
        
        emoji_stats['has_emoji'] = emoji_stats['has_emoji'].map({True: 'Com Emoji', False: 'Sem Emoji'})
        
        # Criar gráfico de comparação
        fig = px.bar(
            emoji_stats, 
            x='has_emoji', 
            y=['avg_views', 'avg_likes', 'avg_engagement'],
            barmode='group',
            labels={
                'has_emoji': 'Presença de Emoji',
                'avg_views': 'Média de Visualizações',
                'avg_likes': 'Média de Likes',
                'avg_engagement': 'Taxa de Engajamento (%)'
            },
            title='Impacto de Emojis no Desempenho dos Vídeos',
            text_auto=True
        )
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Teste estatístico para a diferença
        with_emoji = self.df[self.df['has_emoji']]
        without_emoji = self.df[~self.df['has_emoji']]
        
        try:
            stat, p_value = ttest_ind(
                with_emoji['view_count'].dropna(),
                without_emoji['view_count'].dropna(),
                equal_var=False
            )
            
            # Interpretação do teste
            significance = "estatisticamente significativa" if p_value < 0.05 else "não estatisticamente significativa"
            
            st.markdown(f"""
            ### Análise Estatística - Emojis
            
            A diferença na média de visualizações entre vídeos com e sem emojis é {significance} (p-value: {p_value:.3f}).
            
            {'Isso sugere que emojis têm um impacto real no desempenho dos vídeos.' if p_value < 0.05 else 'Isso sugere que emojis não têm um impacto estatisticamente comprovado no desempenho dos vídeos.'}
            """)
            
            # Proporção de vídeos com emoji
            total_videos = len(self.df)
            with_emoji_count = len(with_emoji)
            emoji_percentage = (with_emoji_count / total_videos) * 100
            
            st.metric("Porcentagem de vídeos com emoji", f"{emoji_percentage:.1f}%")
            
        except Exception as e:
            st.warning(f"Não foi possível realizar o teste estatístico: {e}")
        
        # Recomendações para títulos
        st.subheader("Recomendações para Otimização de Títulos")
        
        # Compilando as descobertas
        recommendations = []
        
        # Recomendação de comprimento
        recommendations.append(f"**Comprimento do título:** Títulos entre {best_views_length} caracteres tendem a ter melhor desempenho em visualizações.")
        
        # Recomendação sobre palavras-chave
        if 'palavra_chave' in locals() and 'p_value' in locals() and p_value < 0.05:
            recommendations.append(f"**Palavras-chave:** Considere incluir a palavra '{palavra_chave}' em seus títulos, pois ela está associada a um melhor desempenho.")
        
        # Recomendação sobre emojis
        if 'p_value' in locals() and p_value < 0.05:
            emoji_recommendation = "Considere incluir emojis nos seus títulos, pois eles estão associados a um melhor desempenho."
        else:
            emoji_recommendation = "O uso de emojis não mostrou um impacto significativo no desempenho dos vídeos."
        
        recommendations.append(f"**Emojis:** {emoji_recommendation}")
        
        # Exibir recomendações
        for i, rec in enumerate(recommendations, 1):
            st.markdown(f"{i}. {rec}")
        




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
            show_overview(self.df, self.df_videos_longos, self.df_videos_curtos)
            
        with tab2:
            show_channel_analysis(self.niche, self.df_videos_longos, self.df_videos_curtos)
            
        with tab3:
            display_additional_info(self.df, self.df_videos_longos)

        with tab4:
            self.display_statiscal_analysis()
            self.analyze_weekday_correlation()
            self.analyze_sazonalidade()
            self.analyze_comparative_performance()
            self.analyze_title_performance()

        
            
        # Seção de sugestão de canais
        st.markdown("---")
        self.suggest_channel()