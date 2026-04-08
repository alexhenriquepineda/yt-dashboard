import boto3
import streamlit as st
import pandas as pd
from dashboard.utils.dashboard_code.overview import show_overview
from dashboard.utils.dashboard_code.suggestions import suggest_channel
from dashboard.utils.dashboard_code.load_data import read_parquet_from_s3
from dashboard.utils.dashboard_code.curiosity import display_additional_info
from dashboard.utils.dashboard_code.channel_analysis import show_channel_analysis
from dashboard.utils.dashboard_code.video_duration import display_statiscal_analysis
from dashboard.utils.dashboard_code.seasonality_analysis import analyze_sazonalidade
from dashboard.utils.dashboard_code.title_performance import analyze_title_performance
from dashboard.utils.dashboard_code.weekday_correlation import analyze_weekday_correlation
from dashboard.utils.dashboard_code.comparative_analysis import analyze_comparative_performance
from dashboard.utils.dashboard_code.niche_landscape import show_niche_landscape
from dashboard.utils.dashboard_code.channel_forecast import show_channel_forecast
from config.channel_id import get_channel_ids_by_niche


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
        # Chave para o arquivo de sugestões
        self.s3_key_suggestions = "silver/suggestion/suggestions.parquet"
        
        self.df, self.df_videos_longos, self.df_videos_curtos = self._load_and_filter_data()
     
    @st.cache_data(ttl=3600)
    def _load_and_filter_data(_self):
        df = read_parquet_from_s3(_self.s3_client, _self.bucket_name, _self.s3_key)
        df = _self.filter_by_niche(df, _self.niche)
        df_videos_longos = pd.DataFrame()
        df_videos_curtos = pd.DataFrame()
        if not df.empty:
            if "duration_seconds" in df.columns:
                df["duration"] = df["duration_seconds"]
            if _self.niche == "Podcast":
                df_videos_longos = df[df['duration'] > 3600]
                df_videos_curtos = df[df['duration'] <= 3600]
            else:
                df_videos_longos = df[df['duration'] > 90]
                df_videos_curtos = df[df['duration'] <= 90]
        return df, df_videos_longos, df_videos_curtos
    
    def filter_by_niche(self, df, niche):
        """
        Filtra o DataFrame para incluir apenas canais do nicho especificado.
        
        Args:
            df (pd.DataFrame): DataFrame completo com todos os dados
            niche (str): Nome do nicho para filtrar
            
        Returns:
            pd.DataFrame: DataFrame filtrado
        """
        channel_ids = get_channel_ids_by_niche(niche)
        if not channel_ids:
            return pd.DataFrame()

        return df[df['channel_id'].isin(channel_ids)]


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
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🌐 Panorama do Nicho",
            "📊 Overview",
            "🔍 Análise por Canal",
            "💡 Curiosidades",
            "📈 Análise Estatística",
            "🤖 Previsão 12 Meses",
        ])
        
        with tab1:
            show_niche_landscape(self.niche, self.df)

        with tab2:
            self.df, self.df_videos_longos, self.df_videos_curtos = show_overview(self.df, self.df_videos_longos, self.df_videos_curtos)
            
        with tab3:
            show_channel_analysis(self.niche, self.df_videos_longos, self.df_videos_curtos)
            
        with tab4:
            display_additional_info(self.df, self.df_videos_longos)

        with tab5:
            display_statiscal_analysis(self.df, self.df_videos_longos)
            analyze_weekday_correlation(self.df)
            analyze_sazonalidade(self.df)
            analyze_comparative_performance(self.df_videos_longos, self.df_videos_curtos)
            analyze_title_performance(self.df)

        with tab6:
            show_channel_forecast(self.df)
            
        # Seção de sugestão de canais
        st.markdown("---")
        suggest_channel(self.s3_client, self.niche, self.bucket_name, self.s3_key_suggestions)
