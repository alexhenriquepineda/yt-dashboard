import os
import boto3
import streamlit as st
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker
from utils.dashboard_code.channel_id import FITNESS_CHANNELS_IDS, FINANCAS_CHANNEL_ID, PODCAST_CHANNEL_ID
from utils.dashboard_code.texts import (
     TITLE_SUGGEST_CHANNEL, INPUT_SUGGEST_CHANNEL, BTN_SEND_SUGGESTION, MSG_SUGGESTION_SUCCESS, MSG_SUGGESTION_EMPTY
)
from utils.dashboard_code.load_data import read_parquet_from_s3
from utils.dashboard_code.overview import show_overview
from utils.dashboard_code.channel_analysis import show_channel_analysis
from utils.dashboard_code.curiosity import display_additional_info
from utils.dashboard_code.video_duration import display_statiscal_analysis
from utils.dashboard_code.weekday_correlation import analyze_weekday_correlation
from utils.dashboard_code.comparative_analysis import analyze_comparative_performance
from utils.dashboard_code.title_performance import analyze_title_performance
from utils.dashboard_code.seasonality_analysis import analyze_sazonalidade



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
            self.df, self.df_videos_longos, self.df_videos_curtos = show_overview(self.df, self.df_videos_longos, self.df_videos_curtos)
            
        with tab2:
            show_channel_analysis(self.niche, self.df_videos_longos, self.df_videos_curtos)
            
        with tab3:
            display_additional_info(self.df, self.df_videos_longos)

        with tab4:
            display_statiscal_analysis(self.df, self.df_videos_longos)
            analyze_weekday_correlation(self.df)
            analyze_sazonalidade(self.df)
            analyze_comparative_performance(self.df_videos_longos, self.df_videos_curtos)
            analyze_title_performance(self.df)

        
            
        # Seção de sugestão de canais
        st.markdown("---")
        self.suggest_channel()