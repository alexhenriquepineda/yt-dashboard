import streamlit as st
import plotly.express as px
from utils.dashboard_code.texts import TITLE_OVERVIEW

def show_overview(df, df_videos_longos, df_videos_curtos):
    st.title(TITLE_OVERVIEW)
    
    channel_filter = st.multiselect(
        "Filtrar por canal", 
        options=df['channel_name'].unique().tolist(),
        default=df['channel_name'].unique().tolist()
    )
    
    filtered_df_longos = df_videos_longos[df_videos_longos['channel_name'].isin(channel_filter)]
    filtered_df_curtos = df_videos_curtos[df_videos_curtos['channel_name'].isin(channel_filter)]
    
    st.header("Métricas de Vídeos Longos")
    c1, c2 = st.columns((1, 1))
    
    c1.subheader("Número de vídeos longos publicados")
    if filtered_df_longos.empty:
        c1.warning("Não há dados disponíveis para vídeos longos")
    else:
        c1.metric('Total', filtered_df_longos.shape[0])
        
        aux_longos_count = (
            filtered_df_longos[["ano_mes_publish", "video_id"]]
            .groupby("ano_mes_publish").agg(qtd=("video_id", "count"))
            .reset_index()
        )
        fig_longos_count = px.line(
            aux_longos_count, 
            x='ano_mes_publish', 
            y='qtd', 
            labels={'ano_mes_publish': 'Mês', 'qtd': 'Quantidade de Vídeos'}
        )
        c1.plotly_chart(fig_longos_count, use_container_width=True)
    
    
    c2.subheader("Número de visualizações de vídeos longos")
    if filtered_df_longos.empty:
        c2.warning("Não há dados disponíveis para visualizações de vídeos longos")
    else:
        c2.metric('Total', f"{filtered_df_longos['view_count'].sum():,}".replace(",", "."))
        
        aux_longos_views = (
            filtered_df_longos[["ano_mes_publish", "view_count"]]
            .groupby("ano_mes_publish").agg(qtd=("view_count", "sum"))
            .reset_index()
        )
        fig_longos_views = px.line(
            aux_longos_views, 
            x='ano_mes_publish', 
            y='qtd', 
            labels={'ano_mes_publish': 'Mês', 'qtd': 'Quantidade de Visualizações'}
        )
        c2.plotly_chart(fig_longos_views, use_container_width=True)
    
    
    st.header("Métricas de Vídeos Curtos")
    c3, c4 = st.columns((1, 1))
    
    
    c3.subheader("Número de vídeos curtos publicados")
    if filtered_df_curtos.empty:
        c3.warning("Não há dados disponíveis para vídeos curtos")
    else:
        c3.metric('Total', filtered_df_curtos.shape[0])
        
        aux_curtos_count = (
            filtered_df_curtos[["ano_mes_publish", "video_id"]]
            .groupby("ano_mes_publish").agg(qtd=("video_id", "count"))
            .reset_index()
        )
        fig_curtos_count = px.line(
            aux_curtos_count, 
            x='ano_mes_publish', 
            y='qtd', 
            labels={'ano_mes_publish': 'Mês', 'qtd': 'Quantidade de Vídeos'}
        )
        c3.plotly_chart(fig_curtos_count, use_container_width=True)
    
    
    c4.subheader("Número de visualizações de vídeos curtos")
    if filtered_df_curtos.empty:
        c4.warning("Não há dados disponíveis para visualizações de vídeos curtos")
    else:
        c4.metric('Total', f"{filtered_df_curtos['view_count'].sum():,}".replace(",", "."))
        # Criar série temporal para visualizações de vídeos curtos
        aux_curtos_views = (
            filtered_df_curtos[["ano_mes_publish", "view_count"]]
            .groupby("ano_mes_publish").agg(qtd=("view_count", "sum"))
            .reset_index()
        )
        fig_curtos_views = px.line(
            aux_curtos_views, 
            x='ano_mes_publish', 
            y='qtd', 
            labels={'ano_mes_publish': 'Mês', 'qtd': 'Quantidade de Visualizações'}
        )
        c4.plotly_chart(fig_curtos_views, use_container_width=True)

