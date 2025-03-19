import pandas as pd
import streamlit as st
import plotly.graph_objects as go


def aggregate_data(df, year):
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

def plot_comparative_analysis(df_2023, df_2024, df_2025, mes):
    agg_2023 = aggregate_data(df_2023, "2023")
    agg_2024 = aggregate_data(df_2024, "2024")
    agg_2025 = aggregate_data(df_2025, "2025")
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


def analyze_videos(df, title):
    st.markdown(f"### {title}")
    if df.empty:
        st.warning(f"Não há dados disponíveis para {title}")
        return
        
    for mes in range(1, 13):
        df_mes_2023 = df[(df['published_at'].dt.year == 2023) & (df['published_at'].dt.month == mes)]
        df_mes_2024 = df[(df['published_at'].dt.year == 2024) & (df['published_at'].dt.month == mes)]
        df_mes_2025 = df[(df['published_at'].dt.year == 2025) & (df['published_at'].dt.month == mes)]
        if not df_mes_2023.empty or not df_mes_2024.empty or not df_mes_2025.empty:
            plot_comparative_analysis(df_mes_2023, df_mes_2024, df_mes_2025, mes)


def show_channel_analysis(niche, df_videos_longos, df_videos_curtos):
    st.title(f'Análise Individual dos canais - {niche.capitalize()}')
    st.markdown("""
    ### 📌 Indicadores de desempenho:
    - 📊 **Número de Views, Comentários e Likes no mês.**
    - 📈 **Taxa de engajamento => (Número de likes + número de comentários) / Total de visualizações no mês.**
    """)
    st.markdown("<br>", unsafe_allow_html=True)
    analyze_videos(df_videos_longos, "VÍDEOS LONGOS")
    analyze_videos(df_videos_curtos, "VÍDEOS CURTOS")

