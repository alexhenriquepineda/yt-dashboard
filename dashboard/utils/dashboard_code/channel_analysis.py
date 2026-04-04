import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st


def aggregate_data(df: pd.DataFrame, year: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "channel_name",
                f"total_views_{year}",
                f"total_likes_{year}",
                f"total_comments_{year}",
                f"avg_engagement_rate_{year}",
            ]
        )

    return (
        df.groupby("channel_name")
        .agg(
            **{
                f"total_views_{year}": ("view_count", "sum"),
                f"total_likes_{year}": ("like_count", "sum"),
                f"total_comments_{year}": ("comment_count", "sum"),
                f"avg_engagement_rate_{year}": ("engagement_rate", "mean"),
            }
        )
        .reset_index()
    )


def plot_comparative_analysis(df_by_year: dict[int, pd.DataFrame], mes: int) -> None:
    available_years = sorted([year for year, year_df in df_by_year.items() if not year_df.empty])
    if not available_years:
        return

    aggregated_frames = [aggregate_data(df_by_year[year], year) for year in available_years]
    agg_comparativo = aggregated_frames[0]
    for frame in aggregated_frames[1:]:
        agg_comparativo = pd.merge(agg_comparativo, frame, on="channel_name", how="outer")

    colors = px.colors.qualitative.Bold + px.colors.qualitative.Safe + px.colors.qualitative.Vivid
    month_name = pd.to_datetime(str(mes), format="%m").strftime("%B")
    fig = go.Figure()

    for index, year in enumerate(available_years):
        color = colors[index % len(colors)]
        fig.add_trace(
            go.Bar(
                x=agg_comparativo["channel_name"],
                y=agg_comparativo[f"total_views_{year}"],
                name=f"Views {month_name} {year}",
                marker_color=color,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=agg_comparativo["channel_name"],
                y=agg_comparativo[f"avg_engagement_rate_{year}"],
                name=f"Engagement {month_name} {year}",
                mode="lines+markers",
                marker_color=color,
                line=dict(width=2, dash="dot"),
                yaxis="y2",
            )
        )

    title_years = ", ".join(str(year) for year in available_years)
    fig.update_layout(
        title=f"Número de views por canal e taxa de engajamento em {month_name} de {title_years}",
        xaxis=dict(title="Canal"),
        yaxis=dict(title="Views", side="left"),
        yaxis2=dict(title="Engagement Rate (%)", overlaying="y", side="right"),
        legend=dict(x=0.01, y=1.1, orientation="h"),
        bargap=0.2,
        template="plotly_white",
        height=560,
    )
    st.plotly_chart(fig, use_container_width=True)


def analyze_videos(df: pd.DataFrame, title: str) -> None:
    st.markdown(f"### {title}")
    if df.empty:
        st.warning(f"Não há dados disponíveis para {title}")
        return

    available_years = sorted(df["published_at"].dropna().dt.year.unique().tolist())
    if not available_years:
        st.warning(f"Não foi possível identificar anos válidos para {title}")
        return

    selected_years = st.multiselect(
        f"Anos exibidos em {title}",
        options=available_years,
        default=available_years[-3:],
        key=f"years_{title}",
    )

    if not selected_years:
        st.info("Selecione pelo menos um ano para visualizar a comparação mensal.")
        return

    for mes in range(1, 13):
        df_by_year = {
            year: df[(df["published_at"].dt.year == year) & (df["published_at"].dt.month == mes)]
            for year in selected_years
        }
        if any(not year_df.empty for year_df in df_by_year.values()):
            plot_comparative_analysis(df_by_year, mes)


def show_channel_analysis(niche: str, df_videos_longos: pd.DataFrame, df_videos_curtos: pd.DataFrame) -> None:
    st.title(f"Análise Individual dos canais - {niche.capitalize()}")
    st.markdown(
        """
    ### 📌 Indicadores de desempenho:
    - 📊 **Número de Views, Comentários e Likes no mês.**
    - 📈 **Taxa de engajamento => (Número de likes + número de comentários) / Total de visualizações no mês.**
    - 🔄 **Comparação automática entre todos os anos disponíveis no dataset.**
    """
    )
    st.markdown("<br>", unsafe_allow_html=True)
    analyze_videos(df_videos_longos, "VÍDEOS LONGOS")
    analyze_videos(df_videos_curtos, "VÍDEOS CURTOS")
