import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime


def _format_number(value: float) -> str:
    if pd.isna(value):
        return "0"
    return f"{int(round(value)):,}".replace(",", ".")


def _format_rank_delta(delta_value: float) -> str:
    if pd.isna(delta_value):
        return "novo"
    if delta_value > 0:
        return f"+{int(delta_value)}"
    if delta_value < 0:
        return str(int(delta_value))
    return "0"


def _build_monthly_channel_ranking(df: pd.DataFrame) -> pd.DataFrame:
    duration_series = pd.to_numeric(df.get("duration"), errors="coerce")
    df = df.copy()
    df["is_short"] = duration_series.le(180).fillna(False)

    ranking_df = (
        df.dropna(subset=["ano_mes_publish", "channel_id", "channel_name"])
        .groupby(["ano_mes_publish", "channel_id", "channel_name"], as_index=False)
        .agg(
            monthly_views=("view_count", "sum"),
            long_views=("view_count", lambda values: values[df.loc[values.index, "is_short"] == False].sum()),
            short_views=("view_count", lambda values: values[df.loc[values.index, "is_short"]].sum()),
            published_videos=("video_id", "nunique"),
            long_videos_published=("video_id", lambda values: values[df.loc[values.index, "is_short"] == False].nunique()),
            shorts_published=("video_id", lambda values: values[df.loc[values.index, "is_short"]].nunique()),
            avg_engagement_rate=("engagement_rate", "mean"),
        )
    )

    if ranking_df.empty:
        return ranking_df

    ranking_df = ranking_df.sort_values(
        ["ano_mes_publish", "monthly_views", "published_videos"],
        ascending=[True, False, False],
    )
    ranking_df["rank"] = ranking_df.groupby("ano_mes_publish").cumcount() + 1
    ranking_df["previous_rank"] = ranking_df.groupby("channel_id")["rank"].shift(1)
    ranking_df["rank_change"] = ranking_df["previous_rank"] - ranking_df["rank"]
    ranking_df["movement"] = np.select(
        [
            ranking_df["previous_rank"].isna(),
            ranking_df["rank_change"] > 0,
            ranking_df["rank_change"] < 0,
        ],
        [
            "Novo no ranking",
            "Subiu",
            "Desceu",
        ],
        default="Estável",
    )
    return ranking_df


def _render_metric_strip(df: pd.DataFrame, month_df: pd.DataFrame, selected_month: str) -> None:
    total_channels = df["channel_id"].nunique()
    total_videos = df["video_id"].nunique()
    avg_views_per_video = df["view_count"].mean()
    month_views = month_df["view_count"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Canais monitorados", _format_number(total_channels))
    col2.metric("Vídeos no nicho", _format_number(total_videos))
    col3.metric("Views médias por vídeo", _format_number(avg_views_per_video))
    col4.metric(f"Views dos vídeos publicados em {selected_month}", _format_number(month_views))


def _render_monthly_trend(df: pd.DataFrame) -> None:
    trend_df = (
        df.dropna(subset=["ano_mes_publish"])
        .groupby("ano_mes_publish", as_index=False)
        .agg(
            monthly_views=("view_count", "sum"),
            active_channels=("channel_id", "nunique"),
            published_videos=("video_id", "nunique"),
        )
        .sort_values("ano_mes_publish")
    )

    if trend_df.empty:
        st.info("Ainda não há meses suficientes para construir a tendência do nicho.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=trend_df["ano_mes_publish"],
            y=trend_df["monthly_views"],
            mode="lines+markers",
            name="Views dos vídeos publicados no mês",
            line=dict(color="#f97316", width=4),
            marker=dict(size=9, color="#0f172a"),
        )
    )
    fig.add_trace(
        go.Bar(
            x=trend_df["ano_mes_publish"],
            y=trend_df["published_videos"],
            name="Vídeos publicados no mês",
            marker_color="#fdba74",
            opacity=0.45,
            yaxis="y2",
        )
    )
    fig.update_layout(
        title="Pulso mensal do nicho",
        template="plotly_white",
        height=420,
        margin=dict(l=20, r=20, t=60, b=20),
        legend=dict(orientation="h", y=1.05, x=0),
        yaxis=dict(title="Views"),
        yaxis2=dict(title="Vídeos", overlaying="y", side="right", showgrid=False),
        xaxis=dict(title="Mês de publicação"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_channel_share(month_ranking: pd.DataFrame) -> None:
    top_channels = month_ranking.head(12).copy()
    if top_channels.empty:
        st.info("Sem dados suficientes para o ranking do mês selecionado.")
        return

    fig = px.treemap(
        top_channels,
        path=["channel_name"],
        values="monthly_views",
        color="avg_engagement_rate",
        color_continuous_scale=["#fee2e2", "#fb923c", "#7c2d12"],
    )
    fig.update_layout(
        title="Participação dos canais nas views dos vídeos publicados no mês",
        margin=dict(l=0, r=0, t=50, b=0),
        height=440,
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_ranking_table(month_ranking: pd.DataFrame) -> None:
    ranking_table = month_ranking.copy()
    ranking_table["rank_change_display"] = ranking_table["rank_change"].apply(_format_rank_delta)
    ranking_table["avg_engagement_rate"] = ranking_table["avg_engagement_rate"].round(2)
    ranking_table = ranking_table.rename(
        columns={
            "rank": "Posição",
            "channel_name": "Canal",
            "long_views": "Views em vídeos longos",
            "short_views": "Views em shorts",
            "monthly_views": "Visualizações dos vídeos publicados no mês",
            "published_videos": "Vídeos publicados",
            "long_videos_published": "Vídeos longos publicados",
            "shorts_published": "Shorts publicados",
            "previous_rank": "Posição anterior",
            "rank_change_display": "Variação",
            "movement": "Movimento",
            "avg_engagement_rate": "Engajamento médio (%)",
        }
    )
    st.dataframe(
        ranking_table[
            [
                "Posição",
                "Canal",
                "Views em vídeos longos",
                "Views em shorts",
                "Visualizações dos vídeos publicados no mês",
                "Vídeos publicados",
                "Vídeos longos publicados",
                "Shorts publicados",
                "Engajamento médio (%)",
                "Posição anterior",
                "Variação",
                "Movimento",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


def _render_movers(month_ranking: pd.DataFrame) -> None:
    col1, col2 = st.columns(2)

    risers = month_ranking[month_ranking["rank_change"] > 0].sort_values("rank_change", ascending=False).head(5)
    fallers = month_ranking[month_ranking["rank_change"] < 0].sort_values("rank_change").head(5)

    with col1:
        st.subheader("Quem mais subiu")
        if risers.empty:
            st.info("Nenhum canal subiu de posição em relação ao mês anterior.")
        else:
            for _, row in risers.iterrows():
                st.metric(
                    row["channel_name"],
                    f"#{int(row['rank'])}",
                    f"+{int(row['rank_change'])} posições",
                )

    with col2:
        st.subheader("Quem mais caiu")
        if fallers.empty:
            st.info("Nenhum canal caiu de posição em relação ao mês anterior.")
        else:
            for _, row in fallers.iterrows():
                st.metric(
                    row["channel_name"],
                    f"#{int(row['rank'])}",
                    f"{int(row['rank_change'])} posições",
                )


def show_niche_landscape(niche: str, df: pd.DataFrame) -> None:
    st.title(f"Panorama do Nicho: {niche}")
    st.markdown(
        """
        Uma leitura executiva do nicho inteiro usando o histórico de extrações mensais.
        O ranking abaixo soma as `view_count` dos vídeos publicados no mês selecionado.
        """
    )

    required_columns = {
        "video_id",
        "channel_id",
        "channel_name",
        "view_count",
        "engagement_rate",
        "ano_mes_publish",
    }
    if df.empty or not required_columns.issubset(df.columns):
        st.warning("Os dados silver ainda não têm todas as colunas necessárias para o panorama do nicho.")
        return

    ranking_df = _build_monthly_channel_ranking(df.copy())
    available_months = ranking_df["ano_mes_publish"].dropna().sort_values(ascending=False).unique().tolist()

    if not available_months:
        st.info("Ainda não há histórico mensal suficiente para montar o ranking.")
        return

    current_year_month = datetime.now().strftime("%Y-%m")
    default_index = available_months.index(current_year_month) if current_year_month in available_months else 0

    selected_month = st.selectbox(
        "Mês de referência para o ranking",
        options=available_months,
        index=default_index,
    )
    available_channels_for_month = ranking_df[ranking_df["ano_mes_publish"] == selected_month]["channel_id"].nunique()
    slider_min = 5 if available_channels_for_month >= 5 else 1
    slider_default = min(30, available_channels_for_month)
    top_n = st.slider(
        "Quantidade de canais no ranking",
        min_value=slider_min,
        max_value=available_channels_for_month,
        value=slider_default,
        step=1,
    )

    month_df = df[df["ano_mes_publish"] == selected_month].copy()
    month_ranking = ranking_df[ranking_df["ano_mes_publish"] == selected_month].head(top_n).copy()

    _render_metric_strip(df, month_df, selected_month)

    col1, col2 = st.columns((1.5, 1))
    with col1:
        _render_monthly_trend(df)
    with col2:
        _render_channel_share(month_ranking)

    st.subheader(f"Ranking mensal de canais em {selected_month}")
    _render_ranking_table(month_ranking)
    _render_movers(month_ranking)
