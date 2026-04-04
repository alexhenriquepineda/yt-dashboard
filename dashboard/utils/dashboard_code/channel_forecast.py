from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, mean_squared_error


from xgboost import XGBRegressor
from xgboost.core import XGBoostError


FORECAST_FEATURE_COLS = ["time_index", "month_number", "quarter", "year", "month_sin", "month_cos"]



def _build_monthly_channel_views(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["year_month"] = df["published_at"].dt.to_period("M").astype(str)

    monthly_df = (
        df.dropna(subset=["channel_id", "channel_name", "published_at"])
        .groupby(["channel_id", "channel_name", "year_month"], as_index=False)
        .agg(monthly_views=("view_count", "sum"))
    )

    monthly_df["month_start"] = pd.to_datetime(monthly_df["year_month"] + "-01", errors="coerce")
    monthly_df = monthly_df.sort_values(["channel_name", "month_start"]).reset_index(drop=True)
    return monthly_df


def _filter_to_last_closed_month(channel_df: pd.DataFrame) -> pd.DataFrame:
    current_month_start = pd.Timestamp(datetime.now().strftime("%Y-%m-01"))
    return channel_df[channel_df["month_start"] < current_month_start].copy()


def _fill_missing_months(channel_df: pd.DataFrame) -> pd.DataFrame:
    if channel_df.empty:
        return channel_df

    full_range = pd.date_range(
        start=channel_df["month_start"].min(),
        end=channel_df["month_start"].max(),
        freq="MS",
    )
    completed_df = (
        channel_df.set_index("month_start")
        .reindex(full_range)
        .rename_axis("month_start")
        .reset_index()
    )
    completed_df["channel_id"] = completed_df["channel_id"].ffill().bfill()
    completed_df["channel_name"] = completed_df["channel_name"].ffill().bfill()
    completed_df["monthly_views"] = completed_df["monthly_views"].fillna(0)
    completed_df["year_month"] = completed_df["month_start"].dt.strftime("%Y-%m")
    return completed_df


def _create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    features_df = df.copy().sort_values("month_start").reset_index(drop=True)
    features_df["time_index"] = np.arange(len(features_df))
    features_df["month_number"] = features_df["month_start"].dt.month
    features_df["quarter"] = features_df["month_start"].dt.quarter
    features_df["year"] = features_df["month_start"].dt.year
    features_df["month_sin"] = np.sin(2 * np.pi * features_df["month_number"] / 12)
    features_df["month_cos"] = np.cos(2 * np.pi * features_df["month_number"] / 12)
    return features_df


def _future_feature_frame(last_month: pd.Timestamp, start_index: int, forecast_horizon: int) -> pd.DataFrame:
    future_dates = pd.date_range(
        start=last_month + pd.offsets.MonthBegin(1),
        periods=forecast_horizon,
        freq="MS",
    )
    future_df = pd.DataFrame({"month_start": future_dates})
    future_df["time_index"] = np.arange(start_index, start_index + forecast_horizon)
    future_df["month_number"] = future_df["month_start"].dt.month
    future_df["quarter"] = future_df["month_start"].dt.quarter
    future_df["year"] = future_df["month_start"].dt.year
    future_df["month_sin"] = np.sin(2 * np.pi * future_df["month_number"] / 12)
    future_df["month_cos"] = np.cos(2 * np.pi * future_df["month_number"] / 12)
    future_df["year_month"] = future_df["month_start"].dt.strftime("%Y-%m")
    return future_df


def _split_time_series(features_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    test_size = max(3, min(6, len(features_df) // 5))
    train_df = features_df.iloc[:-test_size].copy()
    test_df = features_df.iloc[-test_size:].copy()
    return train_df, test_df


def _calculate_metrics(y_true: pd.Series, y_pred: np.ndarray) -> dict[str, float]:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mape = float(mean_absolute_percentage_error(np.where(y_true == 0, 1, y_true), y_pred) * 100)
    wape = float((np.abs(y_true - y_pred).sum() / np.maximum(np.abs(y_true).sum(), 1)) * 100)
    return {
        "MAE": mae,
        "RMSE": rmse,
        "MAPE (%)": mape,
        "WAPE (%)": wape,
    }


def _fit_linear_regression(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    full_df: pd.DataFrame,
    forecast_horizon: int = 12,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    feature_cols = FORECAST_FEATURE_COLS

    validation_model = LinearRegression()
    validation_model.fit(train_df[feature_cols], train_df["monthly_views"])
    validation_preds = np.maximum(validation_model.predict(test_df[feature_cols]), 0)
    metrics = _calculate_metrics(test_df["monthly_views"], validation_preds)
    backtest_df = test_df[["month_start", "year_month"]].copy()
    backtest_df["linear_prediction"] = validation_preds.round(0).astype(int)

    final_model = LinearRegression()
    final_model.fit(full_df[feature_cols], full_df["monthly_views"])

    future_df = _future_feature_frame(
        last_month=full_df["month_start"].max(),
        start_index=len(full_df),
        forecast_horizon=forecast_horizon,
    )
    future_df["linear_prediction"] = np.maximum(final_model.predict(future_df[feature_cols]), 0).round(0).astype(int)
    return backtest_df, future_df[["month_start", "year_month", "linear_prediction"]], metrics


def _fit_xgboost_regressor(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    full_df: pd.DataFrame,
    forecast_horizon: int = 12,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, dict[str, float] | None, str | None]:
    if XGBRegressor is None:
        return None, None, None, "`xgboost` não está disponível no ambiente."

    feature_cols = FORECAST_FEATURE_COLS

    try:
        validation_model = XGBRegressor(
            n_estimators=250,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            objective="reg:squarederror",
            random_state=42,
        )
        validation_model.fit(train_df[feature_cols], train_df["monthly_views"])
        validation_preds = np.maximum(validation_model.predict(test_df[feature_cols]), 0)
        metrics = _calculate_metrics(test_df["monthly_views"], validation_preds)
        backtest_df = test_df[["month_start", "year_month"]].copy()
        backtest_df["xgb_prediction"] = validation_preds.round(0).astype(int)

        final_model = XGBRegressor(
            n_estimators=250,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            objective="reg:squarederror",
            random_state=42,
        )
        final_model.fit(full_df[feature_cols], full_df["monthly_views"])

        future_df = _future_feature_frame(
            last_month=full_df["month_start"].max(),
            start_index=len(full_df),
            forecast_horizon=forecast_horizon,
        )
        future_df["xgb_prediction"] = np.maximum(final_model.predict(future_df[feature_cols]), 0).round(0).astype(int)
        return backtest_df, future_df[["month_start", "year_month", "xgb_prediction"]], metrics, None
    except (XGBoostError, ValueError, OSError) as exc:
        return None, None, None, f"XGBoost indisponível neste ambiente: {exc}"


def _render_forecast_chart(
    history_df: pd.DataFrame,
    actual_test_df: pd.DataFrame,
    linear_backtest_df: pd.DataFrame,
    linear_future_df: pd.DataFrame,
    xgb_backtest_df: pd.DataFrame | None,
    xgb_future_df: pd.DataFrame | None,
    channel_name: str,
) -> None:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=history_df["month_start"],
            y=history_df["monthly_views"],
            mode="lines+markers",
            name="Histórico até último mês fechado",
            line=dict(color="#1d4ed8", width=3),
            marker=dict(size=8),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=actual_test_df["month_start"],
            y=actual_test_df["monthly_views"],
            mode="lines+markers",
            name="Real últimos meses",
            line=dict(color="#fc0707", width=3),
            marker=dict(size=8, symbol="diamond"),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=linear_backtest_df["month_start"],
            y=linear_backtest_df["linear_prediction"],
            mode="lines+markers",
            name="Regressão linear - backtest",
            line=dict(color="#fb923c", width=3),
            marker=dict(size=8),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=linear_future_df["month_start"],
            y=linear_future_df["linear_prediction"],
            mode="lines+markers",
            name="Regressão linear - próximos 12 meses",
            line=dict(color="#ea580c", width=3, dash="dash"),
            marker=dict(size=8),
        )
    )
    if xgb_backtest_df is not None:
        fig.add_trace(
            go.Scatter(
                x=xgb_backtest_df["month_start"],
                y=xgb_backtest_df["xgb_prediction"],
                mode="lines+markers",
                name="XGBoost - backtest",
                line=dict(color="#4ade80", width=3),
                marker=dict(size=8),
            )
        )
    if xgb_future_df is not None:
        fig.add_trace(
            go.Scatter(
                x=xgb_future_df["month_start"],
                y=xgb_future_df["xgb_prediction"],
                mode="lines+markers",
                name="XGBoost - próximos 12 meses",
                line=dict(color="#16a34a", width=3, dash="dot"),
                marker=dict(size=8),
            )
        )
    fig.update_layout(
        title=f"Previsão de visualizações mensais para {channel_name}",
        template="plotly_white",
        height=540,
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(title="Mês"),
        yaxis=dict(title="Visualizações"),
        legend=dict(orientation="h", y=1.05, x=0),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_metrics_table(linear_metrics: dict[str, float], xgb_metrics: dict[str, float] | None) -> None:
    metric_descriptions = {
        "MAE": "Erro absoluto médio entre previsto e real.",
        "RMSE": "Erro quadrático médio com maior peso para erros grandes.",
        "MAPE (%)": "Erro percentual médio em relação ao valor real.",
        "WAPE (%)": "Erro percentual absoluto ponderado pelo volume total real.",
    }
    metrics_df = pd.DataFrame(
        {
            "Métrica": ["MAE", "RMSE", "MAPE (%)", "WAPE (%)"],
            "Descrição": [
                metric_descriptions["MAE"],
                metric_descriptions["RMSE"],
                metric_descriptions["MAPE (%)"],
                metric_descriptions["WAPE (%)"],
            ],
            "Regressão Linear": [linear_metrics["MAE"], linear_metrics["RMSE"], linear_metrics["MAPE (%)"], linear_metrics["WAPE (%)"]],
            "XGBoost": [
                xgb_metrics["MAE"] if xgb_metrics else np.nan,
                xgb_metrics["RMSE"] if xgb_metrics else np.nan,
                xgb_metrics["MAPE (%)"] if xgb_metrics else np.nan,
                xgb_metrics["WAPE (%)"] if xgb_metrics else np.nan,
            ],
        }
    )
    st.subheader("Métricas dos modelos")
    st.dataframe(metrics_df.round(2), use_container_width=True, hide_index=True)


def show_channel_forecast(df: pd.DataFrame) -> None:
    st.title("Previsão de Visualizações")
    st.markdown(
        """
        Esta aba treina os modelos usando apenas os dados até o último mês fechado.
        As métricas são calculadas em validação temporal, usando os meses mais recentes fechados como teste.
        """
    )

    required_columns = {"channel_id", "channel_name", "published_at", "view_count"}
    if df.empty or not required_columns.issubset(df.columns):
        st.warning("Os dados disponíveis não têm as colunas necessárias para gerar previsões.")
        return

    monthly_df = _build_monthly_channel_views(df.copy())
    if monthly_df.empty:
        st.info("Ainda não há histórico mensal suficiente para gerar previsões.")
        return

    channel_options = monthly_df["channel_name"].dropna().sort_values().unique().tolist()
    selected_channel = st.selectbox("Escolha um canal", options=channel_options)

    channel_df = monthly_df[monthly_df["channel_name"] == selected_channel].copy()
    channel_df = _filter_to_last_closed_month(channel_df)
    channel_df = _fill_missing_months(channel_df)
    channel_df = _create_time_features(channel_df)

    if len(channel_df) < 8:
        st.warning("Este canal precisa de pelo menos 8 meses fechados de histórico para treinar e validar os modelos.")
        return

    train_df, test_df = _split_time_series(channel_df)
    linear_backtest_df, linear_forecast_df, linear_metrics = _fit_linear_regression(train_df, test_df, channel_df, forecast_horizon=12)
    xgb_backtest_df, xgb_forecast_df, xgb_metrics, xgb_error = _fit_xgboost_regressor(train_df, test_df, channel_df, forecast_horizon=12)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Meses de treino", f"{len(train_df)}")
    col2.metric("Meses de teste", f"{len(test_df)}")
    col3.metric("Último mês fechado", channel_df["year_month"].max())
    col4.metric("Canal selecionado", selected_channel)

    if xgb_error:
        st.info(xgb_error)

    _render_forecast_chart(
        history_df=train_df,
        actual_test_df=test_df,
        linear_backtest_df=linear_backtest_df,
        linear_future_df=linear_forecast_df,
        xgb_backtest_df=xgb_backtest_df,
        xgb_future_df=xgb_forecast_df,
        channel_name=selected_channel,
    )
    _render_metrics_table(linear_metrics, xgb_metrics)

    forecast_table = linear_forecast_df.rename(
        columns={
            "year_month": "Mês previsto",
            "linear_prediction": "Previsão regressão linear",
        }
    )
    if xgb_forecast_df is not None:
        forecast_table = forecast_table.merge(
            xgb_forecast_df.rename(
                columns={
                    "year_month": "Mês previsto",
                    "xgb_prediction": "Previsão XGBoost",
                }
            ),
            on="Mês previsto",
            how="left",
        )

    st.subheader("Tabela de previsão para os próximos 12 meses")
    visible_columns = [column for column in ["Mês previsto", "Previsão regressão linear", "Previsão XGBoost"] if column in forecast_table.columns]
    st.dataframe(forecast_table[visible_columns], use_container_width=True, hide_index=True)
