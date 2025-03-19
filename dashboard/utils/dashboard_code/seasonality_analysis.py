import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def analyze_sazonalidade(df):
    """Análise de Sazonalidade Completa"""
    st.title("📅 Análise de Sazonalidade")
    
    # Criando um dataframe de análise mensal
    monthly_data = df.groupby(['ano', 'mes']).agg(
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
            