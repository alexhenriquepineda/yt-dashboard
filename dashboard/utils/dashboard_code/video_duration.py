import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from utils.dashboard_code.texts import (
    TITLE_CURIOUS_PATTERNS, 
    TEXT_POPULAR_DAY, 
    TEXT_LEAST_POPULAR_DAY, 
    TEXT_AVG_TITLE_LENGTH, 
    TITLE_CORRELATION_GRAPH, 
    TEXT_CORRELATION
)


def display_statiscal_analysis(df, df_videos_longos):
    st.subheader(TITLE_CURIOUS_PATTERNS)
    if 'published_at' in df.columns:
        df['day_of_week'] = df['published_at'].dt.day_name()
        day_counts = df['day_of_week'].value_counts()
        most_popular_day = day_counts.idxmax()
        least_popular_day = day_counts.idxmin()
        st.markdown(f"**{TEXT_POPULAR_DAY}:** {most_popular_day} ({day_counts[most_popular_day]} vídeos)")
        st.markdown(f"**{TEXT_LEAST_POPULAR_DAY}:** {least_popular_day} ({day_counts[least_popular_day]} vídeos)")
    
    avg_title_length = df['title'].str.len().mean()
    st.markdown(f"**{TEXT_AVG_TITLE_LENGTH}:** {avg_title_length:.1f} caracteres")
    
    if all(col in df_videos_longos.columns for col in ['view_count', 'duration']):
        corr = df_videos_longos['view_count'].corr(df_videos_longos['duration'])
        correlation_description = "positiva" if corr > 0.3 else "negativa" if corr < -0.3 else "pouca"
        st.markdown(f"**Correlação entre duração e visualizações:** {correlation_description} (coeficiente: {corr:.2f})")
        
        df_videos_longos['duration_minutes'] = df_videos_longos['duration'] / 60
        
        st.subheader(TITLE_CORRELATION_GRAPH)
        plot_df = df_videos_longos[
            (df_videos_longos['duration_minutes'] < df_videos_longos['duration_minutes'].quantile(0.99)) & 
            (df_videos_longos['view_count'] < df_videos_longos['view_count'].quantile(0.99))
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