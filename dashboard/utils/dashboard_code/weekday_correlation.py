import streamlit as st
import plotly.express as px
from scipy.stats import f_oneway
import plotly.graph_objects as go
from utils.dashboard_code.texts import (
    TITLE_WEEKDAY_CORRELATION,
    TITLE_STATISTICAL_SUMMARY,
    TEXT_BEST_DAY,
    TEXT_WORST_DAY,
    TEXT_SIGNIFICANCE_ANALYSIS
)


def analyze_weekday_correlation(df):
    st.title(TITLE_WEEKDAY_CORRELATION)
    
    if 'day_of_week' not in df.columns:
        df['day_of_week'] = df['published_at'].dt.day_name()
    
    dias_semana_ordem = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias_semana_pt = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    
    df['day_number'] = df['day_of_week'].map({day: i for i, day in enumerate(dias_semana_ordem)})
    
    weekday_stats = df.groupby('day_of_week').agg(
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
        groups = [df[df['day_of_week'] == day]['view_count'].values for day in dias_semana_ordem if day in df['day_of_week'].unique()]
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