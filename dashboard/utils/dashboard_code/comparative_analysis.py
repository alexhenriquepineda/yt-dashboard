import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from scipy.stats import ttest_ind
import plotly.graph_objects as go


def analyze_comparative_performance(df_videos_longos, df_videos_curtos):
    """Análise Comparativa entre Videos Longos e Curtos"""
    st.title("🔄 Análise Comparativa: Vídeos Longos x Shorts")
    
    
    long_stats = {
        'count': len(df_videos_longos),
        'avg_views': df_videos_longos['view_count'].mean(),
        'avg_likes': df_videos_longos['like_count'].mean(),
        'avg_comments': df_videos_longos['comment_count'].mean(),
        'avg_engagement': df_videos_longos['engagement_rate'].mean(),
        'max_views': df_videos_longos['view_count'].max(),
        'max_engagement': df_videos_longos['engagement_rate'].max(),
    }
    
    short_stats = {
        'count': len(df_videos_curtos),
        'avg_views': df_videos_curtos['view_count'].mean(),
        'avg_likes': df_videos_curtos['like_count'].mean(),
        'avg_comments': df_videos_curtos['comment_count'].mean(),
        'avg_engagement': df_videos_curtos['engagement_rate'].mean(),
        'max_views': df_videos_curtos['view_count'].max(),
        'max_engagement': df_videos_curtos['engagement_rate'].max(),
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Vídeos Longos")
        st.metric("Quantidade", f"{long_stats['count']} vídeos")
        st.metric("Média de Visualizações", f"{long_stats['avg_views']:,.0f}")
        st.metric("Média de Likes", f"{long_stats['avg_likes']:,.0f}")
        st.metric("Taxa de Engajamento Média", f"{long_stats['avg_engagement']:.2f}%")
    
    with col2:
        st.subheader("Shorts")
        st.metric("Quantidade", f"{short_stats['count']} vídeos")
        st.metric("Média de Visualizações", f"{short_stats['avg_views']:,.0f}")
        st.metric("Média de Likes", f"{short_stats['avg_likes']:,.0f}")
        st.metric("Taxa de Engajamento Média", f"{short_stats['avg_engagement']:.2f}%")
    
    st.subheader("Comparação de Métricas")
    
    metrics_to_compare = [
        'avg_views', 'avg_likes', 'avg_comments', 'avg_engagement'
    ]
    
    metric_labels = {
        'avg_views': 'Média de Visualizações',
        'avg_likes': 'Média de Likes',
        'avg_comments': 'Média de Comentários', 
        'avg_engagement': 'Taxa de Engajamento (%)'
    }
    
    selected_metrics = st.multiselect(
        "Selecione as métricas para comparar:",
        options=metrics_to_compare,
        default=metrics_to_compare,
        format_func=lambda x: metric_labels[x]
    )
    
    if selected_metrics:
        comparison_data = []
        
        for metric in selected_metrics:
            if metric == 'avg_engagement':
                comparison_data.append({
                    'Métrica': metric_labels[metric],
                    'Vídeos Longos': long_stats[metric],
                    'Shorts': short_stats[metric]
                })
            else:
                long_value = long_stats[metric]
                short_value = short_stats[metric]
                
                comparison_data.append({
                    'Métrica': metric_labels[metric],
                    'Vídeos Longos': long_value,
                    'Shorts': short_value
                })
        
        comparison_df = pd.DataFrame(comparison_data)
        
        fig = px.bar(
            comparison_df, 
            x='Métrica', 
            y=['Vídeos Longos', 'Shorts'],
            barmode='group',
            title="Comparação de Métricas entre Vídeos Longos e Shorts",
            labels={'value': 'Valor', 'variable': 'Tipo de Vídeo'},
            text_auto='.1f'
        )
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        # Teste estatístico para comparar os grupos
        st.subheader("Análise Estatística Comparativa")
        
        metric_for_test = st.selectbox(
            "Selecione uma métrica para teste estatístico:",
            options=metrics_to_compare,
            format_func=lambda x: metric_labels[x]
        )
        
        # Executando teste t para duas amostras independentes
        try:
            if metric_for_test == 'avg_views':
                stat, p_value = ttest_ind(
                    df_videos_longos['view_count'].dropna(),
                    df_videos_curtos['view_count'].dropna(),
                    equal_var=False  # Welch's t-test não assume variâncias iguais
                )
                metric_label = 'visualizações'
            elif metric_for_test == 'avg_likes':
                stat, p_value = ttest_ind(
                    df_videos_longos['like_count'].dropna(),
                    df_videos_curtos['like_count'].dropna(), 
                    equal_var=False
                )
                metric_label = 'likes'
            elif metric_for_test == 'avg_comments':
                stat, p_value = ttest_ind(
                    df_videos_longos['comment_count'].dropna(),
                    df_videos_curtos['comment_count'].dropna(),
                    equal_var=False
                )
                metric_label = 'comentários'
            else:  # 'avg_engagement'
                stat, p_value = ttest_ind(
                    df_videos_longos['engagement_rate'].dropna(),
                    df_videos_curtos['engagement_rate'].dropna(),
                    equal_var=False
                )
                metric_label = 'taxa de engajamento'
            
            st.markdown(f"""
            #### Resultado do Teste-t para {metric_labels[metric_for_test]}
            - **Estatística t**: {stat:.4f}
            - **Valor p**: {p_value:.10f}
            
            {'**Conclusão**: Há uma diferença estatisticamente significativa na ' + metric_label + ' entre vídeos longos e shorts (p < 0.05).' 
            if p_value < 0.05 else 
            '**Conclusão**: Não há evidência estatística suficiente para afirmar que existe diferença na ' + metric_label + ' entre vídeos longos e shorts (p > 0.05).'}
            """)
        except Exception as e:
            st.warning(f"Não foi possível realizar o teste estatístico: {e}")
        
        # Abordagem 3: Box plots para comparar as distribuições
        st.subheader("Comparação de Visualizações (Box Plot)")
        fig3 = go.Figure()
        fig3.add_trace(go.Box(
            y=df_videos_longos['view_count'],
            name='Vídeos Longos',
            boxpoints='outliers',
            marker_color='blue',
            line_color='blue'
        ))
        fig3.add_trace(go.Box(
            y=df_videos_curtos['view_count'],
            name='Shorts',
            boxpoints='outliers',
            marker_color='red',
            line_color='red'
        ))
        fig3.update_layout(
            title="Comparação de Visualizações por Tipo de Vídeo",
            yaxis_title="Visualizações",
            height=500
        )
        # Usando escala logarítmica no eixo Y para melhor visualização
        fig3.update_yaxes(type="log")
        st.plotly_chart(fig3, use_container_width=True)
        # Abordagem 4: Gráfico de densidade (KDE) para visualizar a distribuição
        st.subheader("Densidade de Visualizações")
        # Preparando os dados para KDE - usando log para melhor visualização
        long_views_log = np.log10(df_videos_longos['view_count'].replace(0, 1))
        short_views_log = np.log10(df_videos_curtos['view_count'].replace(0, 1))
        fig4 = go.Figure()
        # KDE para vídeos longos
        fig4.add_trace(go.Violin(
            y=long_views_log,
            name='Vídeos Longos',
            box_visible=True,
            line_color='blue',
            fillcolor='blue',
            opacity=0.6
        ))
        # KDE para shorts
        fig4.add_trace(go.Violin(
            y=short_views_log,
            name='Shorts',
            box_visible=True,
            line_color='red',
            fillcolor='red',
            opacity=0.6
        ))
        fig4.update_layout(
            title="Densidade de Visualizações (escala logarítmica)",
            yaxis_title="Log10(Visualizações)",
            height=500
        )
        st.plotly_chart(fig4, use_container_width=True)
        
        # Conclusões gerais
        st.subheader("Conclusões da Análise Comparativa")
        
        # Calculando razões para insights
        view_ratio = short_stats['avg_views'] / long_stats['avg_views']
        engagement_ratio = short_stats['avg_engagement'] / long_stats['avg_engagement']
        
        st.markdown(f"""
        ### Principais insights:
        
        - Shorts recebem em média **{view_ratio:.2f}x** mais visualizações que vídeos longos
        - A taxa de engajamento dos shorts é **{engagement_ratio:.2f}x** a dos vídeos longos
        
        **Recomendações:**
        
        {
        "- Considerando que os shorts têm desempenho significativamente melhor em termos de visualizações e engajamento, seria recomendável aumentar a produção deste formato" 
        if view_ratio > 1.5 and engagement_ratio > 1.2 else
        "- Apesar dos shorts terem um desempenho diferente, os vídeos longos continuam sendo relevantes para a estratégia do canal"
        }
        
        - A estratégia ideal seria uma combinação de ambos os formatos, com shorts servindo como porta de entrada para atrair novos espectadores e vídeos longos para aprofundar o relacionamento
        """)