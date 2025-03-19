import re
import pandas as pd
import streamlit as st
import plotly.express as px
from collections import Counter
from wordcloud import WordCloud
import plotly.graph_objects as go
from matplotlib import pyplot as plt
from plotly.subplots import make_subplots
from scipy.stats import ttest_ind, pearsonr


def analyze_title_performance(df):
    """Análise do impacto do título no desempenho dos vídeos"""
    st.title("🔤 Análise de Títulos e Desempenho")
    
    # Análise de comprimento do título
    st.subheader("Relação entre Comprimento do Título e Desempenho")
    
    # Criar coluna de comprimento do título
    df['title_length'] = df['title'].str.len()
    
    # Agrupando por comprimento de título
    title_length_bins = [0, 20, 30, 40, 50, 60, 70, 100]
    title_length_labels = ['< 20', '20-30', '30-40', '40-50', '50-60', '60-70', '> 70']
    
    df['title_length_cat'] = pd.cut(
        df['title_length'], 
        bins=title_length_bins, 
        labels=title_length_labels
    )
    
    title_length_perf = df.groupby('title_length_cat').agg(
        avg_views=('view_count', 'mean'),
        avg_likes=('like_count', 'mean'),
        avg_engagement=('engagement_rate', 'mean'),
        count=('video_id', 'count')
    ).reset_index()
    
    # Gráfico de desempenho por comprimento de título
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Visualizações por Comprimento do Título", "Engajamento por Comprimento do Título"),
        specs=[[{"type": "bar"}, {"type": "bar"}]]
    )
    
    fig.add_trace(
        go.Bar(
            x=title_length_perf['title_length_cat'], 
            y=title_length_perf['avg_views'],
            name="Média de Visualizações",
            marker_color='royalblue',
            text=title_length_perf['avg_views'].round(0),
            textposition='outside'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=title_length_perf['title_length_cat'], 
            y=title_length_perf['avg_engagement'],
            name="Taxa de Engajamento (%)",
            marker_color='firebrick',
            text=title_length_perf['avg_engagement'].round(2),
            textposition='outside'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="Desempenho por Comprimento do Título",
        height=500,
        showlegend=False
    )
    
    fig.update_xaxes(title_text="Caracteres no Título", row=1, col=1)
    fig.update_xaxes(title_text="Caracteres no Título", row=1, col=2)
    fig.update_yaxes(title_text="Média de Visualizações", row=1, col=1)
    fig.update_yaxes(title_text="Taxa de Engajamento (%)", row=1, col=2)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Identificando o comprimento de título ideal
    best_views_length = title_length_perf.loc[title_length_perf['avg_views'].idxmax(), 'title_length_cat']
    best_engagement_length = title_length_perf.loc[title_length_perf['avg_engagement'].idxmax(), 'title_length_cat']
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Melhor comprimento para visualizações", f"{best_views_length} caracteres")
    with col2:
        st.metric("Melhor comprimento para engajamento", f"{best_engagement_length} caracteres")
    
    # Análise de correlação
    corr_title_views, p_value_views = pearsonr(df['title_length'], df['view_count'])
    corr_title_engagement, p_value_engagement = pearsonr(df['title_length'], df['engagement_rate'])
    
    st.markdown(f"""
    ### Correlação entre Comprimento do Título e Desempenho
    
    - **Correlação com visualizações**: {corr_title_views:.3f} (p-value: {p_value_views:.3f})
    - **Correlação com engajamento**: {corr_title_engagement:.3f} (p-value: {p_value_engagement:.3f})
    
    {'O comprimento do título tem uma correlação estatisticamente significativa com as visualizações.' if p_value_views < 0.05 else 'Não há correlação estatisticamente significativa entre o comprimento do título e as visualizações.'}
    
    {'O comprimento do título tem uma correlação estatisticamente significativa com o engajamento.' if p_value_engagement < 0.05 else 'Não há correlação estatisticamente significativa entre o comprimento do título e o engajamento.'}
    """)
    
    # Análise de palavras comuns em títulos populares
    st.subheader("Análise de Palavras-Chave em Títulos Populares")
    
    # Definindo o que são vídeos populares (superior a 75% em visualizações)
    view_threshold = df['view_count'].quantile(0.75)
    popular_videos = df[df['view_count'] >= view_threshold]
    
    # Preparando texto para nuvem de palavras
    try:
        # Se a biblioteca WordCloud estiver disponível
        all_titles = " ".join(popular_videos['title'])
        
        # Lista de stopwords em português
        stopwords = ["a", "o", "e", "de", "da", "do", "em", "para", "com", "um", "uma", "os", "as", "que", "como", "-", "por", "seu", "sua", "seus", "suas", "na", "no", "nos", "nas", "ao", "aos", "à", "às", "pelo", "pela", "pelos", "pelas", "num", "numa", "nuns", "numas", "dum", "duma", "duns", "dumas"]
        
        # Criando a nuvem de palavras
        wordcloud = WordCloud(
            width=800, 
            height=400, 
            background_color='white',
            stopwords=stopwords,
            min_font_size=10,
            max_font_size=100,
            colormap='viridis'
        ).generate(all_titles)
        
        # Exibindo a nuvem de palavras
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.tight_layout(pad=0)
        
        st.pyplot(plt)
        
        
        # Função para extrair palavras das strings
        def extract_words(text):
            return re.findall(r'\b[a-zA-ZÀ-ÿ]{3,}\b', text.lower())
        
        # Extrair todas as palavras dos títulos
        all_words = []
        for title in popular_videos['title']:
            words = extract_words(title)
            all_words.extend([w for w in words if w not in stopwords])
        
        # Contando as palavras mais frequentes
        word_counts = Counter(all_words)
        top_words = word_counts.most_common(100)
        
        # Criando dataframe para visualização
        top_words_df = pd.DataFrame(top_words, columns=['Palavra', 'Frequência'])
        
        # Gráfico de barras para as palavras mais comuns
        fig = px.bar(
            top_words_df, 
            x='Palavra', 
            y='Frequência',
            title="100 Palavras Mais Comuns em Vídeos Populares",
            text_auto=True
        )
        
        fig.update_traces(marker_color='teal', textposition='outside')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Análise de impacto das palavras-chave
        st.subheader("Impacto das Palavras-Chave no Desempenho")
        
        # Selecionar uma palavra-chave para análise
        palavra_chave = st.selectbox(
            "Selecione uma palavra-chave para analisar seu impacto:",
            options=[word for word, _ in top_words]
        )
        
        if palavra_chave:
            # Identificar vídeos com a palavra-chave no título
            df['contains_keyword'] = df['title'].str.lower().str.contains(palavra_chave.lower())
            
            # Calcular estatísticas para vídeos com e sem a palavra-chave
            with_keyword = df[df['contains_keyword']]
            without_keyword = df[~df['contains_keyword']]
            
            keyword_stats = pd.DataFrame([
                {
                    'Grupo': f'Com "{palavra_chave}"',
                    'Número de Vídeos': len(with_keyword),
                    'Média de Visualizações': with_keyword['view_count'].mean(),
                    'Média de Likes': with_keyword['like_count'].mean(),
                    'Taxa de Engajamento (%)': with_keyword['engagement_rate'].mean()
                },
                {
                    'Grupo': f'Sem "{palavra_chave}"',
                    'Número de Vídeos': len(without_keyword),
                    'Média de Visualizações': without_keyword['view_count'].mean(),
                    'Média de Likes': without_keyword['like_count'].mean(),
                    'Taxa de Engajamento (%)': without_keyword['engagement_rate'].mean()
                }
            ])
            
            # Visualizar comparação
            st.dataframe(keyword_stats.round(2))
            
            try:
                stat, p_value = ttest_ind(
                    with_keyword['view_count'].dropna(),
                    without_keyword['view_count'].dropna(),
                    equal_var=False
                )
                
                # Interpretação do teste estatístico
                significance = "estatisticamente significativa" if p_value < 0.05 else "não estatisticamente significativa"
                
                st.markdown(f"""
                ### Análise Estatística
                
                A diferença na média de visualizações entre vídeos com e sem a palavra "{palavra_chave}" é {significance} (p-value: {p_value:.10f}).
                
                {'Isso sugere que esta palavra-chave tem um impacto real no desempenho dos vídeos.' if p_value < 0.05 else 'Isso sugere que esta palavra-chave não tem um impacto estatisticamente comprovado no desempenho dos vídeos.'}
                """)
                
                # Visualizar comparação em gráfico
                fig = px.bar(
                    keyword_stats, 
                    x='Grupo', 
                    y=['Média de Visualizações', 'Média de Likes', 'Taxa de Engajamento (%)'],
                    barmode='group',
                    title=f'Impacto da Palavra "{palavra_chave}" no Desempenho',
                    text_auto=True
                )
                
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
                
            except Exception as e:
                st.warning(f"Não foi possível realizar o teste estatístico: {e}")
    
    except ImportError:
        st.warning("A biblioteca WordCloud não está disponível. Instale-a para visualizar a nuvem de palavras.")
        # Ainda podemos mostrar as palavras mais comuns sem a nuvem
        
    # Análise de emojis no título
    st.subheader("Impacto de Emojis nos Títulos")
    
    # Função para detectar emojis
    def contains_emoji(text):
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F700-\U0001F77F"  # alchemical symbols
            "\U0001F780-\U0001F7FF"  # Geometric Shapes
            "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
            "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
            "\U0001FA00-\U0001FA6F"  # Chess Symbols
            "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
            "\U00002702-\U000027B0"  # Dingbats
            "\U000024C2-\U0001F251" 
            "]+"
        )
        return bool(emoji_pattern.search(text))
    
    # Identificar vídeos com emoji no título
    df['has_emoji'] = df['title'].apply(contains_emoji)
    
    # Comparar desempenho
    emoji_stats = df.groupby('has_emoji').agg(
        avg_views=('view_count', 'mean'),
        avg_likes=('like_count', 'mean'),
        avg_engagement=('engagement_rate', 'mean'),
        count=('video_id', 'count')
    ).reset_index()
    
    emoji_stats['has_emoji'] = emoji_stats['has_emoji'].map({True: 'Com Emoji', False: 'Sem Emoji'})
    
    # Criar gráfico de comparação
    fig = px.bar(
        emoji_stats, 
        x='has_emoji', 
        y=['avg_views', 'avg_likes', 'avg_engagement'],
        barmode='group',
        labels={
            'has_emoji': 'Presença de Emoji',
            'avg_views': 'Média de Visualizações',
            'avg_likes': 'Média de Likes',
            'avg_engagement': 'Taxa de Engajamento (%)'
        },
        title='Impacto de Emojis no Desempenho dos Vídeos',
        text_auto=True
    )
    
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Teste estatístico para a diferença
    with_emoji = df[df['has_emoji']]
    without_emoji = df[~df['has_emoji']]
    
    try:
        stat, p_value = ttest_ind(
            with_emoji['view_count'].dropna(),
            without_emoji['view_count'].dropna(),
            equal_var=False
        )
        
        # Interpretação do teste
        significance = "estatisticamente significativa" if p_value < 0.05 else "não estatisticamente significativa"
        
        st.markdown(f"""
        ### Análise Estatística - Emojis
        
        A diferença na média de visualizações entre vídeos com e sem emojis é {significance} (p-value: {p_value:.3f}).
        
        {'Isso sugere que emojis têm um impacto real no desempenho dos vídeos.' if p_value < 0.05 else 'Isso sugere que emojis não têm um impacto estatisticamente comprovado no desempenho dos vídeos.'}
        """)
        
        # Proporção de vídeos com emoji
        total_videos = len(df)
        with_emoji_count = len(with_emoji)
        emoji_percentage = (with_emoji_count / total_videos) * 100
        
        st.metric("Porcentagem de vídeos com emoji", f"{emoji_percentage:.1f}%")
        
    except Exception as e:
        st.warning(f"Não foi possível realizar o teste estatístico: {e}")
    
    # Recomendações para títulos
    st.subheader("Recomendações para Otimização de Títulos")
    
    # Compilando as descobertas
    recommendations = []
    
    # Recomendação de comprimento
    recommendations.append(f"**Comprimento do título:** Títulos entre {best_views_length} caracteres tendem a ter melhor desempenho em visualizações.")
    
    # Recomendação sobre palavras-chave
    if 'palavra_chave' in locals() and 'p_value' in locals() and p_value < 0.05:
        recommendations.append(f"**Palavras-chave:** Considere incluir a palavra '{palavra_chave}' em seus títulos, pois ela está associada a um melhor desempenho.")
    
    # Recomendação sobre emojis
    if 'p_value' in locals() and p_value < 0.05:
        emoji_recommendation = "Considere incluir emojis nos seus títulos, pois eles estão associados a um melhor desempenho."
    else:
        emoji_recommendation = "O uso de emojis não mostrou um impacto significativo no desempenho dos vídeos."
    
    recommendations.append(f"**Emojis:** {emoji_recommendation}")
    
    # Exibir recomendações
    for i, rec in enumerate(recommendations, 1):
        st.markdown(f"{i}. {rec}")