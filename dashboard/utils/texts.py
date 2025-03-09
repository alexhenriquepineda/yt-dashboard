# texts.py

# Configuração da página
PAGE_CONFIG = {
    "page_title": "YouTube Dashboard",
    "page_icon": "📊",
    "layout": "wide",
    "initial_sidebar_state": "expanded"
}

# Títulos e descrições
TITLE_OVERVIEW = "OVERVIEW"
TITLE_CHANNEL_ANALYSIS = "Análise Individual dos canais"
DESC_CHANNEL_ANALYSIS = (
    "### 📌 Indicadores de desempenho:\n"
    "- 📊 **Número de Views, Comentários e Likes no mês.**\n"
    "- 📈 **Taxa de engajamento => (Número de likes + número de comentários) / Total de visualizações no mês.**"
)
TITLE_CURIOSITIES = "📊 Curiosidades"
TITLE_WEEKDAY_CORRELATION = "📅 Correlação entre Dia da Semana e Visualizações"
TITLE_SUGGEST_CHANNEL = "📢 Sugira um Canal"

# Outros textos usados em métricas e mensagens
METRIC_CHANNEL_VIDEOS = "Canal com mais vídeos publicados"
METRIC_CHANNEL_AVG = "Canal com maior média de vídeos por mês"
METRIC_FIRST_VIDEO_CHANNEL = "Canal que postou o primeiro vídeo"
METRIC_FIRST_VIDEO_DATE = "Data do primeiro vídeo"

# Textos para vídeos com maior engajamento
TITLE_VIDEOS_ENGAGEMENT = "🔥 Vídeos com Maior Engajamento"
TITLE_VIDEO_MOST_VIEWS = "Vídeo com mais visualizações"
TITLE_VIDEO_MOST_LIKES = "Vídeo com mais likes"
TITLE_VIDEO_MOST_COMMENTS = "Vídeo com mais comentários"
TITLE_VIDEO_HIGHEST_ENGAGEMENT = "Vídeo com maior taxa de engajamento"

# Textos para características de conteúdo
TITLE_CONTENT_FEATURES = "📹 Vídeos Longos"
TITLE_LONGEST_VIDEO = "Vídeo mais longo"
TITLE_LONG_VIDEO_MOST_VIEWS = "Vídeo longo com mais visualizações"

# Textos para padrões curiosos
TITLE_CURIOUS_PATTERNS = "🔍 Padrões Curiosos"
TEXT_POPULAR_DAY = "Dia da semana mais popular para publicação"
TEXT_LEAST_POPULAR_DAY = "Dia da semana menos popular para publicação"
TEXT_AVG_TITLE_LENGTH = "Tamanho médio do título"

# Textos para gráfico de correlação
TITLE_CORRELATION_GRAPH = "📊 Gráfico de Correlação: Duração do Vídeo vs. Visualizações"
TEXT_CORRELATION = "Correlação entre duração e visualizações"

# Textos para análise estatística do dia da semana
TITLE_STATISTICAL_SUMMARY = "Resumo Estatístico por Dia da Semana"
TEXT_BEST_DAY = "Dia com maior média de visualizações"
TEXT_WORST_DAY = "Dia com menor média de visualizações"
TEXT_SIGNIFICANCE_ANALYSIS = "Análise de Significância"

# Mensagens para sugestão de canal
INPUT_SUGGEST_CHANNEL = "Digite o nome do canal que você gostaria de sugerir:"
BTN_SEND_SUGGESTION = "Enviar Sugestão"
MSG_SUGGESTION_SUCCESS = "Sugestão recebida! O canal '{}' foi registrado."
MSG_SUGGESTION_EMPTY = "Por favor, insira o nome do canal antes de enviar."
