import streamlit as st

from utils.dashboard_base import BaseDashboard

st.set_page_config(
    page_title="Podcast YouTube Dashboard",
    page_icon="🎙️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Estilo personalizado para a página de fitness
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #FF6F91;
        text-align: center;
        margin-bottom: 1rem;
    }
    .subheader {
        font-size: 1.5rem;
        color: #FF9671;
        margin-bottom: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #FF967140;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FF9671;
        color: white;
    }
</style>
""", unsafe_allow_html=True)


st.markdown('<div class="main-header">Dashboard dos canais de Podcast e entreterimento no YouTube</div>', unsafe_allow_html=True)
st.markdown('<div class="main-subheader">**Para podcasts foram considerados vídeos longos acima de 1 hora e vídeos curtos abaixo de 1 hora!</div>', unsafe_allow_html=True)

podcast_dashboard = BaseDashboard(niche="Podcast")
podcast_dashboard.run_dashboard()
