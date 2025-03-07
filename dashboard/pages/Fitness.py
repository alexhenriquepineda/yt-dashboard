import streamlit as st
import sys
from pathlib import Path

# Adiciona o diretório pai ao path para poder importar os módulos personalizados
sys.path.append(str(Path(__file__).parent.parent))
from utils.dashboard_base import BaseDashboard

st.set_page_config(
    page_title="Fitness YouTube Dashboard",
    page_icon="💪",
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


st.markdown('<div class="main-header">Dashboard dos canais de Saúde e Fitness no YouTube</div>', unsafe_allow_html=True)

fitness_dashboard = BaseDashboard(niche="Fitness")
fitness_dashboard.run_dashboard()