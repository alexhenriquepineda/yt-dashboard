import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="YouTube Dashboard Home",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Título principal
st.title("YouTube Dashboards")
st.markdown("### Explore os insights de diferentes nichos do YouTube")

# Container para os cards dos dashboards
st.markdown("""
<style>
.dashboard-card {
    padding: 20px;
    border-radius: 10px;
    margin-bottom: 20px;
    transition: transform 0.3s;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
}
.dashboard-card:hover {
    transform: scale(1.02);
}
.fitness-card {
    background: linear-gradient(to right, #FF9671, #FF6F91);
    color: white;
}
.financas-card {
    background: linear-gradient(to right, #FFC75F, #F9F871);
    color: #333;
}
.gaming-card {
    background: linear-gradient(to right, #845EC2, #D65DB1);
    color: white;
}
.tech-card {
    background: linear-gradient(to right, #00C9A7, #4D8076);
    color: white;
}
.card-container {
    display: flex;
    flex-direction: column;
}
.card-content {
    flex-grow: 1;
}
.card-content h2, .card-content p {
    margin: 0;
    padding: 0;
}
.card-button {
    margin-top: 10px;
}
</style>
""", unsafe_allow_html=True)

# Função corrigida para criar um card com botão
def dashboard_card(title, description, icon, page_name, card_class):
    # Container para o card
    card_container = st.container()
    
    with card_container:
        # Conteúdo visual do card
        st.markdown(f"""
        <div class="dashboard-card {card_class}">
            <div class="card-content">
                <h2>{icon} {title}</h2>
                <p>{description}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Botão para navegação usando st.switch_page
        if st.button(f"Acessar dashboard {title}", key=f"btn_{page_name}"):
            st.switch_page(f"pages/{page_name}.py")

# Layout das colunas
col1, col2 = st.columns(2)

with col1:
    dashboard_card(
        "Fitness", 
        "Análise de canais de fitness, saúde e bem-estar",
        "💪", 
        "Fitness",
        "fitness-card"
    )
    
with col2:
    dashboard_card(
        "Finanças", 
        "Análise de canais de finanças, investimentos e criptomoedas",
        "💰", 
        "Financas",  # Corrigido de "Creator" para "financas"
        "financas-card"
    )    



# Informações sobre o projeto
st.sidebar.title("Sobre")
st.sidebar.info("""
Este projeto analisa dados de vários nichos do YouTube para fornecer insights sobre o desempenho dos canais.

""")

# Adicionar footer
st.markdown("---")
st.markdown("© 2025 YouTube Dashboard Analytics")