"""Agentic Pipeline — Ponto de entrada do dashboard Streamlit.

Configura o layout principal com sidebar de navegação e conecta
ao monitoring store e às tabelas Delta.
"""

import streamlit as st

st.set_page_config(
    page_title="Agentic Pipeline",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Agentic Data Pipeline")
st.markdown(
    """
    Pipeline de Transformação Agêntica de Dados — Arquitetura Medalhão
    (Bronze → Silver → Gold) gerenciado por agentes de IA autônomos.

    ### Como usar

    1. **Configuração** — Envie seus dados brutos, dicionário de dados e descrição dos KPIs
    2. **Pipeline Monitor** — Acompanhe a execução do pipeline em tempo real
    3. **Agent Monitor** — Monitore as ações dos agentes de IA
    4. **Gold Dashboard** — Visualize os KPIs e insights gerados

    Use a barra lateral para navegar entre as páginas.
    """
)

st.sidebar.success("Selecione uma página acima.")
