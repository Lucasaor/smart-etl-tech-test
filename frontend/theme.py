"""Tema visual e componentes compartilhados para todas as páginas Streamlit.

Centraliza paleta de cores, CSS customizado e sidebar branding para
manter consistência visual em todo o app.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

# ─── Paleta de cores — Viridis (colorblind-friendly) ─────────────────────────
# Sampled from the Viridis perceptual colormap at equal intervals.

BRAND_COLORS = {
    "primary": "#31688E",       # Viridis mid-blue
    "secondary": "#21918C",     # Viridis teal
    "accent": "#35B779",        # Viridis green
    "highlight": "#FDE725",     # Viridis yellow
    "dark": "#440154",          # Viridis deep purple
}

# Escala sequencial para gráficos (Viridis light → dark)
COLOR_SCALE = ["#FDE725", "#35B779", "#21918C", "#31688E", "#440154"]

# Paleta categórica expandida (10 evenly-spaced Viridis samples)
COLOR_CATEGORICAL = [
    "#440154",  # deep purple
    "#482878",  # purple
    "#3E4A89",  # blue-purple
    "#31688E",  # mid-blue
    "#26828E",  # blue-teal
    "#21918C",  # teal
    "#1FA187",  # teal-green
    "#35B779",  # green
    "#6DCD59",  # lime
    "#FDE725",  # yellow
]

# Mapa semântico para outcomes / sentimentos / personas
COLOR_OUTCOME = {
    "venda_fechada": "#35B779",
    "proposta_enviada": "#21918C",
    "em_andamento": "#31688E",
    "ghosting": "#440154",
    "perdido_preco": "#482878",
    "perdido_concorrente": "#3E4A89",
    "sem_interesse": "#26828E",
}

COLOR_SENTIMENT = {
    "positivo": "#35B779",
    "neutro": "#31688E",
    "negativo": "#440154",
}

COLOR_PERSONA = {
    "decidido": "#35B779",
    "pesquisador": "#21918C",
    "negociador": "#31688E",
    "fantasma": "#440154",
    "indeciso": "#482878",
}

# ─── Plotly template defaults ─────────────────────────────────────────────────

PLOTLY_LAYOUT = dict(
    template="plotly_white",
    colorway=COLOR_CATEGORICAL,
    margin=dict(l=40, r=40, t=60, b=40),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
)


# ─── Custom CSS ───────────────────────────────────────────────────────────────

_CUSTOM_CSS = """
<style>
    /* Sidebar branding area */
    .sidebar-logo {
        text-align: center;
        padding: 0.5rem 0 1rem 0;
    }
    .sidebar-logo img {
        max-width: 140px;
        margin-bottom: 0.25rem;
    }
    .sidebar-logo .developed-by {
        font-size: 0.7rem;
        color: #888;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.2rem;
    }

    /* Sidebar divider after logo */
    .sidebar-divider {
        border: none;
        border-top: 1px solid #e0e0e0;
        margin: 0.5rem 0 1rem 0;
    }
</style>
"""


# ─── Public API ───────────────────────────────────────────────────────────────


def apply_theme() -> None:
    """Inject the custom CSS and sidebar logo. Call once per page."""
    st.markdown(_CUSTOM_CSS, unsafe_allow_html=True)
    _render_sidebar_logo()


def _render_sidebar_logo() -> None:
    """Render the company logo at the top of the sidebar."""
    # Try multiple paths (local dev vs Docker)
    logo_candidates = [
        Path(__file__).resolve().parent.parent / "logo_dark.png",  # repo root
        Path("/app/logo_dark.png"),                                 # Docker
        Path("logo_dark.png"),                                      # cwd
    ]

    logo_path = None
    for p in logo_candidates:
        if p.exists():
            logo_path = p
            break

    with st.sidebar:
        if logo_path:
            import base64
            logo_bytes = logo_path.read_bytes()
            b64 = base64.b64encode(logo_bytes).decode()
            st.markdown(
                f"""
                <div class="sidebar-logo">
                    <div class="developed-by">developed by</div>
                    <img src="data:image/png;base64,{b64}" alt="Logo">
                </div>
                <hr class="sidebar-divider">
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                """
                <div class="sidebar-logo">
                    <div class="developed-by">developed by</div>
                    <div style="font-weight:700; color:#31688E; font-size:1.1rem;">Namastex</div>
                </div>
                <hr class="sidebar-divider">
                """,
                unsafe_allow_html=True,
            )
