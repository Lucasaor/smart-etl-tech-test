"""Gold Dashboard — Visualização inteligente dos KPIs gerados pelo pipeline.

Descobre automaticamente as tabelas Gold disponíveis e renderiza
visualizações específicas para cada KPI (funil, lead scoring, personas, etc.)
usando a configuração definida em viz_config.

Fallback genérico para tabelas não reconhecidas.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import streamlit as st

st.set_page_config(page_title="Gold Dashboard", page_icon="🏆", layout="wide")

from frontend.theme import apply_theme
apply_theme()

st.title("🏆 Gold Dashboard")
st.markdown("Insights e KPIs gerados automaticamente pela camada Gold do pipeline.")

# ─── Imports do projeto ────────────────────────────────────────────────────────

from config.settings import get_settings
from core.storage import get_storage_backend
from frontend.viz_config import render_kpi_charts, get_viz_specs

settings = get_settings()
storage = get_storage_backend()


# ─── Descoberta dinâmica de tabelas Gold ──────────────────────────────────────

def _descobrir_tabelas_gold() -> dict[str, pl.DataFrame]:
    """Varre o diretório Gold e carrega todas as tabelas Delta disponíveis."""
    gold_dir = Path(settings.gold_path)
    tabelas: dict[str, pl.DataFrame] = {}

    if not gold_dir.exists():
        return tabelas

    for sub in sorted(gold_dir.iterdir()):
        if sub.is_dir() and (sub / "_delta_log").exists():
            nome = sub.name
            path = str(sub)
            try:
                df = storage.read_table(path)
                if df is not None and len(df) > 0:
                    tabelas[nome] = df
            except Exception:
                pass

    return tabelas


tabelas_gold = _descobrir_tabelas_gold()

if not tabelas_gold:
    st.warning(
        "Nenhuma tabela Gold encontrada. Execute o pipeline completo na página de "
        "Configuração para gerar os dados."
    )
    st.stop()

st.sidebar.markdown("### Tabelas Disponíveis")
for nome in tabelas_gold:
    reconhecido = "📊" if get_viz_specs(nome) else "📋"
    st.sidebar.markdown(f"{reconhecido} {nome.replace('_', ' ').title()}")


# ─── Visão geral ──────────────────────────────────────────────────────────────

st.header("Visão Geral")

cols = st.columns(min(len(tabelas_gold), 5))
for idx, (nome, df) in enumerate(tabelas_gold.items()):
    with cols[idx % len(cols)]:
        st.metric(nome.replace("_", " ").title(), f"{len(df):,} linhas")


# ─── Exibição de cada tabela com visualizações específicas ───────────────────

for nome, df in tabelas_gold.items():
    st.divider()
    titulo_kpi = nome.replace("_", " ").title()
    has_specific_viz = bool(get_viz_specs(nome))
    st.header(titulo_kpi)

    # ── Métricas numéricas automáticas ──
    num_cols = [c for c in df.columns if df[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)]
    if num_cols:
        metric_cols = st.columns(min(len(num_cols), 5))
        for i, col_name in enumerate(num_cols[:5]):
            with metric_cols[i % len(metric_cols)]:
                media = df[col_name].mean()
                label = col_name.replace("_", " ").title()
                st.metric(label, f"{media:.2f}" if media is not None else "—")

    # ── Visualizações específicas do KPI ──
    charts = render_kpi_charts(nome, df)

    if charts:
        # Renderizar Charts em grid de 2 colunas quando há múltiplos
        if len(charts) == 1:
            titulo_chart, fig = charts[0]
            st.plotly_chart(fig, key=f"kpi_{nome}_{0}", width="stretch")
        else:
            for chart_idx in range(0, len(charts), 2):
                chart_cols = st.columns(2)
                for col_offset in range(2):
                    ci = chart_idx + col_offset
                    if ci < len(charts):
                        titulo_chart, fig = charts[ci]
                        with chart_cols[col_offset]:
                            st.plotly_chart(
                                fig,
                                key=f"kpi_{nome}_{ci}",
                                width="stretch",
                            )
    else:
        # ── Fallback genérico para tabelas não reconhecidas ──
        str_cols = [c for c in df.columns if df[c].dtype == pl.Utf8]
        plotted = False
        for str_col in str_cols:
            n_unique = df[str_col].n_unique()
            if 2 <= n_unique <= 30:
                contagem = df.group_by(str_col).len().sort("len", descending=True)
                try:
                    import plotly.express as px

                    fig = px.bar(
                        contagem.to_pandas(),
                        x=str_col,
                        y="len",
                        title=f"Distribuição por {str_col.replace('_', ' ').title()}",
                        color=str_col,
                        color_discrete_sequence=["#440154", "#31688E", "#21918C", "#35B779", "#FDE725"],
                    )
                    fig.update_layout(
                        xaxis_title=str_col.replace("_", " ").title(),
                        yaxis_title="Quantidade",
                        showlegend=False,
                        template="plotly_white",
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(
                        fig,
                        key=f"fallback_{nome}_{str_col}",
                        width="stretch",
                    )
                except ImportError:
                    st.bar_chart(contagem.to_pandas().set_index(str_col)["len"])
                plotted = True
                break

        if not plotted and num_cols:
            try:
                import plotly.express as px

                fig = px.histogram(
                    df.to_pandas(),
                    x=num_cols[0],
                    nbins=30,
                    title=f"Distribuição de {num_cols[0].replace('_', ' ').title()}",
                    color_discrete_sequence=["#31688E"],
                )
                fig.update_layout(
                    template="plotly_white",
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(
                    fig,
                    key=f"fallback_hist_{nome}_{num_cols[0]}",
                    width="stretch",
                )
            except ImportError:
                pass

    # ── Tabela de dados ──
    with st.expander(f"📋 Dados ({len(df):,} linhas × {len(df.columns)} colunas)"):
        st.dataframe(df.head(100).to_pandas(), hide_index=True, width="stretch")


# ─── Exportar dados ───────────────────────────────────────────────────────────

st.divider()
st.header("Exportar Dados")

opcao_export = st.selectbox(
    "Selecione a tabela para exportar",
    options=list(tabelas_gold.keys()),
)

df_export = tabelas_gold.get(opcao_export)
if df_export is not None:
    csv_data = df_export.to_pandas().to_csv(index=False)
    st.download_button(
        label=f"📥 Baixar {opcao_export} (CSV)",
        data=csv_data,
        file_name=f"gold_{opcao_export}.csv",
        mime="text/csv",
    )
    st.caption(f"{len(df_export):,} linhas × {len(df_export.columns)} colunas")
