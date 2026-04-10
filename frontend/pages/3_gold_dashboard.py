"""Gold Dashboard — Visualização de KPIs e insights gerados pelo pipeline.

Exibe distribuição de sentimento, personas, segmentação, funil de conversão,
lead scoring e performance de vendedores. Filtros por período, campanha e vendedor.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import streamlit as st

st.set_page_config(page_title="Gold Dashboard", page_icon="🏆", layout="wide")
st.title("🏆 Gold Dashboard")
st.markdown("Insights e KPIs gerados automaticamente pela camada Gold do pipeline.")

# ─── Imports do projeto ────────────────────────────────────────────────────────

from config.settings import get_settings
from core.storage import get_storage_backend

settings = get_settings()
storage = get_storage_backend()


# ─── Funções auxiliares ────────────────────────────────────────────────────────

def carregar_tabela_safe(path: str) -> pl.DataFrame | None:
    """Carrega uma tabela Delta com tratamento de erro."""
    try:
        if storage.table_exists(path):
            return storage.read_table(path)
    except Exception:
        pass
    return None


def aplicar_filtros(df: pl.DataFrame, filtros: dict) -> pl.DataFrame:
    """Aplica filtros genéricos a um DataFrame."""
    if filtros.get("campaign_id") and "campaign_id" in df.columns:
        df = df.filter(pl.col("campaign_id").is_in(filtros["campaign_id"]))
    if filtros.get("agent_id") and "agent_id" in df.columns:
        df = df.filter(pl.col("agent_id").is_in(filtros["agent_id"]))
    return df


# ─── Carregar dados Gold ──────────────────────────────────────────────────────

df_sentimento = carregar_tabela_safe(settings.gold_sentiment_path)
df_personas = carregar_tabela_safe(settings.gold_personas_path)
df_segmentacao = carregar_tabela_safe(settings.gold_segmentation_path)
df_analytics = carregar_tabela_safe(settings.gold_analytics_path)
df_vendedores = carregar_tabela_safe(settings.gold_vendor_path)

tabelas_disponiveis = {
    "Sentimento": df_sentimento is not None,
    "Personas": df_personas is not None,
    "Segmentação": df_segmentacao is not None,
    "Analytics": df_analytics is not None,
    "Vendedores": df_vendedores is not None,
}

# Verificar se há dados
num_disponiveis = sum(tabelas_disponiveis.values())
if num_disponiveis == 0:
    st.warning(
        "Nenhuma tabela Gold encontrada. Execute o pipeline completo na página de "
        "Configuração para gerar os dados."
    )
    st.stop()

st.sidebar.markdown("### Tabelas Disponíveis")
for nome, disponivel in tabelas_disponiveis.items():
    st.sidebar.markdown(f"{'✅' if disponivel else '❌'} {nome}")


# ─── Filtros globais (sidebar) ─────────────────────────────────────────────────

st.sidebar.divider()
st.sidebar.markdown("### Filtros")

filtros: dict = {}

# Filtro por campanha
if df_sentimento is not None and "campaign_id" in df_sentimento.columns:
    campanhas = sorted(
        df_sentimento["campaign_id"].drop_nulls().unique().to_list()
    )
    if campanhas:
        selecionadas = st.sidebar.multiselect("Campanha", campanhas, default=campanhas)
        filtros["campaign_id"] = selecionadas

# Filtro por vendedor
if df_sentimento is not None and "agent_id" in df_sentimento.columns:
    vendedores = sorted(
        df_sentimento["agent_id"].drop_nulls().unique().to_list()
    )
    if vendedores:
        sel_vendedores = st.sidebar.multiselect("Vendedor", vendedores, default=vendedores)
        filtros["agent_id"] = sel_vendedores

# Aplicar filtros
if df_sentimento is not None:
    df_sentimento = aplicar_filtros(df_sentimento, filtros)
if df_personas is not None:
    df_personas = aplicar_filtros(df_personas, filtros)
if df_segmentacao is not None:
    df_segmentacao = aplicar_filtros(df_segmentacao, filtros)
if df_analytics is not None:
    df_analytics = aplicar_filtros(df_analytics, filtros)


# ─── Visão geral ──────────────────────────────────────────────────────────────

st.header("Visão Geral")

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    if df_sentimento is not None:
        st.metric("Conversas", f"{len(df_sentimento):,}")
with col2:
    if df_sentimento is not None:
        score_medio = df_sentimento["sentimento_score"].mean()
        st.metric("Score Médio", f"{score_medio:.2f}" if score_medio else "—")
with col3:
    if df_analytics is not None and "lead_score" in df_analytics.columns:
        lead_score_medio = df_analytics["lead_score"].mean()
        st.metric("Lead Score Médio", f"{lead_score_medio:.0f}" if lead_score_medio else "—")
with col4:
    if df_vendedores is not None:
        st.metric("Vendedores", f"{len(df_vendedores):,}")
with col5:
    if df_sentimento is not None:
        positivos = df_sentimento.filter(pl.col("sentimento") == "positivo").height
        taxa_pos = positivos / len(df_sentimento) * 100 if len(df_sentimento) > 0 else 0
        st.metric("% Positivo", f"{taxa_pos:.1f}%")


# ─── Análise de Sentimento ─────────────────────────────────────────────────────

st.divider()

if df_sentimento is not None:
    st.header("Análise de Sentimento")

    col_graf1, col_graf2 = st.columns(2)

    with col_graf1:
        try:
            import plotly.express as px

            contagem_sentimento = (
                df_sentimento.group_by("sentimento")
                .len()
                .sort("sentimento")
                .to_pandas()
            )
            cores_sentimento = {
                "positivo": "#2ecc71",
                "neutro": "#f39c12",
                "negativo": "#e74c3c",
            }
            fig = px.pie(
                contagem_sentimento,
                values="len",
                names="sentimento",
                title="Distribuição de Sentimento",
                color="sentimento",
                color_discrete_map=cores_sentimento,
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            contagem = df_sentimento.group_by("sentimento").len().sort("sentimento")
            st.bar_chart(contagem.to_pandas().set_index("sentimento")["len"])

    with col_graf2:
        try:
            import plotly.express as px

            fig = px.histogram(
                df_sentimento.to_pandas(),
                x="sentimento_score",
                nbins=30,
                title="Distribuição do Score de Sentimento",
                color_discrete_sequence=["#3498db"],
            )
            fig.update_layout(
                xaxis_title="Score de Sentimento",
                yaxis_title="Frequência",
            )
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.info("Instale plotly para visualizar o histograma.")

    # Sentimento por vendedor
    if "agent_id" in df_sentimento.columns:
        st.subheader("Sentimento por Vendedor")
        sentimento_vendedor = (
            df_sentimento
            .group_by(["agent_id", "sentimento"])
            .len()
            .sort(["agent_id", "sentimento"])
        )
        try:
            import plotly.express as px

            fig = px.bar(
                sentimento_vendedor.to_pandas(),
                x="agent_id",
                y="len",
                color="sentimento",
                title="Sentimento por Vendedor",
                color_discrete_map=cores_sentimento,
                barmode="group",
            )
            fig.update_layout(xaxis_title="Vendedor", yaxis_title="Conversas")
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.dataframe(sentimento_vendedor.to_pandas(), use_container_width=True)


# ─── Personas ──────────────────────────────────────────────────────────────────

if df_personas is not None:
    st.divider()
    st.header("Personas de Leads")

    col_p1, col_p2 = st.columns(2)

    with col_p1:
        contagem_personas = (
            df_personas.group_by("persona")
            .len()
            .sort("len", descending=True)
        )
        try:
            import plotly.express as px

            cores_personas = {
                "decidido": "#2ecc71",
                "negociador": "#3498db",
                "pesquisador": "#9b59b6",
                "indeciso": "#f39c12",
                "fantasma": "#95a5a6",
            }
            fig = px.bar(
                contagem_personas.to_pandas(),
                x="persona",
                y="len",
                title="Distribuição de Personas",
                color="persona",
                color_discrete_map=cores_personas,
            )
            fig.update_layout(xaxis_title="Persona", yaxis_title="Quantidade")
            st.plotly_chart(fig, use_container_width=True)
        except ImportError:
            st.bar_chart(contagem_personas.to_pandas().set_index("persona")["len"])

    with col_p2:
        if "persona_confianca" in df_personas.columns:
            try:
                import plotly.express as px

                fig = px.box(
                    df_personas.to_pandas(),
                    x="persona",
                    y="persona_confianca",
                    title="Confiança da Classificação por Persona",
                    color="persona",
                    color_discrete_map=cores_personas,
                )
                fig.update_layout(
                    xaxis_title="Persona",
                    yaxis_title="Confiança (0-1)",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.dataframe(
                    df_personas.group_by("persona")
                    .agg(pl.col("persona_confianca").mean().alias("media_confianca"))
                    .to_pandas(),
                    use_container_width=True,
                )

    # Tabela resumo por persona
    st.subheader("Resumo por Persona")
    resumo_personas = (
        df_personas.group_by("persona")
        .agg([
            pl.len().alias("quantidade"),
            pl.col("persona_confianca").mean().round(2).alias("confianca_media"),
        ])
        .sort("quantidade", descending=True)
    )
    st.dataframe(resumo_personas.to_pandas(), use_container_width=True, hide_index=True)


# ─── Segmentação ───────────────────────────────────────────────────────────────

if df_segmentacao is not None:
    st.divider()
    st.header("Segmentação de Audiência")

    # Detectar colunas de segmentação
    colunas_seg = [c for c in df_segmentacao.columns if c.startswith("seg_")]

    if colunas_seg:
        tabs_seg = st.tabs([c.replace("seg_", "").replace("_", " ").title() for c in colunas_seg])

        for tab, col_seg in zip(tabs_seg, colunas_seg):
            with tab:
                contagem = (
                    df_segmentacao.group_by(col_seg)
                    .len()
                    .sort("len", descending=True)
                )
                titulo_seg = col_seg.replace("seg_", "").replace("_", " ").title()

                try:
                    import plotly.express as px

                    fig = px.pie(
                        contagem.to_pandas(),
                        values="len",
                        names=col_seg,
                        title=f"Segmentação por {titulo_seg}",
                    )
                    st.plotly_chart(fig, use_container_width=True)
                except ImportError:
                    st.dataframe(contagem.to_pandas(), use_container_width=True)
    else:
        st.warning("Nenhuma coluna de segmentação encontrada.")


# ─── Analytics — Funil de Conversão ────────────────────────────────────────────

if df_analytics is not None:
    st.divider()
    st.header("Analytics & Lead Scoring")

    col_a1, col_a2 = st.columns(2)

    with col_a1:
        # Distribuição de lead score
        if "lead_score" in df_analytics.columns:
            try:
                import plotly.express as px

                fig = px.histogram(
                    df_analytics.to_pandas(),
                    x="lead_score",
                    nbins=20,
                    title="Distribuição de Lead Score",
                    color_discrete_sequence=["#2ecc71"],
                )
                fig.update_layout(
                    xaxis_title="Lead Score (0-100)",
                    yaxis_title="Frequência",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.info("Instale plotly para visualizar o histograma de lead score.")

    with col_a2:
        # Funil de conversão por outcome
        if "conversation_outcome" in df_analytics.columns:
            funil = (
                df_analytics.group_by("conversation_outcome")
                .len()
                .sort("len", descending=True)
            )
            try:
                import plotly.express as px

                fig = px.funnel(
                    funil.to_pandas(),
                    x="len",
                    y="conversation_outcome",
                    title="Funil de Conversão (por Outcome)",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.dataframe(funil.to_pandas(), use_container_width=True)

    # Top leads por score
    if "lead_score" in df_analytics.columns:
        st.subheader("Top 20 Leads por Score")
        top_leads = (
            df_analytics
            .sort("lead_score", descending=True)
            .head(20)
        )
        colunas_exibir = [
            c for c in [
                "conversation_id", "lead_score", "conversation_outcome",
                "total_messages", "agent_id", "campaign_id",
            ]
            if c in top_leads.columns
        ]
        st.dataframe(
            top_leads.select(colunas_exibir).to_pandas(),
            use_container_width=True,
            hide_index=True,
        )


# ─── Performance de Vendedores ────────────────────────────────────────────────

if df_vendedores is not None:
    st.divider()
    st.header("Performance de Vendedores")

    col_v1, col_v2 = st.columns(2)

    with col_v1:
        # Taxa de conversão por vendedor
        if "taxa_conversao" in df_vendedores.columns and "agent_id" in df_vendedores.columns:
            try:
                import plotly.express as px

                vendedores_sorted = df_vendedores.sort("taxa_conversao", descending=True)
                fig = px.bar(
                    vendedores_sorted.to_pandas(),
                    x="agent_id",
                    y="taxa_conversao",
                    title="Taxa de Conversão por Vendedor (%)",
                    color="taxa_conversao",
                    color_continuous_scale="RdYlGn",
                )
                fig.update_layout(
                    xaxis_title="Vendedor",
                    yaxis_title="Taxa de Conversão (%)",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.dataframe(
                    df_vendedores.select(["agent_id", "taxa_conversao"]).to_pandas(),
                    use_container_width=True,
                )

    with col_v2:
        # Score do vendedor
        if "score_vendedor" in df_vendedores.columns and "agent_id" in df_vendedores.columns:
            try:
                import plotly.express as px

                vendedores_sorted = df_vendedores.sort("score_vendedor", descending=True)
                fig = px.bar(
                    vendedores_sorted.to_pandas(),
                    x="agent_id",
                    y="score_vendedor",
                    title="Score do Vendedor (0-100)",
                    color="score_vendedor",
                    color_continuous_scale="Viridis",
                )
                fig.update_layout(
                    xaxis_title="Vendedor",
                    yaxis_title="Score",
                )
                st.plotly_chart(fig, use_container_width=True)
            except ImportError:
                st.dataframe(
                    df_vendedores.select(["agent_id", "score_vendedor"]).to_pandas(),
                    use_container_width=True,
                )

    # Tabela completa de vendedores
    st.subheader("Detalhes por Vendedor")

    colunas_vendedor = [
        c for c in [
            "agent_id", "total_conversas", "vendas_fechadas", "taxa_conversao",
            "taxa_ghosting", "media_tempo_resposta_sec", "media_mensagens",
            "score_vendedor", "mencoes_concorrentes",
        ]
        if c in df_vendedores.columns
    ]

    st.dataframe(
        df_vendedores.select(colunas_vendedor)
        .sort("score_vendedor" if "score_vendedor" in colunas_vendedor else colunas_vendedor[0], descending=True)
        .to_pandas(),
        use_container_width=True,
        hide_index=True,
    )


# ─── Exportar dados ───────────────────────────────────────────────────────────

st.divider()
st.header("Exportar Dados")

opcao_export = st.selectbox(
    "Selecione a tabela para exportar",
    options=[
        nome for nome, disp in tabelas_disponiveis.items() if disp
    ],
)

mapa_export = {
    "Sentimento": df_sentimento,
    "Personas": df_personas,
    "Segmentação": df_segmentacao,
    "Analytics": df_analytics,
    "Vendedores": df_vendedores,
}

df_export = mapa_export.get(opcao_export)
if df_export is not None:
    csv_data = df_export.to_pandas().to_csv(index=False)
    st.download_button(
        label=f"📥 Baixar {opcao_export} (CSV)",
        data=csv_data,
        file_name=f"gold_{opcao_export.lower()}.csv",
        mime="text/csv",
    )
    st.caption(f"{len(df_export):,} linhas × {len(df_export.columns)} colunas")
