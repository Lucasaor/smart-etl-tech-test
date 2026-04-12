"""Agent Monitor — Feed de ações dos agentes de IA.

Exibe detalhes das ações por agente (CodeGen, Pipeline, Monitor, Repair),
métricas de tokens e custo, filtros por agente e período.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import streamlit as st

st.set_page_config(page_title="Agent Monitor", page_icon="🤖", layout="wide")

from frontend.theme import apply_theme
apply_theme()

st.title("🤖 Agent Monitor")
st.markdown("Monitore as ações dos agentes de IA e acompanhe custos e consumo de tokens.")

# ─── Imports do projeto ────────────────────────────────────────────────────────

from monitoring.store import get_monitoring_store

store = get_monitoring_store()


# ─── Métricas de consumo LLM ──────────────────────────────────────────────────

st.header("Consumo de LLM")

custo_total = store.get_total_llm_cost()
tokens_totais = store.get_total_tokens()
todas_acoes = store.get_agent_actions(limit=500)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Custo Total (USD)", f"${custo_total:.4f}")
with col2:
    st.metric("Tokens de Entrada", f"{tokens_totais['input_tokens']:,}")
with col3:
    st.metric("Tokens de Saída", f"{tokens_totais['output_tokens']:,}")
with col4:
    st.metric("Total de Ações", len(todas_acoes))


# ─── Custo por agente ─────────────────────────────────────────────────────────

st.divider()
st.header("Métricas por Agente")

if todas_acoes:
    # Agrupar por agente
    agentes: dict[str, dict] = {}
    for acao in todas_acoes:
        nome = acao.agent_name
        if nome not in agentes:
            agentes[nome] = {
                "total_acoes": 0,
                "acoes_sucesso": 0,
                "acoes_falha": 0,
                "custo": 0.0,
                "input_tokens": 0,
                "output_tokens": 0,
                "modelos": set(),
            }
        agentes[nome]["total_acoes"] += 1
        if acao.success:
            agentes[nome]["acoes_sucesso"] += 1
        else:
            agentes[nome]["acoes_falha"] += 1
        agentes[nome]["custo"] += acao.cost
        agentes[nome]["input_tokens"] += acao.input_tokens
        agentes[nome]["output_tokens"] += acao.output_tokens
        if acao.model_used:
            agentes[nome]["modelos"].add(acao.model_used)

    # Cards por agente
    colunas = st.columns(min(len(agentes), 4))
    for idx, (nome, metricas) in enumerate(agentes.items()):
        with colunas[idx % len(colunas)]:
            taxa_sucesso = (
                metricas["acoes_sucesso"] / metricas["total_acoes"] * 100
                if metricas["total_acoes"] > 0
                else 0
            )
            st.markdown(f"### {nome}")
            st.metric("Ações", metricas["total_acoes"])
            st.metric("Taxa de Sucesso", f"{taxa_sucesso:.0f}%")
            st.metric("Custo", f"${metricas['custo']:.4f}")
            st.caption(
                f"Tokens: {metricas['input_tokens']:,} in / "
                f"{metricas['output_tokens']:,} out"
            )
            if metricas["modelos"]:
                st.caption(f"Modelos: {', '.join(metricas['modelos'])}")
else:
    st.info("Nenhuma ação de agente registrada ainda.")


# ─── Feed de ações ─────────────────────────────────────────────────────────────

st.divider()
st.header("Feed de Ações")

if todas_acoes:
    # Filtros
    col_filtro1, col_filtro2, col_filtro3 = st.columns(3)

    nomes_agentes = sorted({a.agent_name for a in todas_acoes})

    with col_filtro1:
        filtro_agente = st.multiselect(
            "Filtrar por agente",
            options=nomes_agentes,
            default=nomes_agentes,
        )
    with col_filtro2:
        filtro_sucesso = st.selectbox(
            "Status",
            options=["Todos", "Sucesso", "Falha"],
            index=0,
        )
    with col_filtro3:
        filtro_limite = st.slider("Máximo de ações", 10, 200, 50, step=10)

    # Aplicar filtros
    acoes_filtradas = [
        a for a in todas_acoes
        if a.agent_name in filtro_agente
        and (
            filtro_sucesso == "Todos"
            or (filtro_sucesso == "Sucesso" and a.success)
            or (filtro_sucesso == "Falha" and not a.success)
        )
    ][:filtro_limite]

    # Tabela de ações
    dados_acoes = []
    for a in acoes_filtradas:
        dados_acoes.append({
            "Status": "✅" if a.success else "❌",
            "Agente": a.agent_name,
            "Ação": a.action,
            "Modelo": a.model_used or "—",
            "Tokens": f"{a.input_tokens + a.output_tokens:,}",
            "Custo": f"${a.cost:.4f}",
            "Horário": a.timestamp.strftime("%d/%m %H:%M:%S"),
        })

    st.dataframe(dados_acoes, width="stretch", hide_index=True)

    # Detalhes expandíveis
    st.subheader("Detalhes das Ações")
    for a in acoes_filtradas[:20]:
        icone = "✅" if a.success else "❌"
        with st.expander(
            f"{icone} [{a.agent_name}] {a.action} — {a.timestamp.strftime('%d/%m %H:%M:%S')}",
            expanded=not a.success,
        ):
            col_d1, col_d2, col_d3 = st.columns(3)
            with col_d1:
                st.markdown(f"**Agente:** {a.agent_name}")
                st.markdown(f"**Ação:** {a.action}")
            with col_d2:
                st.markdown(f"**Modelo:** {a.model_used or 'N/A'}")
                st.markdown(f"**Custo:** ${a.cost:.4f}")
            with col_d3:
                st.markdown(f"**Tokens In:** {a.input_tokens:,}")
                st.markdown(f"**Tokens Out:** {a.output_tokens:,}")

            if a.input_summary:
                st.markdown("**Input:**")
                st.code(a.input_summary, language="text")

            if a.output_summary:
                st.markdown("**Output:**")
                st.code(a.output_summary, language="text")

            if a.error_message:
                st.error(f"**Erro:** {a.error_message}")
else:
    st.info("Nenhuma ação de agente registrada. Execute o pipeline para gerar atividade dos agentes.")


# ─── Custo acumulado ao longo do tempo ─────────────────────────────────────────

st.divider()
st.header("Custo Acumulado")

if todas_acoes:
    try:
        import plotly.graph_objects as go

        # Ordenar por timestamp
        acoes_ordenadas = sorted(todas_acoes, key=lambda a: a.timestamp)
        custo_acumulado = 0.0
        timestamps = []
        custos = []

        for a in acoes_ordenadas:
            custo_acumulado += a.cost
            timestamps.append(a.timestamp)
            custos.append(custo_acumulado)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=timestamps,
            y=custos,
            mode="lines+markers",
            name="Custo Acumulado (USD)",
            line=dict(color="#31688E", width=2),
            marker=dict(size=4, color="#35B779"),
        ))
        fig.update_layout(
            title="Evolução do Custo LLM",
            xaxis_title="Tempo",
            yaxis_title="Custo Acumulado (USD)",
            height=350,
            template="plotly_white",
            font=dict(family="Inter, sans-serif", size=12),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, key="agent_cost_chart", width="stretch")

    except ImportError:
        st.warning("Instale plotly para visualizar o gráfico de custo acumulado.")
else:
    st.info("Sem dados para exibir gráfico de custo.")
