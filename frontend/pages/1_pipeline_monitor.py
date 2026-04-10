"""Pipeline Monitor — Acompanhamento em tempo real das execuções do pipeline.

Exibe status atual, histórico de runs, timeline visual Bronze→Silver→Gold,
métricas (rows, tempo, erros) e alertas do sistema.
"""

from __future__ import annotations

from datetime import datetime, timezone

import streamlit as st

st.set_page_config(page_title="Pipeline Monitor", page_icon="📊", layout="wide")
st.title("📊 Pipeline Monitor")
st.markdown("Acompanhe a execução do pipeline em tempo real e consulte o histórico de runs.")

# ─── Imports do projeto ────────────────────────────────────────────────────────

from monitoring.models import AlertSeverity, RunStatus
from monitoring.store import get_monitoring_store

store = get_monitoring_store()


# ─── Auto-refresh ──────────────────────────────────────────────────────────────

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)
if auto_refresh:
    st.markdown(
        '<meta http-equiv="refresh" content="30">',
        unsafe_allow_html=True,
    )


# ─── Métricas gerais ──────────────────────────────────────────────────────────

runs = store.get_pipeline_runs(limit=100)
alertas = store.get_alerts(unresolved_only=False, limit=100)
alertas_abertos = [a for a in alertas if not a.resolved]

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total de Runs", len(runs))
with col2:
    completas = sum(1 for r in runs if r.status == RunStatus.COMPLETED)
    st.metric("Runs Completas", completas)
with col3:
    falhas = sum(1 for r in runs if r.status == RunStatus.FAILED)
    st.metric("Runs com Falha", falhas, delta=f"-{falhas}" if falhas > 0 else None)
with col4:
    st.metric("Alertas Abertos", len(alertas_abertos))


# ─── Status da última execução ─────────────────────────────────────────────────

st.divider()
st.header("Última Execução")

if runs:
    ultimo_run = runs[0]

    # Indicador de status com cor
    status_cores = {
        RunStatus.COMPLETED: "🟢",
        RunStatus.FAILED: "🔴",
        RunStatus.RUNNING: "🟡",
        RunStatus.PENDING: "⚪",
    }
    icone = status_cores.get(ultimo_run.status, "⚪")

    col_status, col_info = st.columns([1, 3])
    with col_status:
        st.markdown(f"### {icone} {ultimo_run.status.value.upper()}")
        st.caption(f"Run ID: `{ultimo_run.run_id}`")
    with col_info:
        st.markdown(f"**Trigger:** {ultimo_run.trigger}")
        st.markdown(f"**Camadas:** {', '.join(ultimo_run.layers)}")
        st.markdown(f"**Início:** {ultimo_run.started_at.strftime('%d/%m/%Y %H:%M:%S')}")
        if ultimo_run.duration_sec:
            st.markdown(f"**Duração:** {ultimo_run.duration_sec:.1f}s")

    # Timeline dos steps
    st.subheader("Timeline dos Steps")
    if ultimo_run.steps:
        for step in ultimo_run.steps:
            step_icone = status_cores.get(step.status, "⚪")
            duracao = f"{step.duration_sec:.1f}s" if step.duration_sec else "—"
            rows_info = f"{step.rows_output:,} rows" if step.rows_output else "—"

            with st.expander(
                f"{step_icone} **{step.step_name.upper()}** — {step.status.value} | {duracao} | {rows_info}",
                expanded=step.status == RunStatus.FAILED,
            ):
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Rows Processadas", f"{step.rows_output:,}")
                with col_b:
                    st.metric("Duração", duracao)
                with col_c:
                    if step.started_at:
                        st.metric("Início", step.started_at.strftime("%H:%M:%S"))

                if step.error_message:
                    st.error(f"**Erro:** {step.error_message}")
    else:
        st.info("Nenhum step registrado para esta execução.")
else:
    st.info("Nenhuma execução registrada ainda. Inicie o pipeline na página de Configuração.")


# ─── Histórico de Runs ─────────────────────────────────────────────────────────

st.divider()
st.header("Histórico de Execuções")

if runs:
    # Filtros
    col_filtro1, col_filtro2 = st.columns(2)
    with col_filtro1:
        filtro_status = st.multiselect(
            "Filtrar por status",
            options=[s.value for s in RunStatus],
            default=[s.value for s in RunStatus],
        )
    with col_filtro2:
        filtro_trigger = st.multiselect(
            "Filtrar por trigger",
            options=list({r.trigger for r in runs}),
            default=list({r.trigger for r in runs}),
        )

    runs_filtradas = [
        r for r in runs
        if r.status.value in filtro_status and r.trigger in filtro_trigger
    ]

    # Tabela de histórico
    dados_tabela = []
    for r in runs_filtradas:
        duracao = f"{r.duration_sec:.1f}s" if r.duration_sec else "em andamento"
        total_rows = sum(s.rows_output for s in r.steps)
        steps_ok = sum(1 for s in r.steps if s.status == RunStatus.COMPLETED)
        steps_total = len(r.steps)
        erro = next(
            (s.error_message for s in r.steps if s.error_message),
            None,
        )

        dados_tabela.append({
            "Status": status_cores.get(r.status, "⚪"),
            "Run ID": r.run_id,
            "Trigger": r.trigger,
            "Camadas": ", ".join(r.layers),
            "Steps": f"{steps_ok}/{steps_total}",
            "Rows": f"{total_rows:,}",
            "Duração": duracao,
            "Início": r.started_at.strftime("%d/%m %H:%M"),
            "Erro": (erro[:80] + "...") if erro and len(erro) > 80 else (erro or "—"),
        })

    st.dataframe(dados_tabela, use_container_width=True, hide_index=True)

    # Detalhes expandíveis por run
    with st.expander("Detalhes por Run", expanded=False):
        run_selecionada = st.selectbox(
            "Selecione uma execução",
            options=[r.run_id for r in runs_filtradas],
            format_func=lambda rid: next(
                f"{rid} — {r.status.value} ({r.started_at.strftime('%d/%m %H:%M')})"
                for r in runs_filtradas if r.run_id == rid
            ),
        )
        if run_selecionada:
            run = next(r for r in runs_filtradas if r.run_id == run_selecionada)
            st.json({
                "run_id": run.run_id,
                "status": run.status.value,
                "trigger": run.trigger,
                "layers": run.layers,
                "started_at": run.started_at.isoformat(),
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                "duration_sec": run.duration_sec,
                "steps": [
                    {
                        "step": s.step_name,
                        "status": s.status.value,
                        "rows_output": s.rows_output,
                        "duration_sec": s.duration_sec,
                        "error": s.error_message,
                    }
                    for s in run.steps
                ],
            })
else:
    st.info("Nenhuma execução registrada.")


# ─── Alertas ────────────────────────────────────────────────────────────────────

st.divider()
st.header("Alertas")

if alertas:
    tab_abertos, tab_todos = st.tabs(["Abertos", "Todos"])

    severidade_icone = {
        AlertSeverity.CRITICAL: "🔴",
        AlertSeverity.ERROR: "🟠",
        AlertSeverity.WARNING: "🟡",
        AlertSeverity.INFO: "🔵",
    }

    with tab_abertos:
        if alertas_abertos:
            for alerta in alertas_abertos:
                icone_sev = severidade_icone.get(alerta.severity, "⚪")
                with st.expander(
                    f"{icone_sev} [{alerta.severity.value.upper()}] {alerta.title}",
                    expanded=alerta.severity in (AlertSeverity.CRITICAL, AlertSeverity.ERROR),
                ):
                    st.markdown(f"**Fonte:** {alerta.source}")
                    st.markdown(f"**Horário:** {alerta.timestamp.strftime('%d/%m/%Y %H:%M:%S')}")
                    if alerta.message:
                        st.markdown(alerta.message)
        else:
            st.success("Nenhum alerta aberto. Sistema saudável.")

    with tab_todos:
        dados_alertas = []
        for a in alertas:
            dados_alertas.append({
                "Severidade": f"{severidade_icone.get(a.severity, '⚪')} {a.severity.value}",
                "Título": a.title,
                "Fonte": a.source,
                "Resolvido": "✅" if a.resolved else "❌",
                "Horário": a.timestamp.strftime("%d/%m %H:%M"),
            })
        st.dataframe(dados_alertas, use_container_width=True, hide_index=True)
else:
    st.info("Nenhum alerta registrado.")
