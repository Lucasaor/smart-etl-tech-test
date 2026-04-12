"""Página de Configuração — upload de dados, dicionário e KPIs.

Interface para receber as três entradas obrigatórias do pipeline:
  1. Amostra de dados brutos (parquet/csv)
  2. Dicionário de dados (markdown)
  3. Descrição dos KPIs desejados (markdown)

Após o envio, os agentes analisam os dados e GERAM o código do pipeline
dinamicamente. O código é testado e só então executado.
"""

import shutil
from pathlib import Path

import polars as pl
import streamlit as st


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _spec_exists() -> bool:
    """Verifica se a spec existe no disco."""
    try:
        from config.settings import get_settings
        from pipeline.specs import spec_existe
        return spec_existe(get_settings().spec_path)
    except Exception:
        return False


def _generated_code_exists() -> bool:
    """Verifica se o código gerado existe."""
    try:
        from config.settings import get_settings
        from agents.codegen_agent import pipeline_gerado_existe
        generated_dir = str(Path(get_settings().spec_path) / "generated")
        return pipeline_gerado_existe(generated_dir)
    except Exception:
        return False


def _tentar_reparo(pipeline_run) -> None:
    """Tenta reparar o pipeline automaticamente usando o Repair Agent."""
    from agents.repair_agent import run_repair_agent

    step_falho = next(
        (s for s in pipeline_run.steps if s.error_message),
        None,
    )

    if not step_falho:
        st.warning("Nenhum step com erro encontrado.")
        return

    with st.spinner(f"🔧 Tentando reparar camada {step_falho.step_name}..."):
        resultado = run_repair_agent(
            erro=step_falho.error_message,
            camada_falha=step_falho.step_name,
            run_id=pipeline_run.run_id,
        )

    if resultado.get("reparado"):
        st.success("✅ Reparo bem-sucedido! O pipeline foi corrigido e re-executado.")
    else:
        analise = resultado.get("analise_erro", {})
        causa = analise.get("causa_raiz", analise.get("causa_raiz_llm", "Não determinada"))
        estrategia = analise.get("estrategia", "N/A")
        tentativas = resultado.get("tentativas", 0)
        st.error(
            f"❌ Reparo falhou após {tentativas} tentativa(s).\n\n"
            f"**Causa raiz:** {causa}\n\n"
            f"**Estratégia tentada:** {estrategia}\n\n"
            f"**Recomendação:** Verifique o código gerado em `data/specs/generated/`, "
            f"corrija manualmente ou ajuste a especificação e regenere o código."
        )

st.set_page_config(page_title="Configuração", page_icon="⚙️", layout="wide")

from frontend.theme import apply_theme
apply_theme()

st.title("⚙️ Configuração do Projeto")

st.markdown(
    """
    Configure o pipeline enviando suas fontes de dados e descrições.
    Os agentes usarão estas informações para **gerar automaticamente
    o código do pipeline** em cada camada (Bronze → Silver → Gold).

    **Fluxo:**
    1. Envie os dados + dicionário + KPIs
    2. Os agentes analisam e **geram o código** de cada camada
    3. Testes são criados e executados para validar o código
    4. O pipeline é executado com o código gerado
    """
)


# ─── Estado da sessão ─────────────────────────────────────────────────────────

if "spec_salva" not in st.session_state:
    st.session_state.spec_salva = False
if "codigo_gerado" not in st.session_state:
    st.session_state.codigo_gerado = False
if "pipeline_executado" not in st.session_state:
    st.session_state.pipeline_executado = False


# ─── 1. Upload de dados brutos ────────────────────────────────────────────────

st.header("1. Amostra de Dados Brutos")
st.markdown("Envie o arquivo com os dados que serão processados na camada **Bronze**.")

arquivo_dados = st.file_uploader(
    "Selecione o arquivo (parquet ou csv)",
    type=["parquet", "csv"],
    help="Arquivo com os dados brutos. Suporta Parquet e CSV.",
)

if arquivo_dados is not None:
    st.success(f"Arquivo recebido: **{arquivo_dados.name}** ({arquivo_dados.size / 1024:.1f} KB)")

    try:
        if arquivo_dados.name.endswith(".csv"):
            df_preview = pl.read_csv(arquivo_dados, n_rows=10)
        else:
            import tempfile
            import os

            tmp_dir = tempfile.mkdtemp()
            tmp_path = os.path.join(tmp_dir, arquivo_dados.name)
            with open(tmp_path, "wb") as f:
                f.write(arquivo_dados.getvalue())
            df_preview = pl.read_parquet(tmp_path).head(10)

        st.subheader("Preview dos dados")
        st.dataframe(df_preview.to_pandas(), width="stretch")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Colunas", len(df_preview.columns))
        with col2:
            st.metric("Linhas (preview)", len(df_preview))
        with col3:
            st.metric("Tamanho", f"{arquivo_dados.size / 1024:.1f} KB")

        st.subheader("Schema detectado")
        schema_info = [
            {"Coluna": col, "Tipo": str(dtype)}
            for col, dtype in zip(df_preview.columns, df_preview.dtypes)
        ]
        st.table(schema_info)

    except Exception as e:
        st.error(f"Erro ao ler o arquivo: {e}")


# ─── 2. Dicionário de dados ───────────────────────────────────────────────────

st.header("2. Dicionário de Dados")
st.markdown(
    "Descreva as colunas dos dados, tipos esperados, valores possíveis e particularidades. "
    "Use formato **Markdown**."
)

dicionario_default = ""
dicionario_path = Path("Dicionario_de_Dados.md")
if dicionario_path.exists():
    dicionario_default = dicionario_path.read_text(encoding="utf-8")

dicionario_texto = st.text_area(
    "Dicionário de dados (markdown)",
    value=dicionario_default,
    height=300,
    help="Descreva cada coluna, seus tipos e valores válidos.",
)


# ─── 3. Descrição dos KPIs ────────────────────────────────────────────────────

st.header("3. Descrição dos KPIs")
st.markdown(
    "Descreva os indicadores e análises que deseja na camada **Gold**. "
    "Os agentes usarão esta descrição para **gerar o código** de analytics."
)

kpis_default = ""
kpis_path = Path("Descricao_KPIs.md")
if kpis_path.exists():
    kpis_default = kpis_path.read_text(encoding="utf-8")

kpis_texto = st.text_area(
    "Descrição dos KPIs (markdown)",
    value=kpis_default,
    height=300,
    help="Descreva os indicadores e análises desejados para a camada Gold.",
)


# ─── ETAPA 1: Salvar especificação ────────────────────────────────────────────

st.divider()
st.header("Etapa 1: Salvar Especificação")

if st.button("💾 Salvar Especificação", type="primary"):
    erros = []
    if arquivo_dados is None:
        erros.append("Envie o arquivo de dados brutos")
    if not dicionario_texto.strip():
        erros.append("Preencha o dicionário de dados")
    if not kpis_texto.strip():
        erros.append("Preencha a descrição dos KPIs")

    if erros:
        for erro in erros:
            st.error(erro)
    else:
        try:
            from config.settings import get_settings
            from pipeline.specs import (
                ProjectSpec,
                analisar_amostra,
                salvar_spec,
            )

            settings = get_settings()
            spec_dir = settings.spec_path
            Path(spec_dir).mkdir(parents=True, exist_ok=True)

            # Salvar arquivo de dados no diretório de specs
            dados_path = str(Path(spec_dir) / arquivo_dados.name)
            with open(dados_path, "wb") as f:
                f.write(arquivo_dados.getvalue())

            # Analisar amostra
            analise = analisar_amostra(dados_path)

            # Criar e salvar spec
            spec = ProjectSpec(
                nome="projeto_pipeline",
                dados_brutos_path=dados_path,
                dicionario_dados=dicionario_texto,
                descricao_kpis=kpis_texto,
                analise=analise,
            )

            salvar_spec(spec, spec_dir)

            st.success("✅ Especificação salva com sucesso!")
            st.json(
                {
                    "diretorio": spec_dir,
                    "dados": dados_path,
                    "linhas_detectadas": analise.num_linhas,
                    "colunas_detectadas": analise.num_colunas,
                }
            )
            st.session_state.spec_salva = True
            st.session_state.codigo_gerado = False
            st.session_state.pipeline_executado = False

        except Exception as e:
            st.error(f"Erro ao salvar especificação: {e}")


# ─── ETAPA 2: Gerar Código do Pipeline ───────────────────────────────────────

st.divider()
st.header("Etapa 2: Gerar Código do Pipeline")

if st.session_state.spec_salva or _spec_exists():
    st.markdown(
        "Os agentes vão analisar sua especificação e **gerar o código Python** "
        "para cada camada do pipeline (Bronze, Silver, Gold)."
    )

    if st.button("🤖 Gerar Código do Pipeline", type="primary"):
        try:
            from config.settings import get_settings
            from pipeline.specs import carregar_spec
            from agents.codegen_agent import (
                CodeGenAgent,
                salvar_pipeline_gerado,
            )

            settings = get_settings()
            spec = carregar_spec(settings.spec_path)
            generated_dir = str(Path(settings.spec_path) / "generated")

            status_text = st.empty()

            # Track progress via step counting:
            # análise(1) + bronze(2) + silver(2) + gold_modules(N×2) — each layer has gen + retry potential
            _progress_state = {"current": 0, "total": 12}  # reasonable default

            def _codegen_progress(msg: str):
                _progress_state["current"] += 1
                pct = min(int(_progress_state["current"] / _progress_state["total"] * 100), 99)
                st.toast(f"[{pct}%] {msg}")
                status_text.info(f"[{pct}%] {msg}")

            agent = CodeGenAgent()

            pipeline = agent.gerar_pipeline_completo(
                spec, progress_callback=_codegen_progress,
            )

            st.toast("✅ [100%] Geração de código concluída!")
            status_text.empty()

            # Salvar no disco
            paths = salvar_pipeline_gerado(pipeline, generated_dir)

            # Mostrar resultado
            if pipeline.completo:
                st.success("✅ Código gerado com sucesso para todas as camadas!")
                st.session_state.codigo_gerado = True
            else:
                st.warning("⚠️ Geração parcial — algumas camadas falharam.")

            # Detalhes por camada
            col1, col2, col3 = st.columns(3)
            with col1:
                if pipeline.bronze and pipeline.bronze.codigo:
                    st.metric("Bronze", f"✅ {pipeline.bronze.codigo.count(chr(10))+1} linhas")
                else:
                    st.metric("Bronze", "❌ Falhou")
            with col2:
                if pipeline.silver and pipeline.silver.codigo:
                    st.metric("Silver", f"✅ {pipeline.silver.codigo.count(chr(10))+1} linhas")
                else:
                    st.metric("Silver", "❌ Falhou")
            with col3:
                gold_ok = sum(1 for g in pipeline.gold if g.codigo)
                st.metric("Gold", f"✅ {gold_ok} módulos")

            # Mostrar código gerado em expanders
            if pipeline.bronze and pipeline.bronze.codigo:
                with st.expander("📜 Código Bronze gerado"):
                    st.code(pipeline.bronze.codigo, language="python")

            if pipeline.silver and pipeline.silver.codigo:
                with st.expander("📜 Código Silver gerado"):
                    st.code(pipeline.silver.codigo, language="python")

            for gold in pipeline.gold:
                if gold.codigo:
                    with st.expander(f"📜 Código Gold ({gold.camada}) gerado"):
                        st.code(gold.codigo, language="python")

            # Mostrar análise
            if pipeline.analise:
                with st.expander("🔍 Análise da Especificação"):
                    st.json({
                        "kpis_identificados": pipeline.analise.kpis_identificados,
                        "colunas_com_pii": pipeline.analise.colunas_com_pii,
                        "colunas_json": pipeline.analise.colunas_json,
                        "coluna_timestamp": pipeline.analise.coluna_timestamp,
                        "coluna_id": pipeline.analise.coluna_id,
                        "coluna_agrupamento": pipeline.analise.coluna_agrupamento,
                        "modulos_gold": pipeline.analise.modulos_gold_recomendados,
                        "transformacoes": pipeline.analise.transformacoes_sugeridas,
                    })

        except Exception as e:
            st.error(f"Erro na geração de código: {e}")
            import traceback
            st.code(traceback.format_exc())

else:
    st.info("Salve a especificação primeiro (Etapa 1).")


# ─── ETAPA 3: Executar Pipeline ──────────────────────────────────────────────

st.divider()
st.header("Etapa 3: Executar Pipeline")

if (st.session_state.codigo_gerado or _generated_code_exists()) and _spec_exists():
    st.markdown(
        "O pipeline será executado usando o **código gerado pelos agentes**. "
        "Os agentes monitoram a execução e tentam corrigir erros automaticamente."
    )

    # Detectar quais camadas já possuem dados de execuções anteriores
    from core.storage import get_storage_backend
    from config.settings import get_settings as _get_settings
    _backend = get_storage_backend()
    _s = _get_settings()

    _bronze_exists = _backend.table_exists(_s.bronze_path)
    _silver_exists = _backend.table_exists(_s.silver_messages_path) or _backend.table_exists(_s.silver_conversations_path)
    _gold_exists = Path(_s.gold_path).is_dir() and any(Path(_s.gold_path).iterdir()) if Path(_s.gold_path).exists() else False

    col_d1, col_d2, col_d3 = st.columns(3)
    with col_d1:
        st.metric("Bronze", "✅ Dados existentes" if _bronze_exists else "⬜ Sem dados")
    with col_d2:
        st.metric("Silver", "✅ Dados existentes" if _silver_exists else "⬜ Sem dados")
    with col_d3:
        st.metric("Gold", "✅ Dados existentes" if _gold_exists else "⬜ Sem dados")

    # Permitir selecionar quais camadas executar
    camadas_opcoes = ["bronze", "silver", "gold"]
    camadas_default = []
    if not _bronze_exists:
        camadas_default = ["bronze", "silver", "gold"]
    elif not _silver_exists:
        camadas_default = ["silver", "gold"]
    elif not _gold_exists:
        camadas_default = ["gold"]
    else:
        camadas_default = ["bronze", "silver", "gold"]

    camadas_selecionadas = st.multiselect(
        "Camadas a executar",
        options=camadas_opcoes,
        default=camadas_default,
        help="Selecione quais camadas executar. Se os dados já existem de uma execução anterior, você pode pular etapas.",
    )

    if not camadas_selecionadas:
        st.warning("Selecione ao menos uma camada para executar.")

    if camadas_selecionadas and st.button("🚀 Executar Pipeline", type="primary"):
        try:
            from config.settings import get_settings
            from pipeline.specs import carregar_spec
            from pipeline.orchestrator import PipelineOrchestrator

            settings = get_settings()
            spec = carregar_spec(settings.spec_path)

            progress_bar = st.progress(0, text="🚀 Iniciando pipeline...")
            status_text = st.empty()

            layer_labels = {
                "bronze": "🔶 Executando Bronze...",
                "silver": "⬜ Executando Silver...",
                "gold": "🥇 Executando Gold...",
                "concluído": "✅ Pipeline concluído!",
            }

            def _pipeline_progress(step: str, pct: float):
                label = layer_labels.get(step, f"Executando {step}...")
                progress_bar.progress(min(int(pct * 100), 100), text=label)
                status_text.info(label)

            orchestrator = PipelineOrchestrator(spec=spec)
            pipeline_run = orchestrator.run_pipeline(
                layers=camadas_selecionadas,
                progress_callback=_pipeline_progress,
            )

            progress_bar.progress(100, text="✅ Pipeline finalizado!")
            status_text.empty()

            if pipeline_run.status.value == "completed":
                st.success(
                    f"✅ Pipeline concluído em {pipeline_run.duration_sec:.1f}s! "
                    "Acesse o **Pipeline Monitor** para ver os detalhes."
                )
                st.session_state.pipeline_executado = True

                # Mostrar resumo dos steps
                for step in pipeline_run.steps:
                    st.markdown(
                        f"- **{step.step_name.upper()}**: "
                        f"{step.rows_output:,} rows em {step.duration_sec:.1f}s"
                    )
            else:
                erro_msg = next(
                    (s.error_message for s in pipeline_run.steps if s.error_message),
                    "Erro desconhecido",
                )
                st.error(f"❌ Pipeline falhou: {erro_msg}")

                # Oferecer reparo automático
                if st.button("🔧 Tentar Reparar Automaticamente"):
                    _tentar_reparo(pipeline_run)

        except Exception as e:
            st.error(f"Erro ao executar pipeline: {e}")
            import traceback
            st.code(traceback.format_exc())

elif _spec_exists() and not _generated_code_exists():
    st.info("Gere o código do pipeline primeiro (Etapa 2).")
else:
    st.info("Configure e salve a especificação primeiro (Etapa 1).")


# ─── Status atual ─────────────────────────────────────────────────────────────

st.divider()
st.header("Status")

col1, col2, col3 = st.columns(3)
with col1:
    spec_ok = "✅" if _spec_exists() else "❌"
    st.metric("Spec Salva", spec_ok)
with col2:
    code_ok = "✅" if _generated_code_exists() else "❌"
    st.metric("Código Gerado", code_ok)
with col3:
    exec_ok = "✅" if st.session_state.pipeline_executado else "❌"
    st.metric("Pipeline Executado", exec_ok)
