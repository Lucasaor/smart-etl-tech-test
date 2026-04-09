"""Página de Configuração — upload de dados, dicionário e KPIs.

Interface para receber as três entradas obrigatórias do pipeline:
  1. Amostra de dados brutos (parquet/csv)
  2. Dicionário de dados (markdown)
  3. Descrição dos KPIs desejados (markdown)

Após o envio, o sistema analisa os dados e salva a especificação
para que os agentes possam gerar o pipeline.
"""

import shutil
from pathlib import Path

import polars as pl
import streamlit as st

st.set_page_config(page_title="Configuração", page_icon="⚙️", layout="wide")
st.title("⚙️ Configuração do Projeto")

st.markdown(
    """
    Configure o pipeline enviando suas fontes de dados e descrições.
    Os agentes usarão estas informações para gerar automaticamente
    os scripts de transformação em cada camada.
    """
)


# ─── Estado da sessão ─────────────────────────────────────────────────────────

if "spec_salva" not in st.session_state:
    st.session_state.spec_salva = False


# ─── 1. Upload de dados brutos ────────────────────────────────────────────────

st.header("1. Amostra de Dados Brutos")
st.markdown("Envie o arquivo com os dados que serão processados na camada **Bronze**.")

arquivo_dados = st.file_uploader(
    "Selecione o arquivo (parquet ou csv)",
    type=["parquet", "csv"],
    help="Arquivo com os dados brutos. Suporta Parquet e CSV.",
)

if arquivo_dados is not None:
    # Preview dos dados
    st.success(f"Arquivo recebido: **{arquivo_dados.name}** ({arquivo_dados.size / 1024:.1f} KB)")

    try:
        if arquivo_dados.name.endswith(".csv"):
            df_preview = pl.read_csv(arquivo_dados, n_rows=10)
        else:
            import tempfile
            import os

            # Salvar temporariamente para leitura com Polars
            tmp_dir = tempfile.mkdtemp()
            tmp_path = os.path.join(tmp_dir, arquivo_dados.name)
            with open(tmp_path, "wb") as f:
                f.write(arquivo_dados.getvalue())
            df_preview = pl.read_parquet(tmp_path).head(10)

        st.subheader("Preview dos dados")
        st.dataframe(df_preview.to_pandas(), use_container_width=True)

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
    "Os agentes usarão esta descrição para gerar os scripts de analytics."
)

kpis_exemplo = """## KPIs Desejados

### Análise de Sentimento
- Classificar cada conversa como positiva, neutra ou negativa
- Score de sentimento por campanha e por vendedor

### Personas de Leads
- Classificar leads em: Pesquisador, Decidido, Negociador, Fantasma, Indeciso
- Baseado em padrões de comportamento conversacional

### Segmentação de Audiência
- Por veículo (popular/médio/premium)
- Por nível de engajamento (frio/morno/quente)
- Por origem (campanha + lead_source)
- Por geografia (cidade/estado)

### Métricas de Performance
- Taxa de conversão por vendedor
- Tempo médio de resposta por vendedor
- Provedores de e-mail mais usados pelos leads
- Funil de conversão: contato → cotação → proposta → venda
- Análise de concorrentes mencionados
"""

kpis_texto = st.text_area(
    "Descrição dos KPIs (markdown)",
    value=kpis_exemplo,
    height=300,
    help="Descreva os indicadores e análises desejados para a camada Gold.",
)


# ─── Salvar especificação ─────────────────────────────────────────────────────

st.divider()

if st.button("💾 Salvar Especificação e Iniciar Pipeline", type="primary"):
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

        except Exception as e:
            st.error(f"Erro ao salvar especificação: {e}")

if st.session_state.spec_salva:
    st.info("Especificação salva. Acesse o **Pipeline Monitor** para acompanhar a execução.")
