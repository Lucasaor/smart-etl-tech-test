# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "polars",
#   "deltalake",
#   "duckdb",
#   "pyarrow",
#   "litellm",
#   "langgraph",
#   "langchain-core",
#   "langchain-text-splitters",
#   "tenacity",
#   "structlog",
#   "pydantic",
#   "pydantic-settings",
#   "python-dotenv",
# ]
# ///
# MAGIC %md
# MAGIC # 05 — Pipeline Agêntico: ETL Completo Dirigido por LLM
# MAGIC
# MAGIC Este notebook executa o **pipeline agêntico completo** no Databricks Free Edition.
# MAGIC Replica o mesmo fluxo que o Streamlit executa localmente via Docker:
# MAGIC
# MAGIC 1. Carregar arquivos de entrada (amostra parquet, dicionário de dados, descrição dos KPIs)
# MAGIC 2. Analisar os dados e criar um `ProjectSpec`
# MAGIC 3. Usar o `CodeGenAgent` (LLM) para gerar código ETL Bronze / Silver / Gold
# MAGIC 4. Executar o código gerado → gravar tabelas Delta nos UC Volumes
# MAGIC 5. Validar e exibir resultados
# MAGIC
# MAGIC **Arquitetura (Databricks Free Edition — Serverless):**
# MAGIC - Usa `RUNTIME_ENV=databricks` com `DATA_ROOT` apontando para UC Volume (catálogo auto-detectado)
# MAGIC - Toda I/O passa pelo `DatabricksBackend` (PySpark) que grava tabelas Delta nos UC Volumes
# MAGIC - Tabelas Delta gravadas em Volumes são legíveis pelo PySpark no mesmo caminho
# MAGIC - Monitoramento usa tabelas Delta (não SQLite) via `force_delta_monitoring`
# MAGIC - Compute serverless é provisionado automaticamente — sem gerenciamento de clusters
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC 1. Fazer upload dos arquivos de dados para o UC Volume (ver Célula 3 abaixo)
# MAGIC 2. Configurar sua API key do LLM (ver Célula 2 abaixo)
# MAGIC 3. Clonar este repositório no Databricks Repos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar Dependências
# MAGIC
# MAGIC Esta célula instala todos os pacotes Python necessários para o pipeline agêntico.
# MAGIC **Nota:** Isso causa um reinício do interpretador Python no Databricks.

# COMMAND ----------

# MAGIC %pip install polars deltalake duckdb pyarrow litellm langgraph langchain-core langchain-text-splitters tenacity structlog pydantic pydantic-settings python-dotenv

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração do Ambiente
# MAGIC
# MAGIC Configurar ambiente de execução, caminhos e API keys do LLM.
# MAGIC
# MAGIC **IMPORTANTE:** Configure sua API key do LLM abaixo antes de executar.

# COMMAND ----------

import os
import sys
from pathlib import Path

# ── Carregar todas as configs do Databricks Secrets ────────────────────────
# O script push_secrets.py armazena os valores do .env (com RUNTIME_ENV="databricks")
# no scope "pipeline". Carrega todos nas variáveis de ambiente.
SECRET_SCOPE = "pipeline"
try:
    for s in dbutils.secrets.list(SECRET_SCOPE):
        os.environ[s.key] = dbutils.secrets.get(SECRET_SCOPE, s.key)
    print(f"✅ {len(dbutils.secrets.list(SECRET_SCOPE))} secrets carregados do scope '{SECRET_SCOPE}'")
except Exception as e:
    print(f"⚠️  Não foi possível carregar secrets do scope '{SECRET_SCOPE}': {e}")
    print("   Usando configuração manual abaixo.")

# ── Configuração de Runtime ────────────────────────────────────────────────
# Usar DatabricksBackend (PySpark) para gravações Delta — necessário porque UC Volumes
# não suportam operações de rename atômico que a biblioteca deltalake precisa.
os.environ["RUNTIME_ENV"] = "databricks"
os.environ["FORCE_DELTA_MONITORING"] = "true"

# Auto-detectar caminho do Volume: encontrar o catálogo do workspace atual
try:
    catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
    user_catalogs = [c for c in catalogs if c not in ("system", "hive_metastore", "samples") and not c.startswith("__")]
    CATALOG = user_catalogs[0] if user_catalogs else "main"
except Exception:
    CATALOG = "main"

VOLUME_ROOT = f"/Volumes/{CATALOG}/default/pipeline_data"
os.environ["DATA_ROOT"] = VOLUME_ROOT

# ── API Key do LLM (necessário apenas se os secrets não foram carregados acima) ──
# Opção A: Já carregado dos secrets — nada a fazer
# Opção B: Definir diretamente (⚠️ não commite secrets no repo)
# os.environ["ANTHROPIC_API_KEY"] = "sk-..."
# Opção C: Usar widgets do notebook para entrada interativa da key
# dbutils.widgets.text("llm_api_key", "", "LLM API Key")
# os.environ["ANTHROPIC_API_KEY"] = dbutils.widgets.get("llm_api_key")

# ── Caminho do Repositório ─────────────────────────────────────────────────
repo_path = os.getenv("PROJECT_REPO_PATH", "/Workspace/Repos/agentic-pipeline")
# Tentar caminhos comuns do Databricks Repos
for candidate in [
    repo_path,
    "/Workspace/Repos/Lucasaor/smart-etl-tech-test",
    "/Workspace/Users/lucas@dharmadatatech.com/smart-etl-tech-test",
    "/Workspace/Repos/agentic-pipeline",
]:
    if candidate and Path(candidate, "config", "settings.py").exists():
        repo_path = candidate
        break

if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

# ── Limpar singletons em cache para que capturem as env vars acima ─────────
# get_settings() e get_storage_backend() usam @lru_cache — se foram cacheados
# de uma execução anterior (ex: com RUNTIME_ENV=local), limpar agora.
from config.settings import get_settings
from core.storage import get_storage_backend
get_settings.cache_clear()
get_storage_backend.cache_clear()

# Verificar que o backend é DatabricksBackend (PySpark), não LocalDeltaBackend (deltalake lib)
_backend = get_storage_backend()
_backend_name = type(_backend).__name__
assert _backend_name == "DatabricksBackend", (
    f"Expected DatabricksBackend but got {_backend_name}. "
    f"Check RUNTIME_ENV={os.environ.get('RUNTIME_ENV')}"
)

print(f"Repo path:    {repo_path}")
print(f"Catalog:      {CATALOG}")
print(f"VOLUME_ROOT:  {VOLUME_ROOT}")
print(f"DATA_ROOT:    {os.environ['DATA_ROOT']}")
print(f"RUNTIME_ENV:  {os.environ['RUNTIME_ENV']}")
print(f"Backend:      {_backend_name}")
print(f"LLM_MODEL:    {os.environ.get('LLM_MODEL', '(not set)')}")
print(f"API Key set:  {'yes' if os.environ.get('ANTHROPIC_API_KEY') or os.environ.get('OPENAI_API_KEY') else 'NO — set it above'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Preparar Diretórios do UC Volume e Upload dos Arquivos de Entrada
# MAGIC
# MAGIC Criar a estrutura de diretórios necessária nos UC Volumes e fazer upload dos dados de entrada.
# MAGIC
# MAGIC **Você precisa de 3 arquivos de entrada:**
# MAGIC - `conversations_bronze.parquet` — amostra de dados brutos de CRM
# MAGIC - `dicionario_dados.md` — dicionário de dados (descrição das colunas)
# MAGIC - `descricao_kpis.md` — definição dos KPIs (quais análises gerar)

# COMMAND ----------

# Verificar que os diretórios do UC Volume existem (criados por setup_volumes.py)
# Nos UC Volumes, diretórios foram criados durante o setup — apenas verificar.

SUBDIRS = [
    "specs", "specs/generated",
    "bronze", "silver", "silver/messages", "silver/conversations",
    "gold", "monitoring",
]

print(f"Verificando estrutura do UC Volume em {VOLUME_ROOT}:")
all_ok = True
for subdir in SUBDIRS:
    path = Path(VOLUME_ROOT) / subdir
    if path.exists():
        print(f"  ✓ {path}")
    else:
        # Try to create it (os.makedirs works on UC Volumes)
        try:
            path.mkdir(parents=True, exist_ok=True)
            print(f"  ✓ {path} (created)")
        except Exception as e:
            print(f"  ✗ {path} - Erro: {e}")
            all_ok = False

if all_ok:
    print(f"\n✅ Estrutura do UC Volume pronta em {VOLUME_ROOT}")
else:
    print(f"\n⚠️  Alguns diretórios não puderam ser verificados. Execute setup_volumes.py primeiro.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção de Upload A: Pela Interface do Databricks
# MAGIC
# MAGIC 1. Clique em **Catalog** (barra lateral esquerda) → seu catálogo → **default** → volume **pipeline_data**
# MAGIC 2. Faça upload dos 3 arquivos na pasta `specs/`
# MAGIC 3. Ou execute `python databricks/setup_volumes.py` localmente para fazer upload de tudo

# COMMAND ----------

# Se os arquivos já foram enviados pelo setup_volumes.py, nada a fazer aqui.
# Para enviar manualmente pela UI:
# 1. Clique em Catalog → <seu-catálogo> → default → volume pipeline_data
# 2. Navegue até a pasta specs/
# 3. Upload: conversations_bronze.parquet, dicionario_dados.md, descricao_kpis.md

# Listar arquivos atuais em specs/:
try:
    specs = dbutils.fs.ls(f"{VOLUME_ROOT}/specs/")
    print(f"Arquivos em {VOLUME_ROOT}/specs/:")
    for f in specs:
        print(f"  {f.name} ({f.size / 1024:.1f} KB)")
except Exception as e:
    print(f"Não foi possível listar specs: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Opção de Upload B: A Partir de Arquivos do Repositório
# MAGIC
# MAGIC Se os arquivos de entrada já estão no repositório clonado:

# COMMAND ----------

import shutil

# Copiar do repositório para o diretório de trabalho do UC Volume
SPEC_DIR = f"{VOLUME_ROOT}/specs"
repo = Path(repo_path)

files_to_copy = {
    "conversations_bronze.parquet": [
        repo / "conversations_bronze.parquet",
        repo / "data" / "conversations_bronze.parquet",
    ],
    "dicionario_dados.md": [
        repo / "Dicionario_de_Dados.md",
        repo / "data" / "specs" / "dicionario_dados.md",
    ],
    "descricao_kpis.md": [
        repo / "Descricao_KPIs.md",
        repo / "data" / "specs" / "descricao_kpis.md",
    ],
}

for target_name, candidates in files_to_copy.items():
    target = Path(SPEC_DIR) / target_name
    if target.exists():
        print(f"  ✓ {target_name} (já existe)")
        continue
    for src in candidates:
        if src.exists():
            shutil.copy2(str(src), str(target))
            print(f"  ✓ {target_name} ← {src}")
            break
    else:
        print(f"  ✗ {target_name} — não encontrado no repo, faça upload manualmente")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar Arquivos de Entrada

# COMMAND ----------

SPEC_DIR = f"{VOLUME_ROOT}/specs"
required_files = [
    "conversations_bronze.parquet",
    "dicionario_dados.md",
    "descricao_kpis.md",
]

all_ok = True
for fname in required_files:
    fpath = Path(SPEC_DIR) / fname
    if fpath.exists():
        size_kb = fpath.stat().st_size / 1024
        print(f"  ✓ {fname} ({size_kb:.1f} KB)")
    else:
        print(f"  ✗ {fname} — AUSENTE! Faça upload antes de continuar.")
        all_ok = False

if all_ok:
    print("\n✅ Todos os arquivos de entrada presentes. Pronto para prosseguir.")
else:
    print("\n❌ Arquivos ausentes. Use a Opção A ou B acima para enviá-los.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Especificação do Projeto
# MAGIC
# MAGIC Analisar a amostra de dados e criar um `ProjectSpec` — a mesma estrutura
# MAGIC que o frontend Streamlit cria localmente.

# COMMAND ----------

from config.settings import get_settings
from pipeline.specs import ProjectSpec, analisar_amostra, salvar_spec

settings = get_settings()
spec_dir = settings.spec_path

PARQUET_PATH = f"{spec_dir}/conversations_bronze.parquet"
DICT_PATH = f"{spec_dir}/dicionario_dados.md"
KPI_PATH = f"{spec_dir}/descricao_kpis.md"

# Analisar a amostra de dados
print("Analisando amostra de dados...")
analise = analisar_amostra(PARQUET_PATH)
print(f"  Columns: {len(analise.colunas)}")
print(f"  Rows: {analise.num_linhas}")
print(f"  Colunas detectadas:")
for col in analise.colunas:
    print(f"    - {col.nome}: {col.tipo} (nulls={col.nulos_pct:.1f}%, uniques={col.valores_unicos})")

# COMMAND ----------

# Ler arquivos de especificação
dicionario = Path(DICT_PATH).read_text(encoding="utf-8")
descricao_kpis = Path(KPI_PATH).read_text(encoding="utf-8")

print(f"Dicionário de dados: {len(dicionario)} chars")
print(f"Descrição dos KPIs: {len(descricao_kpis)} chars")

# COMMAND ----------

# Criar e salvar ProjectSpec
spec = ProjectSpec(
    nome="crm_pipeline_databricks",
    dados_brutos_path=PARQUET_PATH,
    dicionario_dados=dicionario,
    descricao_kpis=descricao_kpis,
    formato_dados="parquet",
    analise=analise,
)

salvar_spec(spec, spec_dir)
print(f"✅ ProjectSpec salvo em {spec_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gerar Código do Pipeline (LLM)
# MAGIC
# MAGIC O `CodeGenAgent` chama o LLM para gerar dinamicamente código ETL em Python/Polars
# MAGIC para cada camada do pipeline. Esta é a funcionalidade central do sistema agêntico.
# MAGIC
# MAGIC O código gerado segue a assinatura:
# MAGIC ```python
# MAGIC def run(read_table, write_table, settings: dict) -> dict
# MAGIC ```

# COMMAND ----------

from agents.codegen_agent import CodeGenAgent, salvar_pipeline_gerado

print("Inicializando CodeGenAgent...")
agent = CodeGenAgent()

print("Gerando pipeline completo (Bronze → Silver → Gold)...")
print("Isso chama o LLM múltiplas vezes — pode levar 1-3 minutos.\n")

pipeline_gerado = agent.gerar_pipeline_completo(spec)

# COMMAND ----------

# Salvar código gerado no UC Volume
generated_dir = str(Path(spec_dir) / "generated")
file_paths = salvar_pipeline_gerado(pipeline_gerado, generated_dir)

print(f"✅ Pipeline gerado com {len(file_paths)} arquivos:\n")
for key, path in file_paths.items():
    print(f"  {key}: {path}")

# Exibir resumo da geração
if pipeline_gerado.analise:
    a = pipeline_gerado.analise
    print(f"\nAnálise da Spec:")
    print(f"  Colunas identificadas: {len(a.colunas_identificadas)}")
    print(f"  KPIs identificados: {len(a.kpis_identificados)}")
    print(f"  Módulos Gold: {a.modulos_gold_recomendados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Opcional) Inspecionar Código Gerado
# MAGIC
# MAGIC Visualizar o código gerado pelo LLM para cada camada.

# COMMAND ----------

# Código Bronze
if pipeline_gerado.bronze and pipeline_gerado.bronze.codigo:
    print(f"═══ Bronze ({pipeline_gerado.bronze.codigo.count(chr(10))+1} lines) ═══")
    print(pipeline_gerado.bronze.codigo[:2000])
    if len(pipeline_gerado.bronze.codigo) > 2000:
        print(f"\n... ({len(pipeline_gerado.bronze.codigo)} total chars)")

# COMMAND ----------

# Código Silver
if pipeline_gerado.silver and pipeline_gerado.silver.codigo:
    print(f"═══ Silver ({pipeline_gerado.silver.codigo.count(chr(10))+1} lines) ═══")
    print(pipeline_gerado.silver.codigo[:2000])
    if len(pipeline_gerado.silver.codigo) > 2000:
        print(f"\n... ({len(pipeline_gerado.silver.codigo)} total chars)")

# COMMAND ----------

# Módulos Gold
for gold in pipeline_gerado.gold:
    if gold.codigo:
        print(f"═══ Gold: {gold.camada} ({gold.codigo.count(chr(10))+1} lines) ═══")
        print(gold.codigo[:1000])
        if len(gold.codigo) > 1000:
            print(f"\n... ({len(gold.codigo)} total chars)")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Executar Pipeline
# MAGIC
# MAGIC Executar o código gerado através do `PipelineOrchestrator`.
# MAGIC Executa Bronze → Silver → Gold sequencialmente, gravando tabelas Delta
# MAGIC nos UC Volumes. Falhas acionam o `RepairAgent` para recuperação automática.

# COMMAND ----------

from pipeline.orchestrator import PipelineOrchestrator

print("Executando pipeline...\n")
orch = PipelineOrchestrator(spec=spec)
run = orch.run_pipeline(
    layers=["bronze", "silver", "gold"],
    trigger="databricks_agentic_notebook",
)

print(f"\n{'='*60}")
print(f"Pipeline Run: {run.run_id}")
print(f"Status: {run.status.value}")
print(f"Duration: {run.duration_sec:.1f}s" if run.duration_sec else "")
print(f"{'='*60}")

for step in run.steps:
    status_icon = "✅" if step.status.value == "completed" else "❌"
    print(f"  {status_icon} {step.step_name}: {step.status.value}")
    print(f"     Rows: {step.rows_output} | Duration: {step.duration_sec:.1f}s" if step.duration_sec else f"     Rows: {step.rows_output}")
    if step.error_message:
        print(f"     Error: {step.error_message[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validar Resultados
# MAGIC
# MAGIC Verificar que as tabelas Delta foram criadas e contêm dados.

# COMMAND ----------

from core.storage import get_storage_backend

backend = get_storage_backend()

# Verificar todas as tabelas esperadas
tables_to_check = {
    "Bronze": settings.bronze_path,
    "Silver Messages": settings.silver_messages_path,
    "Silver Conversations": settings.silver_conversations_path,
}

# Adicionar tabelas Gold dinamicamente
gold_base = Path(settings.gold_path)
if gold_base.exists():
    for sub in sorted(gold_base.iterdir()):
        if sub.is_dir() and not sub.name.startswith("."):
            tables_to_check[f"Gold: {sub.name}"] = str(sub)

print("Validação das Tabelas Delta:")
print(f"{'Tabela':<30} {'Existe':<8} {'Linhas':<10} {'Versão'}")
print("-" * 60)

for label, path in tables_to_check.items():
    try:
        if backend.table_exists(path):
            rows = backend.get_table_row_count(path)
            version = backend.get_table_version(path)
            status = "✅"
            print(f"{label:<30} {status:<8} {rows:<10} v{version}")
        else:
            print(f"{label:<30} {'❌':<8} {'—':<10} —")
    except Exception as e:
        print(f"{label:<30} {'⚠️':<8} Error: {str(e)[:40]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Prévia dos Dados com PySpark
# MAGIC
# MAGIC As tabelas Delta gravadas pela biblioteca `deltalake` nos UC Volumes
# MAGIC são legíveis pelo PySpark no mesmo caminho.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Bronze
try:
    df_bronze = spark.read.format("delta").load(f"{VOLUME_ROOT}/bronze")
    print(f"=== Bronze: {df_bronze.count()} rows ===")
    df_bronze.show(5, truncate=40)
except Exception as e:
    print(f"Bronze não disponível: {e}")

# COMMAND ----------

# Silver Conversations
try:
    df_silver = spark.read.format("delta").load(f"{VOLUME_ROOT}/silver/conversations")
    print(f"=== Silver Conversations: {df_silver.count()} rows ===")
    df_silver.show(5, truncate=40)
except Exception as e:
    print(f"Silver conversations não disponível: {e}")

# COMMAND ----------

# Tabelas Gold
import os as _os

gold_dir = f"{VOLUME_ROOT}/gold"
try:
    gold_tables = [
        item.name for item in _os.scandir(gold_dir)
        if item.is_dir() and not item.name.startswith((".", "_"))
    ]
    for table_name in sorted(gold_tables):
        try:
            df_gold = spark.read.format("delta").load(f"{gold_dir}/{table_name}")
            print(f"=== Gold: {table_name} ({df_gold.count()} rows) ===")
            df_gold.show(3, truncate=40)
            print()
        except Exception as e:
            print(f"Gold {table_name}: {e}\n")
except Exception as e:
    print(f"Diretório Gold não acessível: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo e Próximos Passos
# MAGIC
# MAGIC ### O que aconteceu:
# MAGIC 1. **Análise da Spec**: A amostra de dados foi analisada (tipos de coluna, nulos, valores únicos)
# MAGIC 2. **Geração de Código**: O LLM gerou código ETL para as camadas Bronze/Silver/Gold
# MAGIC 3. **Execução**: O código gerado foi executado, gravando tabelas Delta nos UC Volumes
# MAGIC 4. **Auto-Reparo**: Falhas foram automaticamente diagnosticadas e reparadas pelo RepairAgent
# MAGIC
# MAGIC ### Acessando os resultados:
# MAGIC - **PySpark**: `spark.read.format("delta").load(f"{VOLUME_ROOT}/bronze")`
# MAGIC - **deltalake**: `DeltaTable(f"{VOLUME_ROOT}/bronze")`
# MAGIC - **dbutils.fs**: `dbutils.fs.ls(f"{VOLUME_ROOT}/")`
# MAGIC
# MAGIC ### Re-executando:
# MAGIC - Para regenerar o código: delete `{VOLUME_ROOT}/specs/generated/pipeline_meta.json` e re-execute a partir da Célula 5
# MAGIC - Para re-executar com código existente: re-execute a partir da Célula 6
# MAGIC - Para usar dados diferentes: substitua os arquivos em `{VOLUME_ROOT}/specs/` e re-execute a partir da Célula 4
# MAGIC
# MAGIC ### Limpeza:
# MAGIC ```python
# MAGIC dbutils.fs.rm(f"{VOLUME_ROOT}/", recurse=True)
# MAGIC ```
