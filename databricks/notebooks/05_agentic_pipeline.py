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
# MAGIC # 05 — Agentic Pipeline: Full LLM-Driven ETL
# MAGIC
# MAGIC This notebook runs the **complete agentic pipeline** on Databricks Free Edition.
# MAGIC It replicates the same flow that Streamlit runs locally via Docker:
# MAGIC
# MAGIC 1. Load input files (parquet data sample, data dictionary, KPI description)
# MAGIC 2. Analyze the data and create a `ProjectSpec`
# MAGIC 3. Use `CodeGenAgent` (LLM) to generate Bronze / Silver / Gold ETL code
# MAGIC 4. Execute the generated code → write Delta tables to UC Volumes
# MAGIC 5. Validate and display results
# MAGIC
# MAGIC **Architecture (Databricks Free Edition — Serverless):**
# MAGIC - Uses `RUNTIME_ENV=local` with `DATA_ROOT=/Volumes/main/default/pipeline_data`
# MAGIC - All I/O goes through `LocalDeltaBackend` (deltalake library) via UC Volume POSIX paths
# MAGIC - Delta tables written to Volumes are readable by PySpark at the same path
# MAGIC - Monitoring uses Delta tables (not SQLite) via `force_delta_monitoring`
# MAGIC - Serverless compute is provisioned automatically — no cluster management needed
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC 1. Upload your data files to UC Volume (see Cell 3 below)
# MAGIC 2. Set your LLM API key (see Cell 2 below)
# MAGIC 3. Clone this repository to Databricks Repos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies
# MAGIC
# MAGIC This cell installs all Python packages required by the agentic pipeline.
# MAGIC **Note:** This triggers a Python interpreter restart on Databricks.

# COMMAND ----------

# MAGIC %pip install polars deltalake duckdb pyarrow litellm langgraph langchain-core langchain-text-splitters tenacity structlog pydantic pydantic-settings python-dotenv

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Environment Setup
# MAGIC
# MAGIC Configure runtime environment, paths, and LLM API keys.
# MAGIC
# MAGIC **IMPORTANT:** Set your LLM API key below before running.

# COMMAND ----------

import os
import sys
from pathlib import Path

# ── Runtime Configuration ──────────────────────────────────────────────────
# Use LocalDeltaBackend (deltalake library) with UC Volume paths.
# This avoids PySpark for data processing while still writing to Volumes.
os.environ["RUNTIME_ENV"] = "local"
os.environ["DATA_ROOT"] = "/Volumes/main/default/pipeline_data"
os.environ["FORCE_DELTA_MONITORING"] = "true"

# ── LLM API Key ───────────────────────────────────────────────────────────
# Option A: Set directly (⚠️ don't commit secrets to repos)
# os.environ["OPENAI_API_KEY"] = "sk-..."

# Option B: Use Databricks secrets (recommended for Free Edition)
# os.environ["OPENAI_API_KEY"] = dbutils.secrets.get("llm", "openai_api_key")

# Option C: Use Anthropic instead
# os.environ["ANTHROPIC_API_KEY"] = dbutils.secrets.get("llm", "anthropic_api_key")
# os.environ["LLM_MODEL"] = "anthropic/claude-sonnet-4-20250514"

# Option D: Use notebook widgets for interactive key entry
# dbutils.widgets.text("llm_api_key", "", "LLM API Key")
# os.environ["OPENAI_API_KEY"] = dbutils.widgets.get("llm_api_key")

# ── Repository Path ────────────────────────────────────────────────────────
repo_path = os.getenv("PROJECT_REPO_PATH", "/Workspace/Repos/agentic-pipeline")
# Try common Databricks Repos paths
for candidate in [
    repo_path,
    "/Workspace/Repos/Lucasaor/smart-etl-tech-test",
    "/Workspace/Repos/agentic-pipeline",
]:
    if candidate and Path(candidate, "config", "settings.py").exists():
        repo_path = candidate
        break

if repo_path not in sys.path:
    sys.path.insert(0, repo_path)

print(f"Repo path: {repo_path}")
print(f"DATA_ROOT: {os.environ['DATA_ROOT']}")
print(f"RUNTIME_ENV: {os.environ['RUNTIME_ENV']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prepare UC Volume Directories & Upload Input Files
# MAGIC
# MAGIC Create the required directory structure on UC Volumes and upload your input data.
# MAGIC
# MAGIC **You need 3 input files:**
# MAGIC - `conversations_bronze.parquet` — raw CRM data sample
# MAGIC - `dicionario_dados.md` — data dictionary (column descriptions)
# MAGIC - `descricao_kpis.md` — KPI definitions (what analytics to generate)

# COMMAND ----------

# Create DBFS directory structure using dbutils.fs (required for serverless)
# Note: Serverless compute has public DBFS root disabled. 
# Use Volumes path or FileStore instead.

# Update DBFS_ROOT to use a path compatible with serverless
DBFS_ROOT = "dbfs:/FileStore/delta"  # Use FileStore instead of root DBFS
SUBDIRS = [
    "specs", "specs/generated", "specs/generated/tests",
    "bronze", "silver", "silver/messages", "silver/conversations",
    "gold", "monitoring",
]

for subdir in SUBDIRS:
    path = f"{DBFS_ROOT}/{subdir}"
    try:
        dbutils.fs.mkdirs(path)
        print(f"  ✓ {path}")
    except Exception as e:
        print(f"  ✗ {path} - Error: {e}")

print(f"\nDBFS directories ready at {DBFS_ROOT}/")
print(f"\n⚠️  Note: Updated DATA_ROOT to use FileStore (compatible with serverless)")
print("Update Cell 5 to set: os.environ['DATA_ROOT'] = '/dbfs/FileStore/delta'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload Option A: From Databricks UI
# MAGIC
# MAGIC 1. Click **Catalog** (left sidebar) → **main** → **default** → **pipeline_data** volume
# MAGIC 2. Upload your 3 files to the `specs/` folder
# MAGIC 3. Or use dbutils.fs.cp from another location

# COMMAND ----------

# Uncomment and adjust paths after uploading via UI:

# dbutils.fs.cp(
#     "dbfs:/FileStore/conversations_bronze.parquet",
#     f"{VOLUME_ROOT}/specs/conversations_bronze.parquet",
# )
# dbutils.fs.cp(
#     "dbfs:/FileStore/dicionario_dados.md",
#     f"{VOLUME_ROOT}/specs/dicionario_dados.md",
# )
# dbutils.fs.cp(
#     "dbfs:/FileStore/descricao_kpis.md",
#     f"{VOLUME_ROOT}/specs/descricao_kpis.md",
# )
# print(f"Files copied to {VOLUME_ROOT}/specs/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload Option B: From Repository Files
# MAGIC
# MAGIC If the input files are already in the cloned repository:

# COMMAND ----------

import shutil

# Copy from repository to UC Volume working directory
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
        print(f"  ✓ {target_name} (already exists)")
        continue
    for src in candidates:
        if src.exists():
            shutil.copy2(str(src), str(target))
            print(f"  ✓ {target_name} ← {src}")
            break
    else:
        print(f"  ✗ {target_name} — not found in repo, upload manually")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Input Files

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
        print(f"  ✗ {fname} — MISSING! Upload before continuing.")
        all_ok = False

if all_ok:
    print("\n✅ All input files present. Ready to proceed.")
else:
    print("\n❌ Missing files. Use Option A or B above to upload them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Project Specification
# MAGIC
# MAGIC Analyze the data sample and create a `ProjectSpec` — the same structure
# MAGIC the Streamlit frontend creates locally.

# COMMAND ----------

from config.settings import get_settings
from pipeline.specs import ProjectSpec, analisar_amostra, salvar_spec

settings = get_settings()
spec_dir = settings.spec_path

PARQUET_PATH = f"{spec_dir}/conversations_bronze.parquet"
DICT_PATH = f"{spec_dir}/dicionario_dados.md"
KPI_PATH = f"{spec_dir}/descricao_kpis.md"

# Analyze the data sample
print("Analyzing data sample...")
analise = analisar_amostra(PARQUET_PATH)
print(f"  Columns: {len(analise.colunas)}")
print(f"  Rows: {analise.total_linhas}")
print(f"  Columns detected:")
for col in analise.colunas:
    print(f"    - {col.nome}: {col.tipo} (nulls={col.nulos_pct:.1f}%, uniques={col.valores_unicos})")

# COMMAND ----------

# Read specification files
dicionario = Path(DICT_PATH).read_text(encoding="utf-8")
descricao_kpis = Path(KPI_PATH).read_text(encoding="utf-8")

print(f"Data dictionary: {len(dicionario)} chars")
print(f"KPI description: {len(descricao_kpis)} chars")

# COMMAND ----------

# Create and save ProjectSpec
spec = ProjectSpec(
    nome="crm_pipeline_databricks",
    dados_brutos_path=PARQUET_PATH,
    dicionario_dados=dicionario,
    descricao_kpis=descricao_kpis,
    formato_dados="parquet",
    analise=analise,
)

salvar_spec(spec, spec_dir)
print(f"✅ ProjectSpec saved to {spec_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Pipeline Code (LLM)
# MAGIC
# MAGIC The `CodeGenAgent` calls the LLM to dynamically generate Python/Polars
# MAGIC ETL code for each pipeline layer. This is the core agentic feature.
# MAGIC
# MAGIC The generated code follows the signature:
# MAGIC ```python
# MAGIC def run(read_table, write_table, settings: dict) -> dict
# MAGIC ```

# COMMAND ----------

from agents.codegen_agent import CodeGenAgent, salvar_pipeline_gerado

print("Initializing CodeGenAgent...")
agent = CodeGenAgent()

print("Generating full pipeline (Bronze → Silver → Gold)...")
print("This calls the LLM multiple times — may take 1-3 minutes.\n")

pipeline_gerado = agent.gerar_pipeline_completo(spec)

# COMMAND ----------

# Save generated code to UC Volume
generated_dir = str(Path(spec_dir) / "generated")
file_paths = salvar_pipeline_gerado(pipeline_gerado, generated_dir)

print(f"✅ Pipeline generated with {len(file_paths)} files:\n")
for key, path in file_paths.items():
    print(f"  {key}: {path}")

# Show generation summary
if pipeline_gerado.analise:
    a = pipeline_gerado.analise
    print(f"\nSpec Analysis:")
    print(f"  Columns identified: {len(a.colunas_identificadas)}")
    print(f"  KPIs identified: {len(a.kpis_identificados)}")
    print(f"  Gold modules: {a.modulos_gold_recomendados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### (Optional) Inspect Generated Code
# MAGIC
# MAGIC View the LLM-generated code for each layer.

# COMMAND ----------

# Bronze code
if pipeline_gerado.bronze and pipeline_gerado.bronze.codigo:
    print(f"═══ Bronze ({pipeline_gerado.bronze.codigo.count(chr(10))+1} lines) ═══")
    print(pipeline_gerado.bronze.codigo[:2000])
    if len(pipeline_gerado.bronze.codigo) > 2000:
        print(f"\n... ({len(pipeline_gerado.bronze.codigo)} total chars)")

# COMMAND ----------

# Silver code
if pipeline_gerado.silver and pipeline_gerado.silver.codigo:
    print(f"═══ Silver ({pipeline_gerado.silver.codigo.count(chr(10))+1} lines) ═══")
    print(pipeline_gerado.silver.codigo[:2000])
    if len(pipeline_gerado.silver.codigo) > 2000:
        print(f"\n... ({len(pipeline_gerado.silver.codigo)} total chars)")

# COMMAND ----------

# Gold modules
for gold in pipeline_gerado.gold:
    if gold.codigo:
        print(f"═══ Gold: {gold.camada} ({gold.codigo.count(chr(10))+1} lines) ═══")
        print(gold.codigo[:1000])
        if len(gold.codigo) > 1000:
            print(f"\n... ({len(gold.codigo)} total chars)")
        print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execute Pipeline
# MAGIC
# MAGIC Run the generated code through the `PipelineOrchestrator`.
# MAGIC This executes Bronze → Silver → Gold sequentially, writing Delta tables
# MAGIC to UC Volumes. Failures trigger the `RepairAgent` for automatic recovery.

# COMMAND ----------

from pipeline.orchestrator import PipelineOrchestrator

print("Executing pipeline...\n")
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
# MAGIC ## 7. Validate Results
# MAGIC
# MAGIC Verify that Delta tables were created and contain data.

# COMMAND ----------

from core.storage import get_storage_backend

backend = get_storage_backend()

# Check all expected tables
tables_to_check = {
    "Bronze": settings.bronze_path,
    "Silver Messages": settings.silver_messages_path,
    "Silver Conversations": settings.silver_conversations_path,
}

# Add Gold tables dynamically
gold_base = Path(settings.gold_path)
if gold_base.exists():
    for sub in sorted(gold_base.iterdir()):
        if sub.is_dir() and not sub.name.startswith("."):
            tables_to_check[f"Gold: {sub.name}"] = str(sub)

print("Delta Table Validation:")
print(f"{'Table':<30} {'Exists':<8} {'Rows':<10} {'Version'}")
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
# MAGIC ## 8. Preview Data with PySpark
# MAGIC
# MAGIC The Delta tables written by the `deltalake` library to UC Volumes
# MAGIC are readable by PySpark at the same path.

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
VOLUME_ROOT = "/Volumes/main/default/pipeline_data"

# Bronze
try:
    df_bronze = spark.read.format("delta").load(f"{VOLUME_ROOT}/bronze")
    print(f"=== Bronze: {df_bronze.count()} rows ===")
    df_bronze.show(5, truncate=40)
except Exception as e:
    print(f"Bronze not available: {e}")

# COMMAND ----------

# Silver Conversations
try:
    df_silver = spark.read.format("delta").load(f"{VOLUME_ROOT}/silver/conversations")
    print(f"=== Silver Conversations: {df_silver.count()} rows ===")
    df_silver.show(5, truncate=40)
except Exception as e:
    print(f"Silver conversations not available: {e}")

# COMMAND ----------

# Gold tables
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
    print(f"Gold directory not accessible: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary & Next Steps
# MAGIC
# MAGIC ### What happened:
# MAGIC 1. **Spec Analysis**: The data sample was analyzed (column types, nulls, uniques)
# MAGIC 2. **Code Generation**: The LLM generated ETL code for Bronze/Silver/Gold layers
# MAGIC 3. **Execution**: Generated code was executed, writing Delta tables to UC Volumes
# MAGIC 4. **Auto-Repair**: Any failures were automatically diagnosed and repaired by the RepairAgent
# MAGIC
# MAGIC ### Accessing results:
# MAGIC - **PySpark**: `spark.read.format("delta").load("/Volumes/main/default/pipeline_data/bronze")`
# MAGIC - **deltalake**: `DeltaTable("/Volumes/main/default/pipeline_data/bronze")`
# MAGIC - **dbutils.fs**: `dbutils.fs.ls("/Volumes/main/default/pipeline_data/")`
# MAGIC
# MAGIC ### Re-running:
# MAGIC - To regenerate code: delete `/Volumes/main/default/pipeline_data/specs/generated/pipeline_meta.json` and re-run from Cell 5
# MAGIC - To re-execute with existing code: re-run from Cell 6
# MAGIC - To use different data: replace files in `/Volumes/main/default/pipeline_data/specs/` and re-run from Cell 4
# MAGIC
# MAGIC ### Cleanup:
# MAGIC ```python
# MAGIC dbutils.fs.rm("/Volumes/main/default/pipeline_data/", recurse=True)
# MAGIC ```
