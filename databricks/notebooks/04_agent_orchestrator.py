# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Orquestrador de Agentes
# MAGIC
# MAGIC Notebook responsável pela orquestração dos agentes de IA no Databricks.
# MAGIC Executa o pipeline completo com monitoramento e auto-correção.
# MAGIC
# MAGIC **Compatível com Databricks Free Edition (serverless compute).**
# MAGIC
# MAGIC **Modos de execução:**
# MAGIC - **Manual**: executar célula por célula
# MAGIC - **Job**: agendar via Databricks Jobs (disponível na Free Edition)
# MAGIC
# MAGIC **Nota:** Serverless compute é provisionado automaticamente — sem gerenciar clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

import os
import sys
import time
from datetime import datetime

repo_path = os.getenv("PROJECT_REPO_PATH", "/Workspace/Repos/agentic-pipeline")
if os.path.exists(repo_path):
    sys.path.insert(0, repo_path)

from config.databricks_notebook import (
    bootstrap_notebook, default_paths, notebook_workspace_path,
    parse_notebook_result, save_run_metadata,
)

boot = bootstrap_notebook(explicit_repo_path=repo_path)
paths = default_paths(boot["data_root"])
repo_path = boot["repo_path"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução Completa do Pipeline (Bronze → Silver → Gold)
# MAGIC
# MAGIC Executa todos os notebooks em sequência, rastreando status e tempo.

# COMMAND ----------

def executar_notebook(path, timeout=600, params=None):
    """Executa um notebook e retorna resultado com métricas.

    Args:
        path: Caminho do notebook no workspace.
        timeout: Timeout em segundos (padrão: 10 min).
        params: Parâmetros opcionais para o notebook.

    Returns:
        Dicionário com status, duração e resultado estruturado.
    """
    inicio = time.time()
    status = "sucesso"
    resultado = None
    erro = None

    try:
        raw_result = dbutils.notebook.run(path, timeout, params or {})
        resultado = parse_notebook_result(raw_result)
    except Exception as e:
        status = "falha"
        erro = str(e)
        resultado = {"status": "falha", "error": str(e)}

    duracao = time.time() - inicio

    return {
        "notebook": path,
        "status": status,
        "duracao_sec": round(duracao, 1),
        "resultado": resultado,
        "erro": erro,
        "timestamp": datetime.now().isoformat(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Executar Pipeline Completo

# COMMAND ----------

# Definir notebooks do pipeline
PIPELINE_NOTEBOOKS = [
    notebook_workspace_path(repo_path, "01_bronze.py"),
    notebook_workspace_path(repo_path, "02_silver.py"),
    notebook_workspace_path(repo_path, "03_gold.py"),
]

print(f"=== Pipeline Iniciado: {datetime.now().isoformat()} ===\n")

resultados = []
pipeline_ok = True

for nb_path in PIPELINE_NOTEBOOKS:
    nome = nb_path.split("/")[-1]
    print(f"▶ Executando: {nome}...")

    resultado = executar_notebook(nb_path, timeout=1200)
    resultados.append(resultado)

    if resultado["status"] == "sucesso":
        print(f"  ✅ {nome} — {resultado['duracao_sec']}s")
    else:
        print(f"  ❌ {nome} — FALHA: {resultado['erro']}")
        pipeline_ok = False
        break  # Parar na primeira falha

print(f"\n=== Pipeline {'Completo ✅' if pipeline_ok else 'Falhou ❌'} ===")
print(f"Total de notebooks executados: {len(resultados)}")
print(f"Duração total: {sum(r['duracao_sec'] for r in resultados):.1f}s")

# Persist pipeline-level run metadata
_total_dur = sum(r["duracao_sec"] for r in resultados)
_total_rows = sum(
    (r.get("resultado") or {}).get("rows_written", 0)
    for r in resultados
    if isinstance(r.get("resultado"), dict)
)
save_run_metadata(
    boot["data_root"],
    "04_orchestrator",
    "sucesso" if pipeline_ok else "falha",
    _total_dur,
    rows_written=_total_rows,
    error=resultados[-1].get("erro") if not pipeline_ok else None,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo da Execução

# COMMAND ----------

# Tabela resumo
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("notebook", StringType(), True),
    StructField("status", StringType(), True),
    StructField("duracao_sec", DoubleType(), True),
    StructField("erro", StringType(), True),
    StructField("timestamp", StringType(), True),
])

df_resumo = spark.createDataFrame(
    [(r["notebook"].split("/")[-1], r["status"], r["duracao_sec"], r["erro"], r["timestamp"]) for r in resultados],
    schema=schema,
)
display(df_resumo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação de Saúde das Tabelas

# COMMAND ----------

TABELAS = {
    "Bronze": paths["bronze"],
    "Silver Messages": paths["silver_messages"],
    "Silver Conversations": paths["silver_conversations"],
    "Gold Sentimento": paths["gold_sentiment"],
    "Gold Personas": paths["gold_personas"],
    "Gold Segmentação": paths["gold_segmentation"],
    "Gold Analytics": paths["gold_analytics"],
    "Gold Vendedores": paths["gold_vendor_analysis"],
}

print("=== Verificação de Saúde ===\n")
for nome, path in TABELAS.items():
    try:
        df = spark.read.format("delta").load(path)
        linhas = df.count()
        colunas = len(df.columns)
        # Get version via SQL (Spark Connect compatible — no DeltaTable API)
        hist = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1")
        version = hist.select("version").collect()[0][0]
        print(f"  ✅ {nome}: {linhas:,} linhas, {colunas} colunas (v{version})")
    except Exception as e:
        print(f"  ⚠️  {nome}: não encontrada ou erro - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução Periódica (via Jobs)
# MAGIC
# MAGIC Na Free Edition, você pode agendar este notebook como um Job
# MAGIC (até 5 Jobs concorrentes). Serverless compute é provisionado automaticamente.
# MAGIC
# MAGIC **Para agendar:**
# MAGIC 1. Workflows → Create Job
# MAGIC 2. Selecionar este notebook
# MAGIC 3. Definir schedule (ex: diário)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecção de Novos Dados

# COMMAND ----------

def verificar_novos_dados():
    """Verifica se há novos dados na camada Bronze que ainda não foram processados."""
    try:
        bronze_path = paths["bronze"]
        silver_path = paths["silver_messages"]

        # Check if Bronze exists (Spark Connect compatible)
        try:
            df_bronze = spark.read.format("delta").load(bronze_path)
            count_bronze = df_bronze.count()
        except Exception:
            return {"novos_dados": False, "motivo": "Tabela Bronze não existe"}

        # Check if Silver exists
        try:
            df_silver = spark.read.format("delta").load(silver_path)
            count_silver = df_silver.count()
        except Exception:
            return {"novos_dados": True, "motivo": f"Silver vazia, Bronze tem {count_bronze} linhas"}

        if count_bronze > count_silver:
            return {
                "novos_dados": True,
                "motivo": f"Bronze ({count_bronze}) > Silver ({count_silver})",
                "diferenca": count_bronze - count_silver,
            }

        return {"novos_dados": False, "motivo": "Dados sincronizados"}

    except Exception as e:
        return {"novos_dados": False, "motivo": f"Erro: {e}"}

resultado_check = verificar_novos_dados()
print(f"Novos dados: {resultado_check['novos_dados']}")
print(f"Motivo: {resultado_check['motivo']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Re-executar Pipeline se houver novos dados

# COMMAND ----------

if resultado_check.get("novos_dados"):
    print("Novos dados detectados! Executando pipeline Silver → Gold...")

    # Apenas Silver e Gold (Bronze já ingerida)
    for nb in PIPELINE_NOTEBOOKS[1:]:  # Pular Bronze
        nome = nb.split("/")[-1]
        print(f"▶ Executando: {nome}...")
        res = executar_notebook(nb, timeout=1200)
        if res["status"] == "sucesso":
            print(f"  ✅ {nome} — {res['duracao_sec']}s")
        else:
            print(f"  ❌ {nome} — FALHA: {res['erro']}")
            break
else:
    print("Nenhum dado novo detectado. Pipeline não precisa ser re-executado.")
