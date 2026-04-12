# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Camada Bronze: Ingestão de Dados Brutos
# MAGIC
# MAGIC Notebook responsável pela ingestão dos dados brutos para a camada Bronze.
# MAGIC Lê arquivos Parquet do DBFS, valida o schema conforme o dicionário de dados,
# MAGIC expande colunas JSON e escreve como tabela Delta.
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Arquivo de dados brutos em `/mnt/delta/specs/` ou DBFS
# MAGIC - Executar `setup_dbfs.py` antes da primeira execução

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Ambiente

# COMMAND ----------

import os
import sys

repo_path = os.getenv("PROJECT_REPO_PATH", "/Workspace/Repos/agentic-pipeline")
if os.path.exists(repo_path):
    sys.path.insert(0, repo_path)

from config.databricks_notebook import (
    bootstrap_notebook, default_paths, make_notebook_result, save_run_metadata,
)

boot = bootstrap_notebook(explicit_repo_path=repo_path)
paths = default_paths(boot["data_root"])
_nb_start = __import__("time").time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importações e Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, BooleanType,
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Paths
BRONZE_SOURCE = f"{paths['specs']}/conversations_bronze.parquet"
BRONZE_OUTPUT = paths["bronze"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Esquema Esperado (validação)

# COMMAND ----------

COLUNAS_OBRIGATORIAS = {
    "conversation_id", "message_id", "timestamp", "direction",
    "sender_name", "sender_phone", "message_type", "message_body",
    "message_status", "channel", "campaign_id", "agent_id",
    "lead_source", "metadata",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão

# COMMAND ----------

# Ler dados brutos
print(f"Lendo dados de: {BRONZE_SOURCE}")
df_raw = spark.read.parquet(BRONZE_SOURCE)

# Validar schema
colunas_presentes = set(df_raw.columns)
faltantes = COLUNAS_OBRIGATORIAS - colunas_presentes
if faltantes:
    raise ValueError(f"Colunas obrigatórias faltando: {faltantes}")

print(f"Schema validado. Colunas: {len(df_raw.columns)}, Linhas: {df_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expandir Metadata JSON

# COMMAND ----------

from pyspark.sql.functions import from_json, col, lit, current_timestamp
from pyspark.sql.types import MapType

# Expandir coluna metadata (JSON string → colunas tipadas)
df_parsed = df_raw

if "metadata" in df_raw.columns:
    # Tentar parsear como JSON
    df_parsed = (
        df_parsed
        .withColumn("_meta_parsed", from_json(col("metadata"), MapType(StringType(), StringType())))
        .withColumn("meta_device_type", col("_meta_parsed").getItem("device_type"))
        .withColumn("meta_os", col("_meta_parsed").getItem("os"))
        .withColumn("meta_browser", col("_meta_parsed").getItem("browser"))
        .withColumn("meta_city", col("_meta_parsed").getItem("city"))
        .withColumn("meta_state", col("_meta_parsed").getItem("state"))
        .withColumn("meta_country", col("_meta_parsed").getItem("country"))
        .drop("_meta_parsed")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados de Ingestão e Escrita Delta

# COMMAND ----------

# Adicionar metadados de ingestão
df_final = (
    df_parsed
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(BRONZE_SOURCE))
)

# Modo de escrita: overwrite na primeira vez, merge/append nas seguintes
if DeltaTable.isDeltaTable(spark, BRONZE_OUTPUT):
    print("Tabela Bronze existente detectada — modo incremental (append)")
    # Append apenas registros novos (por message_id)
    existing = spark.read.format("delta").load(BRONZE_OUTPUT)
    existing_ids = existing.select("message_id").distinct()
    df_novos = df_final.join(existing_ids, on="message_id", how="left_anti")
    novos_count = df_novos.count()

    if novos_count > 0:
        df_novos.write.format("delta").mode("append").save(BRONZE_OUTPUT)
        print(f"Adicionadas {novos_count} novas mensagens à Bronze")
    else:
        print("Nenhuma mensagem nova detectada")
else:
    print("Criando tabela Bronze pela primeira vez")
    df_final.write.format("delta").mode("overwrite").save(BRONZE_OUTPUT)
    print(f"Bronze criada com {df_final.count()} mensagens")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação

# COMMAND ----------

df_bronze = spark.read.format("delta").load(BRONZE_OUTPUT)
_row_count = df_bronze.count()
print(f"Tabela Bronze: {_row_count} linhas, {len(df_bronze.columns)} colunas")
display(df_bronze.limit(5))

# Histórico Delta
history = DeltaTable.forPath(spark, BRONZE_OUTPUT).history()
display(history.select("version", "timestamp", "operation", "operationMetrics"))

# Structured result for orchestrator
import time as _time
_nb_duration = _time.time() - _nb_start
save_run_metadata(boot["data_root"], "01_bronze", "sucesso", _nb_duration, rows_written=_row_count)
dbutils.notebook.exit(make_notebook_result(
    "01_bronze", rows_written=_row_count,
    tables_written=[BRONZE_OUTPUT],
))
