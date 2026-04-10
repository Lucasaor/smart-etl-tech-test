# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Orquestrador de Agentes
# MAGIC
# MAGIC Notebook responsável pela orquestração dos agentes de IA no Databricks.
# MAGIC Executa o pipeline completo com monitoramento e auto-correção.
# MAGIC
# MAGIC **Modos de execução:**
# MAGIC - **Manual**: executar célula por célula
# MAGIC - **Periódico**: chamar via `dbutils.notebook.run()` com parâmetros
# MAGIC
# MAGIC **Limitações do Community Edition:**
# MAGIC - Sem Workflows/Jobs nativos
# MAGIC - Cluster auto-termina após 2h de inatividade
# MAGIC - Pipeline "vivo" simulado via execução periódica manual ou trigger

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

# COMMAND ----------

import os
import sys
import time
from datetime import datetime

repo_path = "/Workspace/Repos/agentic-pipeline"
if os.path.exists(repo_path):
    sys.path.insert(0, repo_path)

os.environ["RUNTIME_ENV"] = "databricks"
os.environ["DATA_ROOT"] = "/mnt/delta"

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
        Dicionário com status, duração e resultado.
    """
    inicio = time.time()
    status = "sucesso"
    resultado = None
    erro = None

    try:
        resultado = dbutils.notebook.run(path, timeout, params or {})
    except Exception as e:
        status = "falha"
        erro = str(e)

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
    "/Workspace/Repos/agentic-pipeline/databricks/notebooks/01_bronze",
    "/Workspace/Repos/agentic-pipeline/databricks/notebooks/02_silver",
    "/Workspace/Repos/agentic-pipeline/databricks/notebooks/03_gold",
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
    "Bronze": "/mnt/delta/bronze",
    "Silver Messages": "/mnt/delta/silver/messages",
    "Silver Conversations": "/mnt/delta/silver/conversations",
    "Gold Sentimento": "/mnt/delta/gold/sentiment",
    "Gold Personas": "/mnt/delta/gold/personas",
    "Gold Segmentação": "/mnt/delta/gold/segmentation",
    "Gold Analytics": "/mnt/delta/gold/analytics",
    "Gold Vendedores": "/mnt/delta/gold/vendor_analysis",
}

print("=== Verificação de Saúde ===\n")
for nome, path in TABELAS.items():
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, path):
            df = spark.read.format("delta").load(path)
            linhas = df.count()
            colunas = len(df.columns)
            version = DeltaTable.forPath(spark, path).history(1).select("version").collect()[0][0]
            print(f"  ✅ {nome}: {linhas:,} linhas, {colunas} colunas (v{version})")
        else:
            print(f"  ⚠️  {nome}: tabela não encontrada")
    except Exception as e:
        print(f"  ❌ {nome}: ERRO - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execução Periódica (Simulação de Pipeline Vivo)
# MAGIC
# MAGIC No Databricks Community Edition não há Workflows/Jobs nativos.
# MAGIC Esta célula pode ser executada manualmente para simular
# MAGIC a detecção de novos dados e re-execução do pipeline.
# MAGIC
# MAGIC **Para uso automatizado**, chame este notebook via:
# MAGIC ```python
# MAGIC dbutils.notebook.run(
# MAGIC     "/Workspace/Repos/agentic-pipeline/databricks/notebooks/04_agent_orchestrator",
# MAGIC     timeout_seconds=3600
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detecção de Novos Dados

# COMMAND ----------

from delta.tables import DeltaTable

def verificar_novos_dados():
    """Verifica se há novos dados na camada Bronze que ainda não foram processados."""
    try:
        bronze_path = "/mnt/delta/bronze"
        silver_path = "/mnt/delta/silver/messages"

        if not DeltaTable.isDeltaTable(spark, bronze_path):
            return {"novos_dados": False, "motivo": "Tabela Bronze não existe"}

        df_bronze = spark.read.format("delta").load(bronze_path)
        count_bronze = df_bronze.count()

        if not DeltaTable.isDeltaTable(spark, silver_path):
            return {"novos_dados": True, "motivo": f"Silver vazia, Bronze tem {count_bronze} linhas"}

        df_silver = spark.read.format("delta").load(silver_path)
        count_silver = df_silver.count()

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
