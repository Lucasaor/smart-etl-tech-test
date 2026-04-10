# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Camada Silver: Limpeza, Extração e Agregação
# MAGIC
# MAGIC Notebook responsável pelas transformações da camada Silver:
# MAGIC 1. **Limpeza**: dedup de status, normalização, flags de conteúdo
# MAGIC 2. **Extração**: entidades (CPF, email, telefone, CEP, placa, concorrentes)
# MAGIC 3. **Agregação**: mensagens → nível conversa (~15k conversas)
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Tabela Bronze populada (executar notebook 01_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do Ambiente

# COMMAND ----------

import os
import sys
import re
import hashlib

repo_path = "/Workspace/Repos/agentic-pipeline"
if os.path.exists(repo_path):
    sys.path.insert(0, repo_path)

os.environ["RUNTIME_ENV"] = "databricks"
os.environ["DATA_ROOT"] = "/mnt/delta"

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Paths
BRONZE_PATH = "/mnt/delta/bronze"
SILVER_MESSAGES_PATH = "/mnt/delta/silver/messages"
SILVER_CONVERSATIONS_PATH = "/mnt/delta/silver/conversations"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 1: Limpeza (Dedup + Normalização)

# COMMAND ----------

df_bronze = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze: {df_bronze.count()} linhas")

# ─── Dedup de status ──────────────────────────────────────────────────────────
# Manter apenas o status de maior prioridade por message_id
PRIORIDADE_STATUS = {"delivered": 3, "read": 4, "sent": 2, "failed": 1}

@F.udf(StringType())
def prioridade_status(status):
    return str(PRIORIDADE_STATUS.get(status, 0))

df_ranked = (
    df_bronze
    .withColumn("_status_rank", prioridade_status(F.col("message_status")))
)

window_dedup = Window.partitionBy("message_id").orderBy(F.col("_status_rank").desc())
df_dedup = (
    df_ranked
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num", "_status_rank")
)

print(f"Após dedup: {df_dedup.count()} linhas (removidas {df_bronze.count() - df_dedup.count()} duplicatas de status)")

# COMMAND ----------

# ─── Normalização e Flags ─────────────────────────────────────────────────────

df_clean = (
    df_dedup
    # Normalizar sender_name: lowercase + strip
    .withColumn(
        "sender_name_normalized",
        F.lower(F.trim(F.col("sender_name")))
    )
    # Flag de transcrição de áudio
    .withColumn(
        "is_audio_transcription",
        F.when(
            F.col("message_body").rlike("(?i)\\[transcrição de áudio\\]|\\[audio transcription\\]"),
            True
        ).otherwise(False)
    )
    # Body normalizado (trim + null → "")
    .withColumn(
        "message_body_normalized",
        F.trim(F.coalesce(F.col("message_body"), F.lit("")))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 2: Extração de Entidades

# COMMAND ----------

# ─── UDFs de extração ─────────────────────────────────────────────────────────

@F.udf(ArrayType(StringType()))
def extrair_cpfs(texto):
    """Extrair CPFs do texto via regex."""
    if not texto:
        return []
    padrao = r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b"
    return list(set(re.findall(padrao, texto)))

@F.udf(ArrayType(StringType()))
def extrair_emails(texto):
    """Extrair e-mails do texto via regex."""
    if not texto:
        return []
    padrao = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"
    return list(set(re.findall(padrao, texto)))

@F.udf(ArrayType(StringType()))
def extrair_telefones(texto):
    """Extrair telefones do texto via regex."""
    if not texto:
        return []
    padrao = r"\b(?:\+55\s?)?\(?\d{2}\)?\s?\d{4,5}-?\d{4}\b"
    return list(set(re.findall(padrao, texto)))

@F.udf(ArrayType(StringType()))
def extrair_ceps(texto):
    """Extrair CEPs do texto via regex."""
    if not texto:
        return []
    padrao = r"\b\d{5}-?\d{3}\b"
    return list(set(re.findall(padrao, texto)))

@F.udf(ArrayType(StringType()))
def extrair_placas(texto):
    """Extrair placas Mercosul do texto via regex."""
    if not texto:
        return []
    padrao = r"\b[A-Z]{3}\d[A-Z0-9]\d{2}\b"
    return list(set(re.findall(padrao, texto.upper())))

CONCORRENTES = [
    "porto seguro", "bradesco seguros", "bradesco", "allianz",
    "azul seguros", "hdi", "liberty", "mapfre", "suhai",
    "tokio marine", "sompo", "zurich",
]

@F.udf(ArrayType(StringType()))
def extrair_concorrentes(texto):
    """Detectar menções a concorrentes no texto."""
    if not texto:
        return []
    texto_lower = texto.lower()
    return [c for c in CONCORRENTES if c in texto_lower]

@F.udf(StringType())
def mascarar_pii(valor):
    """Mascarar dados sensíveis com hash SHA256 (primeiros 16 chars)."""
    if not valor:
        return None
    return hashlib.sha256(valor.encode()).hexdigest()[:16]

# COMMAND ----------

# Aplicar extração de entidades
df_entities = (
    df_clean
    .withColumn("extracted_cpfs", extrair_cpfs(F.col("message_body_normalized")))
    .withColumn("extracted_emails", extrair_emails(F.col("message_body_normalized")))
    .withColumn("extracted_phones", extrair_telefones(F.col("message_body_normalized")))
    .withColumn("extracted_ceps", extrair_ceps(F.col("message_body_normalized")))
    .withColumn("extracted_plates", extrair_placas(F.col("message_body_normalized")))
    .withColumn("extracted_competitors", extrair_concorrentes(F.col("message_body_normalized")))
    # Flags de PII encontrada
    .withColumn("pii_cpf_found", F.size("extracted_cpfs") > 0)
    .withColumn("pii_email_found", F.size("extracted_emails") > 0)
    .withColumn("pii_phone_found", F.size("extracted_phones") > 0)
    .withColumn("pii_cep_found", F.size("extracted_ceps") > 0)
)

# Mascarar PII (hash SHA256)
df_masked = (
    df_entities
    .withColumn("extracted_cpfs", F.transform("extracted_cpfs", lambda x: mascarar_pii(x)))
    .withColumn("extracted_emails", F.transform("extracted_emails", lambda x: mascarar_pii(x)))
    .withColumn("extracted_phones", F.transform("extracted_phones", lambda x: mascarar_pii(x)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrever Silver Messages

# COMMAND ----------

# Adicionar versão de processamento
df_silver_msgs = df_masked.withColumn("_silver_version", F.lit(1))

df_silver_msgs.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_MESSAGES_PATH)
print(f"Silver Messages: {df_silver_msgs.count()} linhas escritas em {SILVER_MESSAGES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Etapa 3: Agregação por Conversa

# COMMAND ----------

df_msgs = spark.read.format("delta").load(SILVER_MESSAGES_PATH)

# ─── Agregações por conversa ──────────────────────────────────────────────────

# Contagens de mensagens
df_agg = (
    df_msgs.groupBy("conversation_id")
    .agg(
        # Contagens
        F.count("*").alias("total_messages"),
        F.sum(F.when(F.col("direction") == "inbound", 1).otherwise(0)).alias("inbound_messages"),
        F.sum(F.when(F.col("direction") == "outbound", 1).otherwise(0)).alias("outbound_messages"),
        F.sum(F.when(F.col("message_type") == "audio", 1).otherwise(0)).alias("audio_messages"),
        F.sum(F.when(F.col("message_type") == "image", 1).otherwise(0)).alias("image_messages"),

        # Timing
        F.min("timestamp").alias("first_message_at"),
        F.max("timestamp").alias("last_message_at"),

        # Entidades (any)
        F.max(F.col("pii_cpf_found").cast("int")).cast("boolean").alias("has_cpf"),
        F.max(F.col("pii_email_found").cast("int")).cast("boolean").alias("has_email"),
        F.max(F.col("pii_phone_found").cast("int")).cast("boolean").alias("has_phone"),
        F.max(F.col("pii_cep_found").cast("int")).cast("boolean").alias("has_cep"),

        # Metadata (first non-null)
        F.first("agent_id", ignorenulls=True).alias("agent_id"),
        F.first("campaign_id", ignorenulls=True).alias("campaign_id"),
        F.first("lead_source", ignorenulls=True).alias("lead_source"),
        F.first("channel", ignorenulls=True).alias("channel"),
        F.first("meta_city", ignorenulls=True).alias("lead_city"),
        F.first("meta_state", ignorenulls=True).alias("lead_state"),
    )
)

# Calcular métricas derivadas
df_conversations = (
    df_agg
    .withColumn(
        "duration_minutes",
        (F.unix_timestamp("last_message_at") - F.unix_timestamp("first_message_at")) / 60.0
    )
    .withColumn(
        "conversation_date",
        F.to_date("first_message_at")
    )
)

# ─── Outcome (simplificado — join com lead info via primeira mensagem inbound) ─

# Para Databricks, usar heurística simples baseada no padrão de mensagens
# Em produção, o outcome viria de um CRM ou label na fonte
df_conversations = (
    df_conversations
    .withColumn(
        "conversation_outcome",
        F.when(F.col("total_messages") >= 15, F.lit("em_negociacao"))
        .when(F.col("total_messages") <= 2, F.lit("ghosting"))
        .otherwise(F.lit("contato_inicial"))
    )
    # Tempo médio de resposta placeholder
    .withColumn("avg_response_time_sec", F.lit(None).cast("double"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrever Silver Conversations

# COMMAND ----------

df_conversations.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER_CONVERSATIONS_PATH)
print(f"Silver Conversations: {df_conversations.count()} conversas escritas em {SILVER_CONVERSATIONS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação

# COMMAND ----------

df_silver_check = spark.read.format("delta").load(SILVER_CONVERSATIONS_PATH)
print(f"Silver Conversations: {df_silver_check.count()} conversas, {len(df_silver_check.columns)} colunas")
display(df_silver_check.limit(5))

# Estatísticas rápidas
df_silver_check.select(
    F.count("*").alias("total"),
    F.avg("total_messages").alias("media_mensagens"),
    F.avg("duration_minutes").alias("media_duracao_min"),
).show()
