# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Gold: Analytics, Sentimento, Personas e Segmentação
# MAGIC
# MAGIC Notebook responsável pelas transformações da camada Gold:
# MAGIC 1. **Sentimento**: classificação positivo/neutro/negativo por conversa
# MAGIC 2. **Personas**: classificação de leads em perfis comportamentais
# MAGIC 3. **Segmentação**: segmentação multidimensional da audiência
# MAGIC 4. **Analytics**: lead scoring, funil de conversão, métricas de campanha
# MAGIC 5. **Vendedores**: métricas de performance por vendedor
# MAGIC
# MAGIC **Pré-requisitos:**
# MAGIC - Tabelas Silver populadas (executar notebook 02_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração

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

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder.getOrCreate()

# Paths
SILVER_CONVERSATIONS = paths["silver_conversations"]
GOLD_SENTIMENT = paths["gold_sentiment"]
GOLD_PERSONAS = paths["gold_personas"]
GOLD_SEGMENTATION = paths["gold_segmentation"]
GOLD_ANALYTICS = paths["gold_analytics"]
GOLD_VENDOR = paths["gold_vendor_analysis"]

# COMMAND ----------

df_conv = spark.read.format("delta").load(SILVER_CONVERSATIONS)
print(f"Silver Conversations: {df_conv.count()} conversas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Análise de Sentimento

# COMMAND ----------

# Mapeamento de outcome → score base
OUTCOME_SCORES = {
    "venda_fechada": 0.9, "em_negociacao": 0.5, "proposta_enviada": 0.4,
    "perdido_preco": -0.3, "perdido_concorrente": -0.4,
    "desistencia_lead": -0.6, "ghosting": -0.7,
}

@F.udf(DoubleType())
def outcome_score(outcome):
    return float(OUTCOME_SCORES.get(outcome, 0.0)) if outcome else 0.0

@F.udf(DoubleType())
def engagement_score(inbound, total):
    if not total or total == 0:
        return 0.0
    ratio = inbound / total
    return max(-1.0, min(1.0, (ratio - 0.3) * 2.0))

@F.udf(DoubleType())
def response_score(avg_time):
    if avg_time is None:
        return 0.0
    if avg_time < 60:
        return 0.8
    if avg_time < 180:
        return 0.4
    if avg_time < 600:
        return 0.0
    if avg_time < 1800:
        return -0.3
    return -0.6

@F.udf(DoubleType())
def duration_score(total_msgs):
    if not total_msgs:
        return -0.4
    if total_msgs >= 15:
        return 0.6
    if total_msgs >= 8:
        return 0.3
    if total_msgs >= 4:
        return 0.0
    return -0.4

# COMMAND ----------

df_sentiment = (
    df_conv
    .withColumn("_outcome_s", outcome_score(F.col("conversation_outcome")))
    .withColumn("_engagement_s", engagement_score(F.col("inbound_messages"), F.col("total_messages")))
    .withColumn("_response_s", response_score(F.col("avg_response_time_sec")))
    .withColumn("_duration_s", duration_score(F.col("total_messages")))
    .withColumn(
        "sentimento_score",
        F.round(
            F.col("_outcome_s") * 0.50 +
            F.col("_engagement_s") * 0.20 +
            F.col("_response_s") * 0.15 +
            F.col("_duration_s") * 0.15,
            3
        )
    )
    .withColumn(
        "sentimento",
        F.when(F.col("sentimento_score") > 0.2, F.lit("positivo"))
        .when(F.col("sentimento_score") < -0.2, F.lit("negativo"))
        .otherwise(F.lit("neutro"))
    )
    .withColumn(
        "sentimento_fatores",
        F.concat(
            F.lit("outcome="), F.round(F.col("_outcome_s"), 2).cast("string"),
            F.lit(" eng="), F.round(F.col("_engagement_s"), 2).cast("string"),
            F.lit(" resp="), F.round(F.col("_response_s"), 2).cast("string"),
            F.lit(" dur="), F.round(F.col("_duration_s"), 2).cast("string"),
        )
    )
    .select(
        "conversation_id", "conversation_outcome", "sentimento_score",
        "sentimento", "sentimento_fatores", "total_messages",
        "avg_response_time_sec", "agent_id", "campaign_id", "conversation_date",
    )
)

df_sentiment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_SENTIMENT)
print(f"Gold Sentimento: {df_sentiment.count()} linhas")
df_sentiment.groupBy("sentimento").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Personas de Leads

# COMMAND ----------

@F.udf(StringType())
def classificar_persona(total_msgs, inbound_msgs, outcome, has_competitor):
    """Classificar lead em persona baseado no comportamento."""
    total_msgs = total_msgs or 0
    inbound_msgs = inbound_msgs or 0

    # Fantasma: ghosting + pouca interação
    if outcome == "ghosting" and inbound_msgs <= 3:
        return "fantasma"

    # Decidido: venda rápida
    if outcome == "venda_fechada" and total_msgs <= 12:
        return "decidido"

    # Negociador: muita conversa, menciona concorrente
    if total_msgs > 15 or (has_competitor and outcome in ("perdido_preco", "em_negociacao")):
        return "negociador"

    # Pesquisador: bastante engajamento sem fechar
    if 8 <= total_msgs <= 20 and outcome not in ("venda_fechada",):
        return "pesquisador"

    # Indeciso: fallback
    return "indeciso"

@F.udf(DoubleType())
def confianca_persona(persona, total_msgs, outcome):
    """Score de confiança da classificação (0-1)."""
    total_msgs = total_msgs or 0
    if persona == "fantasma" and outcome == "ghosting":
        return 0.9
    if persona == "decidido" and outcome == "venda_fechada":
        return 0.9
    if persona == "negociador" and total_msgs > 20:
        return 0.7
    if persona == "pesquisador" and 10 <= total_msgs <= 18:
        return 0.6
    return 0.4

# COMMAND ----------

# Preparar coluna has_competitor (simplificada — baseada em extracted_competitors da Silver)
df_conv_enriched = df_conv.withColumn("_has_competitor", F.lit(False))

df_personas = (
    df_conv_enriched
    .withColumn(
        "persona",
        classificar_persona(
            F.col("total_messages"), F.col("inbound_messages"),
            F.col("conversation_outcome"), F.col("_has_competitor"),
        )
    )
    .withColumn(
        "persona_confianca",
        confianca_persona(
            F.col("persona"), F.col("total_messages"), F.col("conversation_outcome"),
        )
    )
    .withColumn("persona_fatores", F.lit("heuristic_v1"))
    .select(
        "conversation_id", "persona", "persona_confianca", "persona_fatores",
        "total_messages", "conversation_outcome", "agent_id", "campaign_id",
    )
)

df_personas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_PERSONAS)
print(f"Gold Personas: {df_personas.count()} linhas")
df_personas.groupBy("persona").count().orderBy(F.desc("count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Segmentação

# COMMAND ----------

# Mapeamento de estado → região
ESTADO_REGIAO = {
    "SP": "sudeste", "RJ": "sudeste", "MG": "sudeste", "ES": "sudeste",
    "PR": "sul", "SC": "sul", "RS": "sul",
    "BA": "nordeste", "PE": "nordeste", "CE": "nordeste", "MA": "nordeste",
    "PB": "nordeste", "RN": "nordeste", "AL": "nordeste", "SE": "nordeste", "PI": "nordeste",
    "GO": "centro_oeste", "MT": "centro_oeste", "MS": "centro_oeste", "DF": "centro_oeste",
    "AM": "norte", "PA": "norte", "AC": "norte", "RO": "norte",
    "RR": "norte", "AP": "norte", "TO": "norte",
}

@F.udf(StringType())
def regiao_por_estado(estado):
    if not estado:
        return "desconhecido"
    return ESTADO_REGIAO.get(estado.upper(), "desconhecido")

# COMMAND ----------

df_segmentation = (
    df_conv
    # Engajamento
    .withColumn(
        "seg_engajamento",
        F.when(F.col("total_messages") >= 15, F.lit("alto"))
        .when(F.col("total_messages") >= 6, F.lit("medio"))
        .otherwise(F.lit("baixo"))
    )
    # Velocidade de resposta
    .withColumn(
        "seg_velocidade_resposta",
        F.when(F.col("avg_response_time_sec").isNull(), F.lit("desconhecido"))
        .when(F.col("avg_response_time_sec") < 120, F.lit("rapido"))
        .when(F.col("avg_response_time_sec") < 600, F.lit("moderado"))
        .otherwise(F.lit("lento"))
    )
    # Veículo
    .withColumn("seg_veiculo", F.lit("sem_veiculo"))  # Simplificado
    # Região
    .withColumn("seg_regiao", regiao_por_estado(F.col("lead_state")))
    # Origem
    .withColumn(
        "seg_origem",
        F.when(F.lower(F.col("lead_source")).contains("google"), F.lit("google"))
        .when(F.lower(F.col("lead_source")).contains("facebook"), F.lit("facebook"))
        .when(F.lower(F.col("lead_source")).contains("instagram"), F.lit("instagram"))
        .when(F.lower(F.col("lead_source")).contains("indica"), F.lit("indicacao"))
        .otherwise(F.lit("outros"))
    )
    # Duração
    .withColumn(
        "seg_duracao",
        F.when(F.col("duration_minutes").isNull(), F.lit("desconhecido"))
        .when(F.col("duration_minutes") < 1, F.lit("instantanea"))
        .when(F.col("duration_minutes") < 5, F.lit("flash"))
        .when(F.col("duration_minutes") < 60, F.lit("curta"))
        .when(F.col("duration_minutes") < 1440, F.lit("media"))
        .otherwise(F.lit("longa"))
    )
    # Qualificação do lead (baseada em PII disponível)
    .withColumn(
        "_pii_count",
        F.col("has_cpf").cast("int") + F.col("has_email").cast("int") + F.col("has_phone").cast("int")
    )
    .withColumn(
        "seg_qualificacao_lead",
        F.when(F.col("_pii_count") >= 2, F.lit("alta"))
        .when(F.col("_pii_count") == 1, F.lit("media"))
        .otherwise(F.lit("baixa"))
    )
    .select(
        "conversation_id",
        "seg_engajamento", "seg_velocidade_resposta", "seg_veiculo",
        "seg_regiao", "seg_origem", "seg_duracao", "seg_qualificacao_lead",
        "agent_id", "campaign_id",
    )
)

df_segmentation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_SEGMENTATION)
print(f"Gold Segmentação: {df_segmentation.count()} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Analytics (Lead Scoring)

# COMMAND ----------

@F.udf(DoubleType())
def calcular_lead_score(inbound, total, avg_resp, has_cpf, has_email, has_phone, has_plate):
    """Lead Score de 0 a 100."""
    score = 0.0
    total = total or 0
    inbound = inbound or 0

    # Engajamento (30%)
    if total > 0:
        score += (inbound / total) * 30.0

    # Velocidade de resposta (25%)
    if avg_resp is not None:
        if avg_resp < 60:
            score += 25.0
        elif avg_resp < 180:
            score += 18.0
        elif avg_resp < 600:
            score += 10.0
        else:
            score += 5.0
    else:
        score += 5.0

    # Qualidade PII (20%)
    pii_count = sum([
        1 if has_cpf else 0,
        1 if has_email else 0,
        1 if has_phone else 0,
    ])
    score += (pii_count / 3.0) * 20.0

    # Profundidade (15%)
    if total >= 15:
        score += 15.0
    elif total >= 8:
        score += 10.0
    elif total >= 4:
        score += 5.0

    # Veículo (10%)
    if has_plate:
        score += 10.0

    return round(min(score, 100.0), 1)

# COMMAND ----------

df_analytics = (
    df_conv
    .withColumn("_has_plate", F.lit(False))
    .withColumn(
        "lead_score",
        calcular_lead_score(
            F.col("inbound_messages"), F.col("total_messages"),
            F.col("avg_response_time_sec"),
            F.col("has_cpf"), F.col("has_email"), F.col("has_phone"),
            F.col("_has_plate"),
        )
    )
    .select(
        "conversation_id", "lead_score", "conversation_outcome",
        "total_messages", "inbound_messages", "avg_response_time_sec",
        "has_cpf", "has_email", "has_phone",
        "agent_id", "campaign_id", "lead_source", "conversation_date",
    )
)

df_analytics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_ANALYTICS)
print(f"Gold Analytics: {df_analytics.count()} linhas")
df_analytics.describe("lead_score").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Análise de Vendedores

# COMMAND ----------

df_vendor = (
    df_conv.groupBy("agent_id")
    .agg(
        F.count("*").alias("total_conversas"),
        F.sum(F.when(F.col("conversation_outcome") == "venda_fechada", 1).otherwise(0)).alias("vendas_fechadas"),
        F.sum(F.when(F.col("conversation_outcome") == "ghosting", 1).otherwise(0)).alias("ghosting_count"),
        F.avg("total_messages").alias("media_mensagens"),
        F.avg("avg_response_time_sec").alias("media_tempo_resposta_sec"),
        F.avg("duration_minutes").alias("media_duracao_min"),
    )
    .withColumn(
        "taxa_conversao",
        F.round(F.col("vendas_fechadas") / F.col("total_conversas") * 100, 2)
    )
    .withColumn(
        "taxa_ghosting",
        F.round(F.col("ghosting_count") / F.col("total_conversas") * 100, 2)
    )
    # Score do vendedor: 50% conversão + 20% retenção + 30% velocidade
    .withColumn(
        "score_vendedor",
        F.round(
            F.col("taxa_conversao") * 0.50 +
            (100.0 - F.col("taxa_ghosting")) * 0.20 +
            F.when(F.col("media_tempo_resposta_sec").isNull(), F.lit(50.0))
            .when(F.col("media_tempo_resposta_sec") < 120, F.lit(100.0))
            .when(F.col("media_tempo_resposta_sec") < 300, F.lit(75.0))
            .when(F.col("media_tempo_resposta_sec") < 600, F.lit(50.0))
            .otherwise(F.lit(25.0)) * 0.30,
            1
        )
    )
)

df_vendor.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_VENDOR)
print(f"Gold Vendedores: {df_vendor.count()} linhas")
display(df_vendor.orderBy(F.desc("score_vendedor")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação Final

# COMMAND ----------

print("=== Resumo das Tabelas Gold ===")
_gold_total_rows = 0
for nome, path in [
    ("Sentimento", GOLD_SENTIMENT),
    ("Personas", GOLD_PERSONAS),
    ("Segmentação", GOLD_SEGMENTATION),
    ("Analytics", GOLD_ANALYTICS),
    ("Vendedores", GOLD_VENDOR),
]:
    try:
        df = spark.read.format("delta").load(path)
        _cnt = df.count()
        _gold_total_rows += _cnt
        print(f"  {nome}: {_cnt} linhas, {len(df.columns)} colunas")
    except Exception as e:
        print(f"  {nome}: ERRO - {e}")

# Structured result for orchestrator
import time as _time
_nb_duration = _time.time() - _nb_start
save_run_metadata(boot["data_root"], "03_gold", "sucesso", _nb_duration, rows_written=_gold_total_rows)
dbutils.notebook.exit(make_notebook_result(
    "03_gold", rows_written=_gold_total_rows,
    tables_written=[GOLD_SENTIMENT, GOLD_PERSONAS, GOLD_SEGMENTATION, GOLD_ANALYTICS, GOLD_VENDOR],
))
