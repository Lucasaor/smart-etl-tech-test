"""Gold Layer — Analytics e KPIs.

Gera tabelas analíticas e KPIs agregados a partir da Silver:
  - Funil de conversão por estágio
  - Lead scoring baseado em sinais comportamentais
  - Performance por campanha
  - Distribuição temporal (dia da semana, hora)

Tabela de saída: gold_analytics
  Múltipas sub-tabelas consolidadas em um único DataFrame com dimensão + métricas.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)


def calcular_funil_conversao(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula funil de conversão por outcome.

    Args:
        df: DataFrame Silver conversations.

    Returns:
        DataFrame com: outcome, contagem, percentual, taxa_acumulada.
    """
    total = len(df)
    if total == 0:
        return pl.DataFrame(schema={
            "conversation_outcome": pl.Utf8,
            "contagem": pl.Int64,
            "percentual": pl.Float64,
        })

    funil = (
        df.group_by("conversation_outcome")
        .len()
        .rename({"len": "contagem"})
        .sort("contagem", descending=True)
    )
    funil = funil.with_columns(
        (pl.col("contagem") / total * 100).round(2).alias("percentual")
    )
    return funil


def calcular_lead_score(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula lead scoring baseado em sinais comportamentais.

    O score é de 0 a 100, composto por:
      - Engajamento (msgs inbound / total) — peso 30
      - Velocidade de resposta — peso 25
      - PII compartilhada (CPF, email, phone) — peso 20
      - Duração da conversa — peso 15
      - Menção a veículo — peso 10

    Args:
        df: DataFrame Silver conversations.

    Returns:
        DataFrame original + coluna lead_score (0-100).
    """
    # Engajamento: proporção de mensagens do lead (0-30 pontos)
    df = df.with_columns(
        pl.when(pl.col("total_messages") > 0)
        .then(
            (pl.col("inbound_messages") / pl.col("total_messages") * 30)
            .clip(0, 30)
        )
        .otherwise(0.0)
        .alias("_score_engajamento")
    )

    # Velocidade de resposta (0-25 pontos): rápido = mais pontos
    df = df.with_columns(
        pl.when(pl.col("avg_response_time_sec").is_null())
        .then(5.0)
        .when(pl.col("avg_response_time_sec") < 60)
        .then(25.0)
        .when(pl.col("avg_response_time_sec") < 180)
        .then(20.0)
        .when(pl.col("avg_response_time_sec") < 600)
        .then(12.0)
        .when(pl.col("avg_response_time_sec") < 1800)
        .then(5.0)
        .otherwise(0.0)
        .alias("_score_resposta")
    )

    # PII compartilhada (0-20 pontos): mais dados = lead mais qualificado
    pii_cols = ["has_cpf", "has_email", "has_phone"]
    available_pii = [c for c in pii_cols if c in df.columns]
    if available_pii:
        df = df.with_columns(
            (
                pl.sum_horizontal(
                    [pl.col(c).cast(pl.Float64) for c in available_pii]
                )
                / len(available_pii)
                * 20
            ).alias("_score_pii")
        )
    else:
        df = df.with_columns(pl.lit(0.0).alias("_score_pii"))

    # Duração/profundidade (0-15 pontos)
    df = df.with_columns(
        pl.when(pl.col("total_messages") >= 15)
        .then(15.0)
        .when(pl.col("total_messages") >= 10)
        .then(12.0)
        .when(pl.col("total_messages") >= 6)
        .then(8.0)
        .when(pl.col("total_messages") >= 3)
        .then(3.0)
        .otherwise(0.0)
        .alias("_score_duracao")
    )

    # Veículo mencionado (0-10 pontos)
    has_plate = "lead_plate" in df.columns
    if has_plate:
        df = df.with_columns(
            pl.when(pl.col("lead_plate").is_not_null())
            .then(10.0)
            .otherwise(0.0)
            .alias("_score_veiculo")
        )
    else:
        df = df.with_columns(pl.lit(0.0).alias("_score_veiculo"))

    # Score final
    df = df.with_columns(
        (
            pl.col("_score_engajamento")
            + pl.col("_score_resposta")
            + pl.col("_score_pii")
            + pl.col("_score_duracao")
            + pl.col("_score_veiculo")
        )
        .round(1)
        .alias("lead_score")
    )

    df = df.drop([
        "_score_engajamento", "_score_resposta", "_score_pii",
        "_score_duracao", "_score_veiculo",
    ])

    return df


def calcular_performance_campanha(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula métricas de performance por campanha.

    Args:
        df: DataFrame Silver conversations (com lead_score se disponível).

    Returns:
        DataFrame com métricas por campanha.
    """
    agg_cols = [
        pl.len().alias("total_conversas"),
        (pl.col("conversation_outcome") == "venda_fechada")
        .sum()
        .alias("vendas"),
        pl.col("total_messages").mean().round(1).alias("media_mensagens"),
        pl.col("avg_response_time_sec").mean().round(1).alias("media_tempo_resposta"),
    ]

    if "lead_score" in df.columns:
        agg_cols.append(pl.col("lead_score").mean().round(1).alias("media_lead_score"))

    campanha = df.group_by("campaign_id").agg(agg_cols).sort("total_conversas", descending=True)

    campanha = campanha.with_columns(
        pl.when(pl.col("total_conversas") > 0)
        .then((pl.col("vendas") / pl.col("total_conversas") * 100).round(2))
        .otherwise(0.0)
        .alias("taxa_conversao_pct")
    )

    return campanha


def gerar_gold_analytics(
    silver_conversations_path: str | Path,
    gold_analytics_path: str | Path,
) -> dict[str, Any]:
    """Gera a tabela Gold de analytics consolidada.

    O output inclui o lead score calculado para cada conversa,
    além de métricas agregadas retornadas nas estatísticas.

    Args:
        silver_conversations_path: Caminho da tabela Silver conversations.
        gold_analytics_path: Caminho destino da tabela Gold analytics.

    Returns:
        Estatísticas com funil, scores e performance por campanha.
    """
    silver_conversations_path = str(silver_conversations_path)
    gold_analytics_path = str(gold_analytics_path)
    storage = get_storage_backend()

    logger.info("lendo_silver_conversations_para_analytics",
                extra={"path": silver_conversations_path})
    df = storage.read_table(silver_conversations_path)

    # Calcular lead score
    df = calcular_lead_score(df)

    # Gerar tabela Gold com dados por conversa + score
    gold_df = df.select([
        "conversation_id",
        "conversation_outcome",
        "lead_score",
        "total_messages",
        "inbound_messages",
        "outbound_messages",
        "avg_response_time_sec",
        "duration_minutes",
        "agent_id",
        "campaign_id",
        "lead_source",
        "lead_state",
        "conversation_date",
    ])

    storage.write_table(gold_df, gold_analytics_path, mode="overwrite", schema_mode="overwrite")

    # Funil de conversão
    funil = calcular_funil_conversao(df)

    # Performance por campanha
    perf_campanha = calcular_performance_campanha(df)

    stats: dict[str, Any] = {
        "total_conversas": len(gold_df),
        "rows_written": len(gold_df),
        "lead_score_medio": round(gold_df["lead_score"].mean(), 1) if len(gold_df) > 0 else 0.0,
        "lead_score_mediano": round(gold_df["lead_score"].median(), 1) if len(gold_df) > 0 else 0.0,
        "funil": {
            row["conversation_outcome"]: row["contagem"]
            for row in funil.iter_rows(named=True)
        },
        "campanhas": len(perf_campanha),
    }

    logger.info("gold_analytics_gerado", extra={"total": stats["total_conversas"]})
    return stats
