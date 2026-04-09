"""Gold Layer — Análise de Sentimento por Conversa.

Classifica cada conversa em sentimento positivo/neutro/negativo
com base em sinais comportamentais da Silver (outcome, duração,
engajamento) e opcionalmente enriquecido via LLM.

Tabela de saída: gold_sentiment (~15k rows)
  - conversation_id, sentimento, score, fatores
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)

# ─── Mapeamento de outcome → sentimento base ─────────────────────────────────

_OUTCOME_SCORE: dict[str, float] = {
    "venda_fechada": 0.9,
    "em_negociacao": 0.5,
    "proposta_enviada": 0.4,
    "perdido_preco": -0.3,
    "perdido_concorrente": -0.4,
    "desistencia_lead": -0.6,
    "ghosting": -0.7,
}


def calcular_sentimento_heuristico(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula score de sentimento com base em heurísticas comportamentais.

    O score final é uma média ponderada de:
      - Outcome da conversa (peso 0.5)
      - Engajamento do lead (proporção inbound/total) (peso 0.2)
      - Tempo de resposta (peso 0.15)
      - Duração relativa da conversa (peso 0.15)

    Args:
        df: DataFrame Silver conversations com colunas esperadas.

    Returns:
        DataFrame com colunas adicionais: sentimento_score, sentimento, sentimento_fatores.
    """
    # Score baseado no outcome
    outcome_mapping = pl.DataFrame({
        "conversation_outcome": list(_OUTCOME_SCORE.keys()),
        "_outcome_score": list(_OUTCOME_SCORE.values()),
    })
    df = df.join(outcome_mapping, on="conversation_outcome", how="left")
    df = df.with_columns(pl.col("_outcome_score").fill_null(0.0))

    # Score de engajamento: proporção de mensagens inbound (lead ativo)
    df = df.with_columns(
        pl.when(pl.col("total_messages") > 0)
        .then(pl.col("inbound_messages") / pl.col("total_messages"))
        .otherwise(0.0)
        .alias("_engagement_score")
    )
    # Normalizar para [-1, 1]: 0.5 ratio = neutro, >0.5 = positivo
    df = df.with_columns(
        ((pl.col("_engagement_score") - 0.3) * 2.0).clip(-1.0, 1.0).alias("_engagement_score")
    )

    # Score de tempo de resposta: rápido = positivo
    df = df.with_columns(
        pl.when(pl.col("avg_response_time_sec").is_null())
        .then(0.0)
        .when(pl.col("avg_response_time_sec") < 60)
        .then(0.8)
        .when(pl.col("avg_response_time_sec") < 180)
        .then(0.4)
        .when(pl.col("avg_response_time_sec") < 600)
        .then(0.0)
        .when(pl.col("avg_response_time_sec") < 1800)
        .then(-0.3)
        .otherwise(-0.6)
        .alias("_response_score")
    )

    # Score de duração: conversas mais longas indicam mais interesse
    df = df.with_columns(
        pl.when(pl.col("total_messages") >= 15)
        .then(0.6)
        .when(pl.col("total_messages") >= 8)
        .then(0.3)
        .when(pl.col("total_messages") >= 4)
        .then(0.0)
        .otherwise(-0.4)
        .alias("_duration_score")
    )

    # Score final ponderado
    df = df.with_columns(
        (
            pl.col("_outcome_score") * 0.50
            + pl.col("_engagement_score") * 0.20
            + pl.col("_response_score") * 0.15
            + pl.col("_duration_score") * 0.15
        )
        .round(3)
        .alias("sentimento_score")
    )

    # Classificação categórica
    df = df.with_columns(
        pl.when(pl.col("sentimento_score") > 0.2)
        .then(pl.lit("positivo"))
        .when(pl.col("sentimento_score") < -0.2)
        .then(pl.lit("negativo"))
        .otherwise(pl.lit("neutro"))
        .alias("sentimento")
    )

    # Fatores que contribuíram para o score
    df = df.with_columns(
        pl.concat_str(
            [
                pl.lit("outcome="),
                pl.col("_outcome_score").round(2).cast(pl.Utf8),
                pl.lit(" eng="),
                pl.col("_engagement_score").round(2).cast(pl.Utf8),
                pl.lit(" resp="),
                pl.col("_response_score").round(2).cast(pl.Utf8),
                pl.lit(" dur="),
                pl.col("_duration_score").round(2).cast(pl.Utf8),
            ]
        ).alias("sentimento_fatores")
    )

    # Limpar colunas temporárias
    df = df.drop(
        ["_outcome_score", "_engagement_score", "_response_score", "_duration_score"]
    )

    return df


def gerar_gold_sentiment(
    silver_conversations_path: str | Path,
    gold_sentiment_path: str | Path,
) -> dict[str, Any]:
    """Gera a tabela Gold de sentimento a partir das conversas Silver.

    Args:
        silver_conversations_path: Caminho da tabela Silver conversations.
        gold_sentiment_path: Caminho destino da tabela Gold sentiment.

    Returns:
        Estatísticas da geração com contagens por sentimento.
    """
    silver_conversations_path = str(silver_conversations_path)
    gold_sentiment_path = str(gold_sentiment_path)
    storage = get_storage_backend()

    logger.info("lendo_silver_conversations_para_sentimento",
                extra={"path": silver_conversations_path})
    df = storage.read_table(silver_conversations_path)

    df = calcular_sentimento_heuristico(df)

    # Selecionar colunas para a tabela Gold
    gold_df = df.select([
        "conversation_id",
        "conversation_outcome",
        "sentimento_score",
        "sentimento",
        "sentimento_fatores",
        "total_messages",
        "avg_response_time_sec",
        "agent_id",
        "campaign_id",
        "conversation_date",
    ])

    storage.write_table(gold_df, gold_sentiment_path, mode="overwrite", schema_mode="overwrite")

    # Estatísticas
    contagens = gold_df.group_by("sentimento").len().sort("sentimento")
    stats = {
        "total_conversas": len(gold_df),
        "positivo": gold_df.filter(pl.col("sentimento") == "positivo").height,
        "neutro": gold_df.filter(pl.col("sentimento") == "neutro").height,
        "negativo": gold_df.filter(pl.col("sentimento") == "negativo").height,
        "score_medio": round(gold_df["sentimento_score"].mean(), 3) if len(gold_df) > 0 else 0.0,
        "rows_written": len(gold_df),
    }
    logger.info("gold_sentimento_gerado", extra=stats)
    return stats
