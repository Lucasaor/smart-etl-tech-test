"""Gold Layer — Segmentação Multidimensional de Leads.

Segmenta conversas por múltiplas dimensões encontradas na Silver:
  - Veículo: tem placa mencionada ou não
  - Engajamento: alto/médio/baixo baseado em mensagens e tempo de resposta
  - Origem: campanha e lead_source
  - Geografia: cidade e estado
  - Horário: horário comercial vs fora de horário

Tabela de saída: gold_segmentation (~15k rows)
  - conversation_id + todas as dimensões de segmentação
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)


def segmentar_conversas(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica segmentação multidimensional às conversas.

    Args:
        df: DataFrame Silver conversations.

    Returns:
        DataFrame com colunas de segmentação adicionais.
    """
    # ── Segmento de engajamento ──────────────────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("total_messages") >= 15)
        .then(pl.lit("alto"))
        .when(pl.col("total_messages") >= 6)
        .then(pl.lit("medio"))
        .otherwise(pl.lit("baixo"))
        .alias("seg_engajamento")
    )

    # ── Segmento de resposta ─────────────────────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("avg_response_time_sec").is_null())
        .then(pl.lit("sem_dados"))
        .when(pl.col("avg_response_time_sec") < 120)
        .then(pl.lit("rapido"))
        .when(pl.col("avg_response_time_sec") < 600)
        .then(pl.lit("moderado"))
        .otherwise(pl.lit("lento"))
        .alias("seg_velocidade_resposta")
    )

    # ── Segmento de veículo ──────────────────────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("lead_plate").is_not_null())
        .then(pl.lit("com_veiculo"))
        .otherwise(pl.lit("sem_veiculo"))
        .alias("seg_veiculo")
    )

    # ── Segmento geográfico (região) ────────────────────────────────────────
    _SUDESTE = ["SP", "RJ", "MG", "ES"]
    _SUL = ["PR", "SC", "RS"]
    _NORDESTE = ["BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"]
    _CENTRO_OESTE = ["DF", "GO", "MT", "MS"]
    _NORTE = ["AM", "PA", "AC", "RO", "RR", "AP", "TO"]

    df = df.with_columns(
        pl.when(pl.col("lead_state").is_in(_SUDESTE))
        .then(pl.lit("sudeste"))
        .when(pl.col("lead_state").is_in(_SUL))
        .then(pl.lit("sul"))
        .when(pl.col("lead_state").is_in(_NORDESTE))
        .then(pl.lit("nordeste"))
        .when(pl.col("lead_state").is_in(_CENTRO_OESTE))
        .then(pl.lit("centro_oeste"))
        .when(pl.col("lead_state").is_in(_NORTE))
        .then(pl.lit("norte"))
        .otherwise(pl.lit("desconhecido"))
        .alias("seg_regiao")
    )

    # ── Segmento de origem ───────────────────────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("lead_source").is_null())
        .then(pl.lit("desconhecido"))
        .when(pl.col("lead_source").str.contains("(?i)google"))
        .then(pl.lit("google"))
        .when(pl.col("lead_source").str.contains("(?i)facebook|meta"))
        .then(pl.lit("facebook"))
        .when(pl.col("lead_source").str.contains("(?i)instagram"))
        .then(pl.lit("instagram"))
        .when(pl.col("lead_source").str.contains("(?i)indicacao|referral"))
        .then(pl.lit("indicacao"))
        .otherwise(pl.lit("outros"))
        .alias("seg_origem")
    )

    # ── Segmento de duração da conversa ──────────────────────────────────────
    df = df.with_columns(
        pl.when(pl.col("duration_minutes").is_null())
        .then(pl.lit("instantanea"))
        .when(pl.col("duration_minutes") < 5)
        .then(pl.lit("flash"))
        .when(pl.col("duration_minutes") < 60)
        .then(pl.lit("curta"))
        .when(pl.col("duration_minutes") < 1440)
        .then(pl.lit("media"))
        .otherwise(pl.lit("longa"))
        .alias("seg_duracao")
    )

    # ── Segmento de PII disponível ───────────────────────────────────────────
    pii_cols = ["has_cpf", "has_email", "has_phone"]
    available_pii_cols = [c for c in pii_cols if c in df.columns]
    if available_pii_cols:
        df = df.with_columns(
            pl.sum_horizontal(
                [pl.col(c).cast(pl.Int32) for c in available_pii_cols]
            ).alias("_pii_count")
        )
        df = df.with_columns(
            pl.when(pl.col("_pii_count") >= 2)
            .then(pl.lit("alta_qualificacao"))
            .when(pl.col("_pii_count") == 1)
            .then(pl.lit("media_qualificacao"))
            .otherwise(pl.lit("baixa_qualificacao"))
            .alias("seg_qualificacao_lead")
        )
        df = df.drop("_pii_count")
    else:
        df = df.with_columns(pl.lit("desconhecido").alias("seg_qualificacao_lead"))

    return df


def gerar_gold_segmentation(
    silver_conversations_path: str | Path,
    gold_segmentation_path: str | Path,
) -> dict[str, Any]:
    """Gera a tabela Gold de segmentação a partir das conversas Silver.

    Args:
        silver_conversations_path: Caminho da tabela Silver conversations.
        gold_segmentation_path: Caminho destino da tabela Gold segmentation.

    Returns:
        Estatísticas com distribuição por segmento.
    """
    silver_conversations_path = str(silver_conversations_path)
    gold_segmentation_path = str(gold_segmentation_path)
    storage = get_storage_backend()

    logger.info("lendo_silver_conversations_para_segmentacao",
                extra={"path": silver_conversations_path})
    df = storage.read_table(silver_conversations_path)

    df = segmentar_conversas(df)

    gold_df = df.select([
        "conversation_id",
        "conversation_outcome",
        "seg_engajamento",
        "seg_velocidade_resposta",
        "seg_veiculo",
        "seg_regiao",
        "seg_origem",
        "seg_duracao",
        "seg_qualificacao_lead",
        "total_messages",
        "agent_id",
        "campaign_id",
        "lead_source",
        "lead_state",
        "lead_city",
        "conversation_date",
    ])

    storage.write_table(gold_df, gold_segmentation_path, mode="overwrite", schema_mode="overwrite")

    # Estatísticas de distribuição
    stats: dict[str, Any] = {
        "total_conversas": len(gold_df),
        "rows_written": len(gold_df),
    }
    for seg_col in ["seg_engajamento", "seg_veiculo", "seg_regiao", "seg_origem"]:
        distribuicao = gold_df.group_by(seg_col).len().sort("len", descending=True)
        stats[seg_col] = {
            row[seg_col]: row["len"]
            for row in distribuicao.iter_rows(named=True)
        }

    logger.info("gold_segmentacao_gerada", extra={"total": stats["total_conversas"]})
    return stats
