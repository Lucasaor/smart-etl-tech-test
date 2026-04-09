"""Gold Layer — Análise de Performance por Vendedor.

Gera métricas agregadas por vendedor (agent_id) a partir da Silver:
  - Total de conversas e taxa de conversão
  - Tempo médio de resposta
  - Distribuição de outcomes
  - Score médio de engajamento

Tabela de saída: gold_vendor_analysis (~15-20 vendedores)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)


def calcular_metricas_vendedor(df: pl.DataFrame) -> pl.DataFrame:
    """Calcula métricas de performance por vendedor.

    Args:
        df: DataFrame Silver conversations.

    Returns:
        DataFrame com uma linha por vendedor e suas métricas.
    """
    vendedores = df.group_by("agent_id").agg([
        pl.len().alias("total_conversas"),

        # Contagens de outcomes
        (pl.col("conversation_outcome") == "venda_fechada")
        .sum()
        .alias("vendas_fechadas"),
        (pl.col("conversation_outcome") == "ghosting")
        .sum()
        .alias("ghosting"),
        (pl.col("conversation_outcome") == "perdido_preco")
        .sum()
        .alias("perdido_preco"),
        (pl.col("conversation_outcome") == "perdido_concorrente")
        .sum()
        .alias("perdido_concorrente"),
        (pl.col("conversation_outcome") == "proposta_enviada")
        .sum()
        .alias("propostas_enviadas"),
        (pl.col("conversation_outcome") == "em_negociacao")
        .sum()
        .alias("em_negociacao"),

        # Métricas de eficiência
        pl.col("total_messages").mean().round(1).alias("media_mensagens"),
        pl.col("avg_response_time_sec").mean().round(1).alias("media_tempo_resposta_sec"),
        pl.col("duration_minutes").mean().round(1).alias("media_duracao_minutos"),

        # Engajamento
        pl.col("inbound_messages").mean().round(1).alias("media_msgs_lead"),

        # Menções a concorrentes
        pl.col("competitor_mentioned").is_not_null().sum().alias("conversas_com_concorrente"),
    ]).sort("total_conversas", descending=True)

    # Taxa de conversão
    vendedores = vendedores.with_columns(
        pl.when(pl.col("total_conversas") > 0)
        .then(
            (pl.col("vendas_fechadas") / pl.col("total_conversas") * 100).round(2)
        )
        .otherwise(0.0)
        .alias("taxa_conversao_pct")
    )

    # Taxa de ghosting
    vendedores = vendedores.with_columns(
        pl.when(pl.col("total_conversas") > 0)
        .then(
            (pl.col("ghosting") / pl.col("total_conversas") * 100).round(2)
        )
        .otherwise(0.0)
        .alias("taxa_ghosting_pct")
    )

    # Score do vendedor (0-100): combinação de taxa de conversão e engajamento
    vendedores = vendedores.with_columns(
        (
            pl.col("taxa_conversao_pct") * 0.5  # Conversão vale 50%
            + (100 - pl.col("taxa_ghosting_pct").clip(0, 100)) * 0.2  # Retenção vale 20%
            + pl.when(pl.col("media_tempo_resposta_sec").is_null())
            .then(10.0)
            .when(pl.col("media_tempo_resposta_sec") < 120)
            .then(30.0)
            .when(pl.col("media_tempo_resposta_sec") < 300)
            .then(20.0)
            .otherwise(10.0)
            * 0.3  # Velocidade vale 30%
        )
        .round(1)
        .alias("score_vendedor")
    )

    return vendedores


def gerar_gold_vendor(
    silver_conversations_path: str | Path,
    gold_vendor_path: str | Path,
) -> dict[str, Any]:
    """Gera a tabela Gold de análise por vendedor.

    Args:
        silver_conversations_path: Caminho da tabela Silver conversations.
        gold_vendor_path: Caminho destino da tabela Gold vendor analysis.

    Returns:
        Estatísticas com total de vendedores e métricas gerais.
    """
    silver_conversations_path = str(silver_conversations_path)
    gold_vendor_path = str(gold_vendor_path)
    storage = get_storage_backend()

    logger.info("lendo_silver_conversations_para_vendedores",
                extra={"path": silver_conversations_path})
    df = storage.read_table(silver_conversations_path)

    vendedores = calcular_metricas_vendedor(df)

    storage.write_table(vendedores, gold_vendor_path, mode="overwrite", schema_mode="overwrite")

    stats: dict[str, Any] = {
        "total_vendedores": len(vendedores),
        "rows_written": len(vendedores),
        "melhor_taxa_conversao": (
            round(vendedores["taxa_conversao_pct"].max(), 2)
            if len(vendedores) > 0 else 0.0
        ),
        "pior_taxa_conversao": (
            round(vendedores["taxa_conversao_pct"].min(), 2)
            if len(vendedores) > 0 else 0.0
        ),
        "media_taxa_conversao": (
            round(vendedores["taxa_conversao_pct"].mean(), 2)
            if len(vendedores) > 0 else 0.0
        ),
    }

    logger.info("gold_vendedores_gerado", extra=stats)
    return stats
