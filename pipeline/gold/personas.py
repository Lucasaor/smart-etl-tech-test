"""Gold Layer — Classificação de Personas de Leads.

Classifica cada lead em uma persona baseada no comportamento
conversacional observado na Silver: duração, engajamento,
menção a concorrentes, outcome, e padrões de resposta.

Personas definidas:
  - Decidido: conversa curta, fecha rápido
  - Pesquisador: muitas perguntas, compara preços/concorrentes
  - Negociador: conversa longa, vai e volta, pede desconto
  - Fantasma: para de responder no meio da conversa
  - Indeciso: conversa média, sem decisão clara

Tabela de saída: gold_personas (~15k rows)
  - conversation_id, persona, confianca, fatores
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)


def classificar_personas(df: pl.DataFrame) -> pl.DataFrame:
    """Classifica cada conversa em uma persona baseada em regras comportamentais.

    Regras de classificação (aplicadas em ordem de prioridade):

    1. **Fantasma**: outcome='ghosting' E poucas mensagens do lead (<=3 inbound)
    2. **Decidido**: outcome='venda_fechada' E conversa relativamente curta (<=12 msgs)
    3. **Negociador**: conversa longa (>15 msgs) OU menção a concorrente OU
       outcome='perdido_preco'
    4. **Pesquisador**: conversa média (8-15 msgs) E sem fechamento
    5. **Indeciso**: padrão para os demais

    Cada regra produz um score de confiança (0-1).

    Args:
        df: DataFrame Silver conversations.

    Returns:
        DataFrame com colunas: persona, persona_confianca, persona_fatores.
    """
    # Scoring por persona — cada conversa recebe scores em todas as dimensões

    # Fantasma: ghosting + baixo engajamento do lead
    df = df.with_columns(
        pl.when(
            (pl.col("conversation_outcome") == "ghosting")
            & (pl.col("inbound_messages") <= 3)
        )
        .then(0.9)
        .when(pl.col("conversation_outcome") == "ghosting")
        .then(0.7)
        .when(
            (pl.col("inbound_messages") <= 1)
            & (pl.col("total_messages") >= 3)
        )
        .then(0.6)
        .otherwise(0.0)
        .alias("_score_fantasma")
    )

    # Decidido: venda fechada + conversa não muito longa
    df = df.with_columns(
        pl.when(
            (pl.col("conversation_outcome") == "venda_fechada")
            & (pl.col("total_messages") <= 12)
        )
        .then(0.9)
        .when(pl.col("conversation_outcome") == "venda_fechada")
        .then(0.6)
        .otherwise(0.0)
        .alias("_score_decidido")
    )

    # Negociador: conversa longa + menção concorrente + perdido_preco
    df = df.with_columns(
        (
            pl.when(pl.col("total_messages") > 20).then(0.4).otherwise(0.0)
            + pl.when(pl.col("total_messages") > 15).then(0.2).otherwise(0.0)
            + pl.when(pl.col("competitor_mentioned").is_not_null()).then(0.3).otherwise(0.0)
            + pl.when(pl.col("conversation_outcome") == "perdido_preco").then(0.2).otherwise(0.0)
            + pl.when(pl.col("conversation_outcome") == "perdido_concorrente").then(0.2).otherwise(0.0)
        )
        .clip(0.0, 1.0)
        .alias("_score_negociador")
    )

    # Pesquisador: conversa média, sem fechamento, alto engajamento
    df = df.with_columns(
        (
            pl.when(
                (pl.col("total_messages") >= 8)
                & (pl.col("total_messages") <= 20)
            )
            .then(0.4)
            .otherwise(0.0)
            + pl.when(
                pl.col("conversation_outcome").is_in([
                    "proposta_enviada", "em_negociacao", "desistencia_lead"
                ])
            )
            .then(0.3)
            .otherwise(0.0)
            + pl.when(
                pl.col("inbound_messages") >= 4
            )
            .then(0.2)
            .otherwise(0.0)
        )
        .clip(0.0, 1.0)
        .alias("_score_pesquisador")
    )

    # Indeciso: score residual
    df = df.with_columns(
        pl.when(
            (pl.col("conversation_outcome").is_in([
                "proposta_enviada", "em_negociacao"
            ]))
            & (pl.col("total_messages") >= 5)
        )
        .then(0.5)
        .otherwise(0.2)
        .alias("_score_indeciso")
    )

    # Selecionar persona com maior score
    score_cols = [
        "_score_fantasma",
        "_score_decidido",
        "_score_negociador",
        "_score_pesquisador",
        "_score_indeciso",
    ]
    persona_names = ["Fantasma", "Decidido", "Negociador", "Pesquisador", "Indeciso"]

    # Usar concat horizontal dos scores para encontrar o máximo
    df = df.with_columns(
        pl.concat_list(score_cols).alias("_scores_list")
    )

    df = df.with_columns(
        pl.col("_scores_list").list.eval(pl.element().arg_max()).list.first().alias("_max_idx"),
        pl.col("_scores_list").list.max().alias("persona_confianca"),
    )

    # Mapear índice para nome da persona
    persona_map = pl.DataFrame({
        "_max_idx": list(range(len(persona_names))),
        "persona": persona_names,
    }).with_columns(pl.col("_max_idx").cast(pl.UInt32))

    df = df.with_columns(pl.col("_max_idx").cast(pl.UInt32))
    df = df.join(persona_map, on="_max_idx", how="left")
    df = df.with_columns(pl.col("persona").fill_null("Indeciso"))

    # Fatores para explicabilidade
    df = df.with_columns(
        pl.concat_str([
            pl.lit("fan="),
            pl.col("_score_fantasma").round(2).cast(pl.Utf8),
            pl.lit(" dec="),
            pl.col("_score_decidido").round(2).cast(pl.Utf8),
            pl.lit(" neg="),
            pl.col("_score_negociador").round(2).cast(pl.Utf8),
            pl.lit(" pes="),
            pl.col("_score_pesquisador").round(2).cast(pl.Utf8),
            pl.lit(" ind="),
            pl.col("_score_indeciso").round(2).cast(pl.Utf8),
        ]).alias("persona_fatores")
    )

    # Limpar temporários
    df = df.drop(score_cols + ["_scores_list", "_max_idx"])

    return df


def gerar_gold_personas(
    silver_conversations_path: str | Path,
    gold_personas_path: str | Path,
) -> dict[str, Any]:
    """Gera a tabela Gold de personas a partir das conversas Silver.

    Args:
        silver_conversations_path: Caminho da tabela Silver conversations.
        gold_personas_path: Caminho destino da tabela Gold personas.

    Returns:
        Estatísticas com contagens por persona.
    """
    silver_conversations_path = str(silver_conversations_path)
    gold_personas_path = str(gold_personas_path)
    storage = get_storage_backend()

    logger.info("lendo_silver_conversations_para_personas",
                extra={"path": silver_conversations_path})
    df = storage.read_table(silver_conversations_path)

    df = classificar_personas(df)

    gold_df = df.select([
        "conversation_id",
        "persona",
        "persona_confianca",
        "persona_fatores",
        "conversation_outcome",
        "total_messages",
        "inbound_messages",
        "outbound_messages",
        "competitor_mentioned",
        "agent_id",
        "campaign_id",
        "lead_source",
        "conversation_date",
    ])

    storage.write_table(gold_df, gold_personas_path, mode="overwrite", schema_mode="overwrite")

    # Estatísticas por persona
    stats: dict[str, Any] = {
        "total_conversas": len(gold_df),
        "rows_written": len(gold_df),
    }
    for persona_nome in ["Decidido", "Pesquisador", "Negociador", "Fantasma", "Indeciso"]:
        stats[persona_nome.lower()] = gold_df.filter(
            pl.col("persona") == persona_nome
        ).height

    logger.info("gold_personas_gerado", extra=stats)
    return stats
