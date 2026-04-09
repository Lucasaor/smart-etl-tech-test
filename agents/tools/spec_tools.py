"""Ferramentas para análise de especificações — usadas pelos agentes.

Fornece funções utilitárias para os agentes inspecionarem os dados
de entrada, o dicionário e os KPIs do projeto.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from pipeline.specs import ProjectSpec, analisar_amostra

logger = structlog.get_logger(__name__)


def obter_preview_dados(caminho: str, num_linhas: int = 5) -> list[dict[str, Any]]:
    """Retorna as primeiras N linhas do arquivo de dados como dicts.

    Args:
        caminho: Caminho para o arquivo parquet ou csv.
        num_linhas: Número de linhas a retornar.

    Returns:
        Lista de dicionários, um por linha.
    """
    if caminho.endswith(".csv"):
        df = pl.read_csv(caminho, n_rows=num_linhas)
    else:
        df = pl.read_parquet(caminho)
        df = df.head(num_linhas)

    return df.to_dicts()


def obter_estatisticas_colunas(caminho: str) -> dict[str, Any]:
    """Calcula estatísticas descritivas das colunas numéricas.

    Args:
        caminho: Caminho para o arquivo de dados.

    Returns:
        Dicionário com estatísticas por coluna numérica.
    """
    if caminho.endswith(".csv"):
        df = pl.read_csv(caminho)
    else:
        df = pl.read_parquet(caminho)

    stats: dict[str, Any] = {}
    for col_nome in df.columns:
        serie = df[col_nome]
        col_stats: dict[str, Any] = {
            "tipo": str(serie.dtype),
            "nulos": serie.null_count(),
            "unicos": serie.n_unique(),
        }

        if serie.dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8):
            non_null = serie.drop_nulls()
            if len(non_null) > 0:
                col_stats["min"] = float(non_null.min())
                col_stats["max"] = float(non_null.max())
                col_stats["media"] = float(non_null.mean())

        stats[col_nome] = col_stats

    return stats


def validar_spec_completa(spec: ProjectSpec) -> dict[str, Any]:
    """Validação completa da spec com detalhes para o agente.

    Args:
        spec: Especificação do projeto.

    Returns:
        Dicionário com resultado da validação e detalhes.
    """
    problemas = spec.validar()
    return {
        "valida": len(problemas) == 0,
        "problemas": problemas,
        "tem_dados": bool(spec.dados_brutos_path),
        "tem_dicionario": bool(spec.dicionario_dados.strip()),
        "tem_kpis": bool(spec.descricao_kpis.strip()),
        "tem_analise": spec.analise is not None,
        "num_colunas": spec.analise.num_colunas if spec.analise else 0,
        "num_linhas": spec.analise.num_linhas if spec.analise else 0,
    }
