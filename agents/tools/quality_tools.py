"""Ferramentas de qualidade — verificação de nulos, duplicatas, schema e comparações.

Utilizadas pelos agentes (especialmente o repair_agent) para diagnosticar
problemas de qualidade de dados nas tabelas do pipeline.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from agents.tools.data_tools import _resolver_path
from core.storage import get_storage_backend

logger = structlog.get_logger(__name__)


def verificar_nulos(nome_ou_path: str) -> dict[str, Any]:
    """Verifica contagem de nulos por coluna em uma tabela.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.

    Returns:
        Dicionário com contagem de nulos e percentual por coluna.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    total = len(df)

    colunas: dict[str, dict[str, Any]] = {}
    for col in df.columns:
        nulos = df[col].null_count()
        colunas[col] = {
            "nulos": nulos,
            "percentual": round(nulos / total * 100, 2) if total > 0 else 0.0,
        }

    nulos_total = sum(c["nulos"] for c in colunas.values())
    return {
        "tabela": nome_ou_path,
        "total_linhas": total,
        "total_nulos": nulos_total,
        "colunas": colunas,
    }


def verificar_duplicatas(
    nome_ou_path: str,
    colunas_chave: list[str] | None = None,
) -> dict[str, Any]:
    """Verifica duplicatas na tabela, opcionalmente por colunas-chave.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.
        colunas_chave: Colunas para verificar duplicatas. Se None, usa todas.

    Returns:
        Dicionário com contagem de duplicatas e linhas afetadas.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    total = len(df)

    cols = colunas_chave if colunas_chave else df.columns
    # Validar que as colunas existem
    cols_validas = [c for c in cols if c in df.columns]
    if not cols_validas:
        return {
            "tabela": nome_ou_path,
            "total_linhas": total,
            "duplicatas": 0,
            "colunas_verificadas": [],
            "erro": "Nenhuma coluna válida encontrada",
        }

    unicos = df.select(cols_validas).n_unique()
    duplicatas = total - unicos

    return {
        "tabela": nome_ou_path,
        "total_linhas": total,
        "linhas_unicas": unicos,
        "duplicatas": duplicatas,
        "percentual_duplicatas": round(duplicatas / total * 100, 2) if total > 0 else 0.0,
        "colunas_verificadas": cols_validas,
    }


def comparar_schemas(
    tabela_a: str,
    tabela_b: str,
) -> dict[str, Any]:
    """Compara schemas de duas tabelas.

    Args:
        tabela_a: Nome lógico ou path da primeira tabela.
        tabela_b: Nome lógico ou path da segunda tabela.

    Returns:
        Dicionário com colunas em comum, apenas em A, apenas em B e diferenças de tipo.
    """
    path_a = _resolver_path(tabela_a)
    path_b = _resolver_path(tabela_b)
    storage = get_storage_backend()

    df_a = storage.read_table(path_a)
    df_b = storage.read_table(path_b)

    schema_a = {col: str(dtype) for col, dtype in zip(df_a.columns, df_a.dtypes)}
    schema_b = {col: str(dtype) for col, dtype in zip(df_b.columns, df_b.dtypes)}

    cols_a = set(schema_a.keys())
    cols_b = set(schema_b.keys())

    em_comum = cols_a & cols_b
    diferencas_tipo = {
        col: {"tabela_a": schema_a[col], "tabela_b": schema_b[col]}
        for col in em_comum
        if schema_a[col] != schema_b[col]
    }

    return {
        "tabela_a": tabela_a,
        "tabela_b": tabela_b,
        "colunas_em_comum": sorted(em_comum),
        "apenas_tabela_a": sorted(cols_a - cols_b),
        "apenas_tabela_b": sorted(cols_b - cols_a),
        "diferencas_tipo": diferencas_tipo,
        "schemas_identicos": cols_a == cols_b and not diferencas_tipo,
    }


def verificar_integridade(nome_ou_path: str) -> dict[str, Any]:
    """Verificação geral de integridade: nulos, duplicatas e tipos.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.

    Returns:
        Dicionário com resumo de integridade e score de saúde (0-100).
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    total = len(df)

    # Nulos
    nulos_total = sum(df[col].null_count() for col in df.columns)
    pct_nulos = round(nulos_total / (total * len(df.columns)) * 100, 2) if total > 0 and df.columns else 0.0

    # Colunas com muitos nulos (>50%)
    colunas_criticas = [
        col for col in df.columns
        if total > 0 and df[col].null_count() / total > 0.5
    ]

    # Score de saúde (100 = perfeito)
    score = 100.0
    score -= min(pct_nulos * 2, 40)  # Até -40 por nulos
    score -= min(len(colunas_criticas) * 10, 30)  # Até -30 por colunas críticas
    score = max(0.0, round(score, 1))

    return {
        "tabela": nome_ou_path,
        "total_linhas": total,
        "total_colunas": len(df.columns),
        "total_nulos": nulos_total,
        "percentual_nulos": pct_nulos,
        "colunas_criticas_nulos": colunas_criticas,
        "score_saude": score,
        "status": "saudavel" if score >= 70 else "atencao" if score >= 40 else "critico",
    }


def validar_valores(
    nome_ou_path: str,
    coluna: str,
    valores_esperados: list[Any],
) -> dict[str, Any]:
    """Valida que uma coluna contém apenas os valores esperados.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.
        coluna: Nome da coluna a verificar.
        valores_esperados: Lista de valores válidos.

    Returns:
        Dicionário com valores inesperados e contagens.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)

    if coluna not in df.columns:
        return {
            "tabela": nome_ou_path,
            "coluna": coluna,
            "erro": f"Coluna '{coluna}' não encontrada",
        }

    valores_reais = df[coluna].drop_nulls().unique().to_list()
    inesperados = [v for v in valores_reais if v not in valores_esperados]
    ausentes = [v for v in valores_esperados if v not in valores_reais]

    return {
        "tabela": nome_ou_path,
        "coluna": coluna,
        "total_valores_unicos": len(valores_reais),
        "valores_inesperados": inesperados,
        "valores_ausentes": ausentes,
        "valido": len(inesperados) == 0,
    }
