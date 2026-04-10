"""Ferramentas de dados — leitura, validação e amostragem de tabelas Delta.

Utilizadas pelos agentes para inspecionar o estado das tabelas em cada
camada do pipeline (Bronze, Silver, Gold) sem conhecer detalhes internos
do storage backend.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from config.settings import get_settings
from core.storage import StorageBackend, get_storage_backend

logger = structlog.get_logger(__name__)


def listar_tabelas() -> dict[str, dict[str, Any]]:
    """Lista todas as tabelas conhecidas e seu status (existe / linhas / versão).

    Returns:
        Dicionário {nome_tabela: {path, exists, rows, version}} para cada
        tabela configurada nas settings.
    """
    settings = get_settings()
    storage = get_storage_backend()

    tabelas: dict[str, str] = {
        "bronze": settings.bronze_path,
        "silver_messages": settings.silver_messages_path,
        "silver_conversations": settings.silver_conversations_path,
        "gold_sentiment": settings.gold_sentiment_path,
        "gold_personas": settings.gold_personas_path,
        "gold_segmentation": settings.gold_segmentation_path,
        "gold_analytics": settings.gold_analytics_path,
        "gold_vendor": settings.gold_vendor_path,
    }

    resultado: dict[str, dict[str, Any]] = {}
    for nome, path in tabelas.items():
        info: dict[str, Any] = {"path": path, "exists": False, "rows": 0, "version": -1}
        try:
            if storage.table_exists(path):
                info["exists"] = True
                info["rows"] = storage.get_table_row_count(path)
                info["version"] = storage.get_table_version(path)
        except Exception as e:
            logger.warning("tabela_check_falhou", tabela=nome, erro=str(e))
        resultado[nome] = info

    return resultado


def ler_tabela(nome_ou_path: str, limite: int = 100) -> pl.DataFrame:
    """Lê até *limite* linhas de uma tabela Delta.

    Args:
        nome_ou_path: Nome lógico (ex: 'bronze', 'silver_messages') ou path direto.
        limite: Máximo de linhas a retornar.

    Returns:
        DataFrame Polars com as linhas lidas.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    return df.head(limite)


def obter_schema(nome_ou_path: str) -> dict[str, str]:
    """Retorna o schema da tabela como {coluna: tipo}.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.

    Returns:
        Dicionário coluna → tipo (string).
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    return {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}


def amostrar_tabela(nome_ou_path: str, n: int = 5) -> list[dict[str, Any]]:
    """Retorna N linhas aleatórias da tabela como lista de dicts.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.
        n: Número de linhas a amostrar.

    Returns:
        Lista de dicionários, um por linha.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    df = storage.read_table(path)
    if len(df) <= n:
        return df.to_dicts()
    return df.sample(n=n, seed=42).to_dicts()


def contar_linhas(nome_ou_path: str) -> int:
    """Retorna a contagem de linhas de uma tabela.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.

    Returns:
        Número de linhas.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    return storage.get_table_row_count(path)


def tabela_existe(nome_ou_path: str) -> bool:
    """Verifica se uma tabela existe.

    Args:
        nome_ou_path: Nome lógico ou path da tabela.

    Returns:
        True se a tabela existe.
    """
    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()
    return storage.table_exists(path)


# ─── Helpers ────────────────────────────────────────────────────────────────

_TABLE_NAMES: dict[str, str] | None = None


def _get_table_map() -> dict[str, str]:
    """Cache lazy do mapeamento nome lógico → path."""
    global _TABLE_NAMES
    if _TABLE_NAMES is None:
        s = get_settings()
        _TABLE_NAMES = {
            "bronze": s.bronze_path,
            "silver_messages": s.silver_messages_path,
            "silver_conversations": s.silver_conversations_path,
            "gold_sentiment": s.gold_sentiment_path,
            "gold_personas": s.gold_personas_path,
            "gold_segmentation": s.gold_segmentation_path,
            "gold_analytics": s.gold_analytics_path,
            "gold_vendor": s.gold_vendor_path,
        }
    return _TABLE_NAMES


def _resolver_path(nome_ou_path: str) -> str:
    """Resolve nome lógico para path, ou retorna o path diretamente."""
    mapa = _get_table_map()
    return mapa.get(nome_ou_path, nome_ou_path)
