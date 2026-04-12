"""Helpers compartilhados para bootstrap de notebooks Databricks.

Colocado em `config` para evitar conflitos de nome de módulo com o pacote
externo `databricks` SDK.

compatível com Databricks Free Edition (compute serverless, Unity Catalog
Volumes). Todos os caminhos utilizam UC Volumes em vez de DBFS.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Caminho padrão do UC Volume para dados do pipeline.
# Formato: /Volumes/<catalog>/<schema>/<volume>/<path>
# O catálogo é detectado automaticamente em tempo de execução via detect_volume_root().
_DEFAULT_VOLUME_TEMPLATE = "/Volumes/{catalog}/default/pipeline_data"


def detect_volume_root() -> str:
    """Detecta automaticamente o caminho raiz do UC Volume consultando os catálogos disponíveis.

    No Databricks Free Edition o catálogo padrão frequentemente NÃO é 'main'
    (ex: 'workspace'). Esta função consulta o Spark SQL para encontrar o correto.
    Retorna 'main' como fallback se a detecção falhar.
    """
    try:
        # spark está disponível globalmente nos notebooks Databricks
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        rows = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [r[0] for r in rows]
        # Filtrar catálogos do sistema
        user_catalogs = [
            c for c in catalog_names
            if c not in ("system", "hive_metastore", "samples") and not c.startswith("__")
        ]
        if "main" in user_catalogs:
            catalog = "main"
        elif user_catalogs:
            catalog = user_catalogs[0]
        else:
            catalog = "main"
    except Exception:
        catalog = "main"

    return _DEFAULT_VOLUME_TEMPLATE.format(catalog=catalog)


# Inicialização lazy; computado no primeiro uso por bootstrap_notebook / default_paths
DEFAULT_VOLUME_ROOT: str | None = None


def _get_volume_root() -> str:
    """Retorna o caminho raiz do volume, detectando automaticamente na primeira chamada."""
    global DEFAULT_VOLUME_ROOT
    if DEFAULT_VOLUME_ROOT is None:
        DEFAULT_VOLUME_ROOT = detect_volume_root()
    return DEFAULT_VOLUME_ROOT


def detect_repo_path(explicit_repo_path: str | None = None) -> str:
    """Retorna o caminho do repositório dentro do Databricks Workspace quando possível."""
    candidates = [
        explicit_repo_path,
        os.getenv("PROJECT_REPO_PATH"),
        "/Workspace/Repos/agentic-pipeline",
        "/Workspace/Repos/Lucasaor/smart-etl-tech-test",
    ]

    for candidate in candidates:
        if not candidate:
            continue
        marker = Path(candidate) / "config" / "settings.py"
        if marker.exists():
            return str(Path(candidate))

    return "/Workspace/Repos/agentic-pipeline"


def bootstrap_notebook(
    explicit_repo_path: str | None = None,
    data_root: str | None = None,
) -> dict[str, str]:
    """Configura sys.path e variáveis de ambiente para execução no Databricks.

    No Databricks Free Edition (serverless), o data_root padrão aponta para um
    UC Volume detectado automaticamente a partir do catálogo do workspace.
    """
    if data_root is None:
        data_root = _get_volume_root()

    repo_path = detect_repo_path(explicit_repo_path)

    if repo_path not in sys.path and Path(repo_path).exists():
        sys.path.insert(0, repo_path)

    os.environ["RUNTIME_ENV"] = "databricks"
    os.environ["DATA_ROOT"] = data_root

    return {
        "repo_path": repo_path,
        "data_root": data_root,
    }


def default_paths(data_root: str | None = None) -> dict[str, str]:
    """Retorna os caminhos canônicos de cada camada para notebooks Databricks.

    Todos os caminhos apontam para Unity Catalog Volumes (caminhos estilo POSIX).
    """
    if data_root is None:
        data_root = _get_volume_root()

    return {
        "specs": f"{data_root}/specs",
        "bronze": f"{data_root}/bronze",
        "silver": f"{data_root}/silver",
        "silver_messages": f"{data_root}/silver/messages",
        "silver_conversations": f"{data_root}/silver/conversations",
        "gold": f"{data_root}/gold",
        "gold_sentiment": f"{data_root}/gold/sentiment",
        "gold_personas": f"{data_root}/gold/personas",
        "gold_segmentation": f"{data_root}/gold/segmentation",
        "gold_analytics": f"{data_root}/gold/analytics",
        "gold_vendor_analysis": f"{data_root}/gold/vendor_analysis",
        "monitoring": f"{data_root}/monitoring",
    }


def notebook_workspace_path(repo_path: str, notebook_filename: str) -> str:
    """Constrói o caminho de notebook no workspace aceito por dbutils.notebook.run."""
    stem = notebook_filename.removesuffix(".py")
    return f"{repo_path}/databricks/notebooks/{stem}"


# ---------------------------------------------------------------------------
# Helpers de resultado estruturado — notebooks devem usar estes para emitir
# resultados que o orquestrador e o monitoramento possam consumir.
# ---------------------------------------------------------------------------


def make_notebook_result(
    notebook_name: str,
    status: str = "sucesso",
    rows_written: int = 0,
    tables_written: list[str] | None = None,
    error: str | None = None,
    extra: dict | None = None,
) -> str:
    """Constrói uma string JSON de resultado para ``dbutils.notebook.exit()``.

    Exemplo de uso ao final de cada notebook::

        dbutils.notebook.exit(make_notebook_result(
            "01_bronze", rows_written=150347,
            tables_written=["/Volumes/main/default/pipeline_data/bronze"],
        ))
    """
    payload: dict = {
        "notebook": notebook_name,
        "status": status,
        "rows_written": rows_written,
        "tables_written": tables_written or [],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        payload["error"] = error
    if extra:
        payload.update(extra)
    return json.dumps(payload)


def parse_notebook_result(raw: str | None) -> dict:
    """Faz o parse de uma string JSON retornada por ``dbutils.notebook.run()``."""
    if not raw:
        return {"status": "desconhecido", "error": "resultado vazio"}
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {"status": "desconhecido", "raw": str(raw)}


def save_run_metadata(
    spark_or_path,
    notebook_name: str,
    status: str,
    duration_sec: float,
    rows_written: int = 0,
    error: str | None = None,
) -> None:
    """Adiciona um registro de metadata de execução à tabela Delta de monitoramento.

    Funciona tanto com uma sessão Spark (Databricks) quanto com um caminho
    string (testes locais via deltalake).

    Notebooks podem chamar esta função ao final da execução para persistir uma
    trilha de auditoria independente do orquestrador.
    """
    record = {
        "notebook": notebook_name,
        "status": status,
        "duration_sec": duration_sec,
        "rows_written": rows_written,
        "error": error,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Tenta o caminho via Spark primeiro (runtime Databricks)
    try:
        from pyspark.sql import SparkSession

        if isinstance(spark_or_path, str):
            spark = SparkSession.getActiveSession()
            monitoring_path = f"{spark_or_path}/monitoring/notebook_runs"
        else:
            spark = spark_or_path
            monitoring_path = f"{_get_volume_root()}/monitoring/notebook_runs"

        if spark is not None:
            df = spark.createDataFrame([record])
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(monitoring_path)
            return
    except ImportError:
        pass

    # Fallback: usa a biblioteca deltalake diretamente
    import polars as pl
    from deltalake import write_deltalake

    if isinstance(spark_or_path, str):
        monitoring_path = f"{spark_or_path}/monitoring/notebook_runs"
    else:
        monitoring_path = "./data/monitoring/notebook_runs"
    Path(monitoring_path).parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame([record])
    write_deltalake(monitoring_path, df.to_arrow(), mode="append", schema_mode="merge")
