"""Executor de código gerado — executa dinamicamente os scripts de pipeline.

Recebe código Python gerado pelo CodeGenAgent e o executa de forma segura,
provendo as funções read_table/write_table e o dicionário de settings.

Cada camada é executada via exec() com namespace isolado. O resultado
é capturado via convenção: a função `run()` deve retornar um dict.
"""

from __future__ import annotations

import traceback
from typing import Any, Callable, Literal

import polars as pl
import structlog

from config.settings import get_settings
from core.storage import get_storage_backend

logger = structlog.get_logger(__name__)


def _normalize_timestamps(df: pl.DataFrame) -> pl.DataFrame:
    """Cast all Datetime columns to μs precision.

    Delta Lake / PySpark use microsecond timestamps.  Polars and Pandas may
    introduce nanosecond precision during reads, causing schema-mismatch
    errors on subsequent writes or concat operations.
    """
    casts = [
        pl.col(name).cast(pl.Datetime("us", dtype.time_zone))  # type: ignore[union-attr]
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_unit != "us"
    ]
    return df.with_columns(casts) if casts else df


class CodeExecutionError(Exception):
    """Erro na execução de código gerado."""

    def __init__(self, message: str, code: str = "", traceback_str: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.traceback_str = traceback_str


def _make_read_source() -> Callable:
    """Cria wrapper para leitura de arquivos fonte (Parquet/CSV brutos).

    Diferente de read_table() que lê Delta tables, esta função lê
    arquivos Parquet ou CSV comuns — usada pela camada Bronze para
    ler os dados brutos de entrada.
    """

    def read_source(path: str) -> pl.DataFrame:
        from pathlib import Path as _P
        p = _P(path)
        if not p.exists():
            raise FileNotFoundError(f"Arquivo fonte não encontrado: {path}")
        if p.suffix == ".csv":
            logger.info("reading_source_csv", path=path)
            return pl.read_csv(path)
        logger.info("reading_source_parquet", path=path)
        return _normalize_timestamps(pl.read_parquet(path))

    return read_source


def _make_read_table() -> Callable:
    """Cria wrapper de leitura que usa o storage backend configurado.

    Tenta ler como Delta table primeiro; se falhar (ex: arquivo Parquet
    simples), faz fallback para leitura direta via Polars.
    """
    backend = get_storage_backend()

    def read_table(path: str, version: int | None = None) -> pl.DataFrame:
        try:
            return _normalize_timestamps(backend.read_table(path, version=version))
        except Exception as delta_err:
            # Fallback: tentar ler como Parquet simples
            from pathlib import Path as _P
            p = _P(path)
            if p.exists() and p.suffix == ".parquet":
                logger.warning(
                    "delta_read_fallback_parquet",
                    path=path,
                    delta_error=str(delta_err),
                )
                return _normalize_timestamps(pl.read_parquet(path))
            # Tentar como diretório com parquet files dentro
            if p.is_dir():
                parquets = list(p.glob("*.parquet"))
                if parquets:
                    logger.warning(
                        "delta_read_fallback_dir_parquet",
                        path=path,
                        n_files=len(parquets),
                    )
                    return _normalize_timestamps(pl.read_parquet(str(p / "*.parquet")))
            raise

    return read_table


def _make_write_table() -> Callable:
    """Cria wrapper de escrita que usa o storage backend configurado."""
    backend = get_storage_backend()

    def write_table(
        df: pl.DataFrame,
        path: str,
        mode: Literal["overwrite", "append"] = "overwrite",
    ) -> None:
        backend.write_table(_normalize_timestamps(df), path, mode=mode)

    return write_table


def _resolve_path(path: str) -> str:
    """Resolve um path relativo para absoluto a partir do diretório de trabalho."""
    from pathlib import Path as _P
    p = _P(path)
    if not p.is_absolute():
        p = _P.cwd() / p
    return str(p.resolve())


def build_settings_dict(
    spec_dados_path: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Constrói o dicionário de settings para injetar no código gerado.

    Contém todos os paths e parâmetros que o código gerado precisa.
    Todos os paths são resolvidos para absolutos para evitar problemas
    com diretório de trabalho.
    """
    settings = get_settings()

    raw_source = spec_dados_path or settings.bronze_source_path

    s = {
        "source_path": _resolve_path(raw_source),
        "formato": "parquet",
        "bronze_path": _resolve_path(settings.bronze_path),
        "silver_messages_path": _resolve_path(settings.silver_messages_path),
        "silver_conversations_path": _resolve_path(settings.silver_conversations_path),
        "gold_path": _resolve_path(settings.gold_path),
        "data_root": _resolve_path(settings.data_root),
    }

    # Adicionar paths Gold dinâmicos
    gold_base = _resolve_path(settings.gold_path)
    s["gold_base_path"] = gold_base

    if extra:
        s.update(extra)

    logger.info("settings_dict_built", source_path=s["source_path"], bronze_path=s["bronze_path"])
    return s


def execute_generated_code(
    code: str,
    settings_dict: dict[str, Any],
    label: str = "unknown",
) -> dict[str, Any]:
    """Executa código gerado dinamicamente.

    O código deve definir uma função `run(read_table, write_table, settings) -> dict`.

    Args:
        code: Código Python gerado pelo CodeGenAgent.
        settings_dict: Dicionário de configuração/paths para o código.
        label: Label identificador para logs.

    Returns:
        Resultado da execução (dict retornado pela função run).

    Raises:
        CodeExecutionError: Se o código falhar na compilação ou execução.
    """
    logger.info("executando_codigo_gerado", label=label, code_lines=code.count("\n") + 1)

    if not code.strip():
        raise CodeExecutionError("Código vazio — nada para executar", code=code)

    # Guard: reject generated code that imports deltalake directly.
    # On Databricks UC Volumes, the deltalake Python library fails because
    # Volumes don't support atomic rename. All Delta I/O must go through
    # the write_table/read_table wrappers (which use the correct backend).
    import re as _re
    if _re.search(r"\b(from\s+deltalake|import\s+deltalake)\b", code):
        raise CodeExecutionError(
            f"Código gerado ({label}) importa 'deltalake' diretamente. "
            "Use write_table()/read_table() passadas como parâmetro em vez de "
            "importar deltalake. Isso é necessário para compatibilidade com "
            "Databricks UC Volumes.",
            code=code,
        )

    # Namespace isolado com imports permitidos
    namespace: dict[str, Any] = {
        "__builtins__": __builtins__,
    }

    # 1. Compilar e executar definições (def run, helpers, etc)
    try:
        compiled = compile(code, f"<generated_{label}>", "exec")
        exec(compiled, namespace)
    except SyntaxError as e:
        raise CodeExecutionError(
            f"Erro de sintaxe no código gerado ({label}): {e}",
            code=code,
            traceback_str=traceback.format_exc(),
        )
    except Exception as e:
        raise CodeExecutionError(
            f"Erro ao compilar código gerado ({label}): {e}",
            code=code,
            traceback_str=traceback.format_exc(),
        )

    # 2. Verificar que a função run() foi definida
    if "run" not in namespace or not callable(namespace["run"]):
        raise CodeExecutionError(
            f"Código gerado ({label}) não define uma função 'run()' chamável",
            code=code,
        )

    # 3. Executar run()
    read_table = _make_read_table()
    write_table = _make_write_table()
    read_source = _make_read_source()

    # Injetar read_source no namespace para que o código gerado possa usá-la
    namespace["read_source"] = read_source

    try:
        result = namespace["run"](read_table, write_table, settings_dict)
    except Exception as e:
        raise CodeExecutionError(
            f"Erro na execução do código gerado ({label}): {type(e).__name__}: {e}",
            code=code,
            traceback_str=traceback.format_exc(),
        )

    if not isinstance(result, dict):
        result = {"rows_written": 0, "raw_result": str(result)}

    logger.info(
        "codigo_gerado_executado",
        label=label,
        rows_written=result.get("rows_written", 0),
        status=result.get("status", "ok"),
    )

    return result


def execute_generated_tests(
    test_code: str,
    pipeline_code: str,
    label: str = "unknown",
) -> dict[str, Any]:
    """Executa testes gerados para validar o código de pipeline.

    Roda os testes em processo usando pytest. Retorna resultado.

    Args:
        test_code: Código dos testes pytest.
        pipeline_code: Código da camada sendo testada (contexto).
        label: Label identificador.

    Returns:
        Dict com resultado dos testes: passed, failed, errors.
    """
    if not test_code.strip():
        return {"status": "skipped", "reason": "Sem testes para executar"}

    import tempfile
    from pathlib import Path

    logger.info("executando_testes_gerados", label=label)

    # Escrever código e testes em arquivos temporários
    with tempfile.TemporaryDirectory() as tmpdir:
        # Escrever o código da camada como módulo importável
        code_file = Path(tmpdir) / f"{label}_code.py"
        code_file.write_text(pipeline_code, encoding="utf-8")

        # Escrever os testes
        test_file = Path(tmpdir) / f"test_{label}.py"
        # Injetar import do módulo gerado no início dos testes
        import_header = f"import sys; sys.path.insert(0, {tmpdir!r})\n"
        test_code_with_import = import_header + test_code
        test_file.write_text(test_code_with_import, encoding="utf-8")

        # Rodar pytest
        try:
            import pytest

            exit_code = pytest.main([
                str(test_file),
                "-v",
                "--tb=short",
                "--no-header",
                "-q",
            ])

            status = "passed" if exit_code == 0 else "failed"
            return {
                "status": status,
                "exit_code": exit_code,
                "test_file": str(test_file),
            }
        except Exception as e:
            logger.warning("erro_testes_gerados", label=label, erro=str(e))
            return {
                "status": "error",
                "error": str(e),
            }
