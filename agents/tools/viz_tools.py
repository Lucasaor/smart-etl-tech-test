"""Ferramentas de visualização — design de gráficos para KPIs Gold.

Utilizada pelos agentes para selecionar e configurar as visualizações
mais adequadas para cada KPI, baseado no schema da tabela e na descrição.
Compatível com Databricks (usa Plotly nativo).
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from agents.tools.data_tools import _resolver_path
from core.storage import get_storage_backend

logger = structlog.get_logger(__name__)


def sugerir_visualizacoes(nome_ou_path: str) -> dict[str, Any]:
    """Analisa uma tabela Gold e sugere as melhores visualizações.

    Examina o schema, tipos de dados e cardinalidade das colunas
    para recomendar os gráficos mais adequados.

    Args:
        nome_ou_path: Nome lógico ou path da tabela Gold.

    Returns:
        Dicionário com sugestões de visualização e metadados.
    """
    from frontend.viz_config import get_viz_specs, render_kpi_charts

    path = _resolver_path(nome_ou_path)
    storage = get_storage_backend()

    try:
        df = storage.read_table(path)
    except Exception as e:
        return {"erro": f"Falha ao ler tabela: {e}", "sugestoes": []}

    table_name = path.rstrip("/").split("/")[-1]

    # Obter specs registradas
    specs = get_viz_specs(table_name)
    kpi_reconhecido = len(specs) > 0

    # Tentar renderizar e ver quais funcionam
    charts = render_kpi_charts(table_name, df)

    # Análise de colunas para sugestões adicionais
    num_cols = [c for c in df.columns if df[c].dtype in (pl.Float32, pl.Float64, pl.Int32, pl.Int64)]
    str_cols = [c for c in df.columns if df[c].dtype == pl.Utf8]
    date_cols = [c for c in df.columns if df[c].dtype in (pl.Date, pl.Datetime)]
    bool_cols = [c for c in df.columns if df[c].dtype == pl.Boolean]

    sugestoes = []
    for titulo, _ in charts:
        sugestoes.append({"titulo": titulo, "status": "funcional"})

    # Sugestões genéricas para colunas não cobertas pelos specs
    if not kpi_reconhecido:
        for col in str_cols:
            n_unique = df[col].n_unique()
            if 2 <= n_unique <= 15:
                sugestoes.append({
                    "titulo": f"Distribuição por {col}",
                    "tipo": "bar" if n_unique > 5 else "pie",
                    "coluna": col,
                    "status": "sugerido",
                })

        for col in num_cols[:3]:
            sugestoes.append({
                "titulo": f"Histograma de {col}",
                "tipo": "histogram",
                "coluna": col,
                "status": "sugerido",
            })

        if date_cols and num_cols:
            sugestoes.append({
                "titulo": f"Série temporal: {num_cols[0]} ao longo de {date_cols[0]}",
                "tipo": "line",
                "colunas": [date_cols[0], num_cols[0]],
                "status": "sugerido",
            })

    return {
        "tabela": table_name,
        "kpi_reconhecido": kpi_reconhecido,
        "total_linhas": len(df),
        "total_colunas": len(df.columns),
        "colunas_numericas": num_cols,
        "colunas_categoricas": str_cols,
        "colunas_temporais": [str(c) for c in date_cols],
        "colunas_booleanas": bool_cols,
        "visualizacoes_funcionais": len(charts),
        "sugestoes": sugestoes,
    }


def listar_kpis_disponiveis() -> dict[str, Any]:
    """Lista todos os KPIs Gold disponíveis e seus tipos de visualização.

    Returns:
        Dicionário com KPIs encontrados e suas visualizações configuradas.
    """
    from pathlib import Path
    from config.settings import get_settings
    from frontend.viz_config import get_viz_specs

    settings = get_settings()
    gold_dir = Path(settings.gold_path)

    if not gold_dir.exists():
        return {"kpis": [], "total": 0}

    storage = get_storage_backend()
    kpis = []

    for sub in sorted(gold_dir.iterdir()):
        if sub.is_dir() and (sub / "_delta_log").exists():
            nome = sub.name
            specs = get_viz_specs(nome)
            try:
                df = storage.read_table(str(sub))
                linhas = len(df) if df is not None else 0
            except Exception:
                linhas = 0

            kpis.append({
                "nome": nome,
                "kpi_reconhecido": len(specs) > 0,
                "visualizacoes_configuradas": len(specs),
                "tipos_graficos": [s.tipo for s in specs],
                "linhas": linhas,
            })

    return {"kpis": kpis, "total": len(kpis)}
