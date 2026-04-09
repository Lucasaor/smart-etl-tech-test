"""Especificação do projeto — define dados de entrada, dicionário e KPIs.

O sistema recebe três entradas obrigatórias:
  1. Amostra dos dados brutos (caminho para arquivo parquet/csv) → camada Bronze
  2. Dicionário de dados (markdown) → orienta limpeza e extração na Silver
  3. Descrição dos KPIs desejados (markdown) → orienta a geração da camada Gold

Os agentes usam estas especificações para gerar e manter o pipeline de
forma autônoma.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl
import structlog

logger = structlog.get_logger(__name__)

# Nomes dos arquivos padrão dentro do diretório de specs
_SPEC_META_FILE = "spec_meta.json"
_DICIONARIO_FILE = "dicionario_dados.md"
_KPIS_FILE = "descricao_kpis.md"
_AMOSTRA_FILE = "dados_brutos.parquet"


@dataclass
class ColunaInfo:
    """Informação sobre uma coluna detectada na amostra de dados."""

    nome: str
    tipo: str
    nulos: int = 0
    nulos_pct: float = 0.0
    valores_unicos: int = 0
    exemplo: str = ""


@dataclass
class AnaliseAmostra:
    """Resultado da análise automática da amostra de dados brutos."""

    num_linhas: int = 0
    num_colunas: int = 0
    colunas: list[ColunaInfo] = field(default_factory=list)
    tamanho_bytes: int = 0
    timestamp_analise: str = ""

    def resumo(self) -> str:
        """Retorna um resumo textual da análise."""
        linhas = [
            f"Linhas: {self.num_linhas:,}",
            f"Colunas: {self.num_colunas}",
            f"Tamanho: {self.tamanho_bytes / 1024 / 1024:.1f} MB",
            "",
            "Colunas detectadas:",
        ]
        for col in self.colunas:
            linhas.append(
                f"  - {col.nome} ({col.tipo}) — "
                f"{col.valores_unicos} únicos, {col.nulos_pct:.1f}% nulos"
            )
        return "\n".join(linhas)


@dataclass
class ProjectSpec:
    """Especificação completa de um projeto de pipeline.

    Armazena as três entradas obrigatórias do sistema e os metadados
    derivados da análise automática da amostra de dados.
    """

    nome: str = "projeto_pipeline"
    dados_brutos_path: str = ""
    dicionario_dados: str = ""
    descricao_kpis: str = ""
    formato_dados: str = "parquet"
    criado_em: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    analise: AnaliseAmostra | None = None

    def validar(self) -> list[str]:
        """Valida se a spec contém todas as entradas obrigatórias.

        Retorna lista de problemas encontrados (vazia = tudo OK).
        """
        problemas: list[str] = []
        if not self.dados_brutos_path:
            problemas.append("Caminho para dados brutos não informado")
        elif not Path(self.dados_brutos_path).exists():
            problemas.append(
                f"Arquivo de dados brutos não encontrado: {self.dados_brutos_path}"
            )
        if not self.dicionario_dados.strip():
            problemas.append("Dicionário de dados está vazio")
        if not self.descricao_kpis.strip():
            problemas.append("Descrição dos KPIs está vazia")
        return problemas

    @property
    def is_valida(self) -> bool:
        """Retorna True se a spec é válida (sem problemas)."""
        return len(self.validar()) == 0


def analisar_amostra(caminho: str) -> AnaliseAmostra:
    """Analisa a amostra de dados brutos e retorna estatísticas do schema.

    Args:
        caminho: Caminho para o arquivo de dados (parquet ou csv).

    Returns:
        AnaliseAmostra com informações sobre linhas, colunas e tipos.
    """
    caminho_path = Path(caminho)
    if not caminho_path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    logger.info("analisando_amostra", caminho=caminho)

    if caminho.endswith(".csv"):
        df = pl.read_csv(caminho)
    else:
        df = pl.read_parquet(caminho)

    colunas: list[ColunaInfo] = []
    for col_nome in df.columns:
        serie = df[col_nome]
        nulos = serie.null_count()
        total = len(serie)
        exemplo_val = ""
        non_null = serie.drop_nulls()
        if len(non_null) > 0:
            exemplo_val = str(non_null[0])

        colunas.append(
            ColunaInfo(
                nome=col_nome,
                tipo=str(serie.dtype),
                nulos=nulos,
                nulos_pct=round((nulos / total * 100) if total > 0 else 0.0, 2),
                valores_unicos=serie.n_unique(),
                exemplo=exemplo_val[:100],
            )
        )

    tamanho = caminho_path.stat().st_size

    analise = AnaliseAmostra(
        num_linhas=len(df),
        num_colunas=len(df.columns),
        colunas=colunas,
        tamanho_bytes=tamanho,
        timestamp_analise=datetime.now(timezone.utc).isoformat(),
    )

    logger.info(
        "amostra_analisada",
        linhas=analise.num_linhas,
        colunas=analise.num_colunas,
        tamanho_mb=round(tamanho / 1024 / 1024, 2),
    )
    return analise


def salvar_spec(spec: ProjectSpec, spec_dir: str) -> str:
    """Salva a especificação do projeto em um diretório.

    Grava três arquivos:
      - spec_meta.json: metadados e análise da amostra
      - dicionario_dados.md: dicionário de dados
      - descricao_kpis.md: descrição dos KPIs

    Args:
        spec: Especificação do projeto.
        spec_dir: Diretório onde os arquivos serão salvos.

    Returns:
        Caminho do diretório da spec.
    """
    dir_path = Path(spec_dir)
    dir_path.mkdir(parents=True, exist_ok=True)

    # Metadados (JSON)
    meta = {
        "nome": spec.nome,
        "dados_brutos_path": spec.dados_brutos_path,
        "formato_dados": spec.formato_dados,
        "criado_em": spec.criado_em,
        "analise": asdict(spec.analise) if spec.analise else None,
    }
    meta_path = dir_path / _SPEC_META_FILE
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Dicionário de dados
    dict_path = dir_path / _DICIONARIO_FILE
    dict_path.write_text(spec.dicionario_dados, encoding="utf-8")

    # KPIs
    kpis_path = dir_path / _KPIS_FILE
    kpis_path.write_text(spec.descricao_kpis, encoding="utf-8")

    logger.info("spec_salva", diretorio=spec_dir)
    return str(dir_path)


def carregar_spec(spec_dir: str) -> ProjectSpec:
    """Carrega uma especificação do projeto a partir do diretório.

    Args:
        spec_dir: Diretório contendo os arquivos da spec.

    Returns:
        ProjectSpec reconstituído.

    Raises:
        FileNotFoundError: Se o diretório ou arquivo de metadados não existir.
    """
    dir_path = Path(spec_dir)
    if not dir_path.exists():
        raise FileNotFoundError(f"Diretório de spec não encontrado: {spec_dir}")

    meta_path = dir_path / _SPEC_META_FILE
    if not meta_path.exists():
        raise FileNotFoundError(f"Arquivo de metadados não encontrado: {meta_path}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    # Dicionário de dados
    dict_path = dir_path / _DICIONARIO_FILE
    dicionario = dict_path.read_text(encoding="utf-8") if dict_path.exists() else ""

    # KPIs
    kpis_path = dir_path / _KPIS_FILE
    kpis = kpis_path.read_text(encoding="utf-8") if kpis_path.exists() else ""

    # Análise
    analise = None
    if meta.get("analise"):
        analise_data = meta["analise"]
        colunas = [ColunaInfo(**c) for c in analise_data.get("colunas", [])]
        analise = AnaliseAmostra(
            num_linhas=analise_data.get("num_linhas", 0),
            num_colunas=analise_data.get("num_colunas", 0),
            colunas=colunas,
            tamanho_bytes=analise_data.get("tamanho_bytes", 0),
            timestamp_analise=analise_data.get("timestamp_analise", ""),
        )

    spec = ProjectSpec(
        nome=meta.get("nome", "projeto_pipeline"),
        dados_brutos_path=meta.get("dados_brutos_path", ""),
        dicionario_dados=dicionario,
        descricao_kpis=kpis,
        formato_dados=meta.get("formato_dados", "parquet"),
        criado_em=meta.get("criado_em", ""),
        analise=analise,
    )

    logger.info("spec_carregada", nome=spec.nome, diretorio=spec_dir)
    return spec


def spec_existe(spec_dir: str) -> bool:
    """Verifica se existe uma spec salva no diretório indicado."""
    meta_path = Path(spec_dir) / _SPEC_META_FILE
    return meta_path.exists()
