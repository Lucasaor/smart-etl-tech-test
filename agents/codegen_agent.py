"""Agente gerador de código — analisa especificações e gera scripts do pipeline.

O CodeGen Agent recebe uma ProjectSpec (dados brutos + dicionário + KPIs)
e gera os scripts Python para cada camada do pipeline:
  - Bronze: ingestão e validação de schema
  - Silver: limpeza, extração de entidades, agregação
  - Gold: analytics e KPIs conforme descrição fornecida

O código gerado segue o padrão do projeto: documentado em pt-BR,
usa Polars para compute e Delta Lake para storage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import structlog

from agents.llm_provider import LLMProvider
from pipeline.specs import ProjectSpec

logger = structlog.get_logger(__name__)

# ─── Módulos Gold disponíveis ────────────────────────────────────────────────

GOLD_MODULES = {
    "sentiment": {
        "nome": "Análise de Sentimento",
        "modulo": "pipeline.gold.sentiment",
        "funcao": "gerar_gold_sentiment",
        "descricao": "Classifica conversas em sentimento positivo/neutro/negativo via heurísticas comportamentais.",
    },
    "personas": {
        "nome": "Classificação de Personas",
        "modulo": "pipeline.gold.personas",
        "funcao": "gerar_gold_personas",
        "descricao": "Classifica leads em personas (Decidido, Pesquisador, Negociador, Fantasma, Indeciso).",
    },
    "segmentation": {
        "nome": "Segmentação Multidimensional",
        "modulo": "pipeline.gold.segmentation",
        "funcao": "gerar_gold_segmentation",
        "descricao": "Segmenta conversas por engajamento, velocidade, veículo, região, origem e qualificação.",
    },
    "analytics": {
        "nome": "Analytics e KPIs",
        "modulo": "pipeline.gold.analytics",
        "funcao": "gerar_gold_analytics",
        "descricao": "Funil de conversão, lead scoring, performance por campanha.",
    },
    "vendor_analysis": {
        "nome": "Análise por Vendedor",
        "modulo": "pipeline.gold.vendor_analysis",
        "funcao": "gerar_gold_vendor",
        "descricao": "Métricas por vendedor: conversão, tempo de resposta, score de performance.",
    },
}


@dataclass
class AnaliseEspecificacao:
    """Resultado da análise de uma especificação pelo agente."""

    colunas_identificadas: list[str] = field(default_factory=list)
    colunas_com_pii: list[str] = field(default_factory=list)
    colunas_json: list[str] = field(default_factory=list)
    coluna_timestamp: str | None = None
    coluna_id: str | None = None
    coluna_agrupamento: str | None = None
    kpis_identificados: list[str] = field(default_factory=list)
    transformacoes_sugeridas: list[str] = field(default_factory=list)
    modulos_gold_recomendados: list[str] = field(default_factory=list)
    resumo: str = ""


class CodeGenAgent:
    """Agente que analisa especificações e gera código de pipeline.

    Usa LLM para:
      1. Analisar o dicionário de dados e identificar padrões
      2. Planejar transformações para cada camada
      3. Gerar código Python para a camada Gold baseado nos KPIs
      4. Recomendar quais módulos Gold devem ser ativados
    """

    def __init__(self, llm: LLMProvider | None = None) -> None:
        self.llm = llm or LLMProvider()

    def analisar_spec(self, spec: ProjectSpec) -> AnaliseEspecificacao:
        """Analisa a especificação do projeto e retorna insights estruturados.

        Combina análise estática (schema da amostra) com análise LLM
        (dicionário e KPIs) para produzir um plano de implementação.

        Args:
            spec: Especificação do projeto com dados, dicionário e KPIs.

        Returns:
            AnaliseEspecificacao com colunas, PII, KPIs e sugestões.
        """
        logger.info("analisando_especificacao", nome=spec.nome)

        analise = AnaliseEspecificacao()

        # Análise estática do schema
        if spec.analise:
            analise.colunas_identificadas = [
                c.nome for c in spec.analise.colunas
            ]

        # Análise via LLM do dicionário + KPIs
        modulos_disponiveis = "\n".join(
            f"  - {k}: {v['descricao']}" for k, v in GOLD_MODULES.items()
        )

        prompt_sistema = (
            "Você é um engenheiro de dados especialista em pipelines de transformação. "
            "Analise o dicionário de dados e a descrição dos KPIs fornecidos. "
            "Responda em JSON com os campos: "
            "colunas_com_pii (lista de nomes de colunas que podem conter dados sensíveis), "
            "colunas_json (lista de colunas que contêm JSON), "
            "coluna_timestamp (nome da coluna principal de timestamp), "
            "coluna_id (nome da coluna de ID principal), "
            "coluna_agrupamento (coluna para agrupar registros, ex: conversation_id), "
            "kpis_identificados (lista de KPIs extraídos da descrição), "
            "transformacoes_sugeridas (lista de transformações necessárias), "
            "modulos_gold_recomendados (lista dos módulos Gold a ativar, escolhendo entre os disponíveis). "
            "Responda APENAS o JSON, sem explicações."
        )

        prompt_usuario = (
            f"## Dicionário de Dados\n\n{spec.dicionario_dados}\n\n"
            f"## Descrição dos KPIs\n\n{spec.descricao_kpis}\n\n"
            f"## Colunas Detectadas\n\n{', '.join(analise.colunas_identificadas)}\n\n"
            f"## Módulos Gold Disponíveis\n\n{modulos_disponiveis}"
        )

        try:
            resposta = self.llm.complete_json(
                messages=[
                    {"role": "system", "content": prompt_sistema},
                    {"role": "user", "content": prompt_usuario},
                ]
            )

            if isinstance(resposta, dict):
                analise.colunas_com_pii = resposta.get("colunas_com_pii", [])
                analise.colunas_json = resposta.get("colunas_json", [])
                analise.coluna_timestamp = resposta.get("coluna_timestamp")
                analise.coluna_id = resposta.get("coluna_id")
                analise.coluna_agrupamento = resposta.get("coluna_agrupamento")
                analise.kpis_identificados = resposta.get("kpis_identificados", [])
                analise.transformacoes_sugeridas = resposta.get(
                    "transformacoes_sugeridas", []
                )
                analise.modulos_gold_recomendados = resposta.get(
                    "modulos_gold_recomendados", list(GOLD_MODULES.keys())
                )

            logger.info(
                "especificacao_analisada",
                kpis=len(analise.kpis_identificados),
                transformacoes=len(analise.transformacoes_sugeridas),
                modulos_gold=analise.modulos_gold_recomendados,
            )

        except Exception as e:
            logger.warning("analise_llm_falhou", erro=str(e))
            analise.resumo = f"Análise LLM não disponível: {e}"
            # Fallback: recomendar todos os módulos
            analise.modulos_gold_recomendados = list(GOLD_MODULES.keys())

        return analise

    def gerar_plano_gold(self, spec: ProjectSpec) -> dict[str, Any]:
        """Gera um plano de implementação para a camada Gold.

        Baseado na descrição dos KPIs, determina quais módulos Gold
        devem ser criados e quais transformações devem ser aplicadas.

        Args:
            spec: Especificação do projeto.

        Returns:
            Dicionário com o plano: módulos, tabelas e transformações.
        """
        logger.info("gerando_plano_gold", nome=spec.nome)

        prompt_sistema = (
            "Você é um engenheiro de dados. Com base na descrição dos KPIs, "
            "gere um plano de implementação para a camada Gold do pipeline. "
            "Responda em JSON com: "
            "modulos (lista de dicts com 'nome', 'descricao', 'tabela_saida', "
            "'colunas_saida'), "
            "dependencias_silver (lista de colunas da Silver necessárias). "
            "O código será em Python com Polars e Delta Lake. "
            "Responda APENAS o JSON."
        )

        schema_silver = ""
        if spec.analise:
            schema_silver = "\n".join(
                f"  - {c.nome} ({c.tipo})" for c in spec.analise.colunas
            )

        prompt_usuario = (
            f"## KPIs Desejados\n\n{spec.descricao_kpis}\n\n"
            f"## Dicionário de Dados\n\n{spec.dicionario_dados}\n\n"
            f"## Schema Disponível na Silver\n\n{schema_silver}"
        )

        try:
            plano = self.llm.complete_json(
                messages=[
                    {"role": "system", "content": prompt_sistema},
                    {"role": "user", "content": prompt_usuario},
                ]
            )
            logger.info("plano_gold_gerado", modulos=len(plano.get("modulos", [])))
            return plano if isinstance(plano, dict) else {"modulos": [], "erro": "Resposta inválida"}

        except Exception as e:
            logger.warning("geracao_plano_gold_falhou", erro=str(e))
            return {"modulos": [], "erro": str(e)}

    def recomendar_modulos_gold(self, spec: ProjectSpec) -> list[str]:
        """Recomenda quais módulos Gold devem ser executados.

        Análise heurística (sem LLM) dos KPIs descritos para determinar
        quais módulos Gold são relevantes.

        Args:
            spec: Especificação do projeto.

        Returns:
            Lista de nomes dos módulos Gold recomendados.
        """
        kpis_lower = spec.descricao_kpis.lower()
        recomendados = []

        # Mapeamento de palavras-chave → módulos
        _keyword_map = {
            "sentiment": ["sentimento", "sentiment", "satisfação", "humor", "opinião"],
            "personas": ["persona", "perfil", "classificação", "tipo de lead", "comportamento"],
            "segmentation": ["segmentação", "segmento", "audiência", "região", "geografia", "engajamento"],
            "analytics": ["funil", "conversão", "kpi", "score", "lead scoring", "campanha", "performance"],
            "vendor_analysis": ["vendedor", "agente", "agent", "atendente", "time de vendas"],
        }

        for modulo, keywords in _keyword_map.items():
            if any(kw in kpis_lower for kw in keywords):
                recomendados.append(modulo)

        # Se nenhum KPI específico detectado, ativar todos
        if not recomendados:
            recomendados = list(GOLD_MODULES.keys())

        logger.info("modulos_gold_recomendados", modulos=recomendados)
        return recomendados
