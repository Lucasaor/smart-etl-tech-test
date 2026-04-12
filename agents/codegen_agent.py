"""Agente gerador de código — analisa especificações e GERA scripts do pipeline.

O CodeGen Agent recebe uma ProjectSpec (dados brutos + dicionário + KPIs)
e GERA DINAMICAMENTE os scripts Python para cada camada do pipeline:
  - Bronze: ingestão e validação de schema adaptada ao dado de entrada
  - Silver: limpeza, extração de entidades, agregação baseada no dicionário
  - Gold: analytics e KPIs conforme descrição fornecida pelo usuário

NENHUM código de pipeline é pré-codificado. Todo pipeline é gerado
sob demanda pelo LLM com base nos inputs do usuário.

O código gerado segue o padrão do projeto: documentado em pt-BR,
usa Polars para compute e Delta Lake para storage.
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from agents.llm_provider import LLMProvider
from config.llm_config import get_llm_config_for_role
from pipeline.specs import ProjectSpec

logger = structlog.get_logger(__name__)


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


@dataclass
class GeneratedCode:
    """Resultado da geração de código para uma camada."""

    camada: str
    codigo: str
    nome_funcao: str
    descricao: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    testes: str = ""
    erro: str = ""


@dataclass
class PipelineGerado:
    """Conjunto completo de código gerado para o pipeline."""

    bronze: GeneratedCode | None = None
    silver: GeneratedCode | None = None
    gold: list[GeneratedCode] = field(default_factory=list)
    analise: AnaliseEspecificacao | None = None
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def completo(self) -> bool:
        return self.bronze is not None and self.silver is not None and len(self.gold) > 0

    @property
    def resumo(self) -> dict[str, Any]:
        return {
            "bronze": bool(self.bronze),
            "silver": bool(self.silver),
            "gold_modules": len(self.gold),
            "gold_nomes": [g.nome_funcao for g in self.gold],
            "completo": self.completo,
            "timestamp": self.timestamp,
        }


# ─── Prompts base para geração de código ─────────────────────────────────────

_SYSTEM_PROMPT_BASE = textwrap.dedent("""\
    Você é um engenheiro de dados sênior especialista em gerar código Python
    para pipelines de dados. Você gera código de PRODUÇÃO usando:
    - Polars para manipulação de DataFrames
    - Delta Lake para persistência (via funções write_table/read_table passadas como parâmetro)
    - structlog para logging

    REGRAS OBRIGATÓRIAS para o código gerado:
    1. Use APENAS Polars (import polars as pl), NUNCA pandas
    2. Para leitura de tabelas Delta Lake: use a função read_table(path) passada como parâmetro
    3. Para escrita do Delta: use a função write_table(df, path, mode) passada como parâmetro
    4. A função principal DEVE retornar um dict com 'rows_written' e outras métricas
    5. O código deve ser autocontido — defina helpers dentro se necessário
    6. Docstrings e comentários em pt-BR
    7. Trate erros com try/except em operações de IO
    8. NÃO importe módulos do projeto — use apenas polars, re, json, hashlib, datetime
    9. A assinatura da função principal DEVE ser:
       def run(read_table, write_table, settings: dict) -> dict
       onde settings contém paths e parâmetros configuráveis
    10. Retorne APENAS o código Python, sem markdown, sem ```python, sem explicações
    11. Para ler arquivos FONTE brutos (Parquet/CSV que NÃO são Delta tables),
        use pl.read_parquet(path) ou pl.read_csv(path) diretamente.
        A função read_table() é APENAS para tabelas Delta já existentes.
        Na camada Bronze, SEMPRE use pl.read_parquet(settings["source_path"])
        para ler o arquivo fonte.
    12. Todos os paths em settings já são ABSOLUTOS — use-os diretamente.
    13. NUNCA importe deltalake (from deltalake, import deltalake, write_deltalake,
        DeltaTable). A persistência Delta é feita EXCLUSIVAMENTE via write_table()
        e read_table() passadas como parâmetro. Importar deltalake causa erro.
    14. ATENÇÃO à API do Polars — erros comuns a EVITAR:
        - NÃO existe df.sort_by(). Use df.sort("coluna") ou df.sort(["col1", "col2"]).
        - NÃO existe df.groupby(). Use df.group_by("coluna").
        - NÃO existe df.rename({{...}}). Use df.rename({{"old": "new"}}).
        - NÃO existe df.drop_duplicates(). Use df.unique().
        - NÃO existe df.fillna(). Use df.fill_null().
        - NÃO existe df.isna(). Use df.is_null().
        - NÃO existe df.astype(). Use df.cast().
        - NÃO existe pl.col("x").str.contains(pat, regex=True). Use pl.col("x").str.contains(pat).
        - NÃO use .apply() — use .map_elements() ou expressões nativas Polars.
    15. NUNCA engula exceções dentro de try/except retornando rows_written=0.
        Se uma operação falhar, faça raise para que o erro possa ser diagnosticado.
        O try/except deve ser APENAS para operações opcionais (ex: parsing de um campo).
""")

_BRONZE_PROMPT = textwrap.dedent("""\
    Gere o código Python da camada BRONZE do pipeline de dados.

    A camada Bronze é responsável por:
    - Ler o arquivo fonte (parquet ou csv) do caminho em settings["source_path"]
      IMPORTANTE: Use pl.read_parquet(settings["source_path"]) para ler o arquivo fonte.
      NÃO use read_table() para o arquivo fonte — read_table é apenas para Delta tables.
    - Validar que as colunas esperadas existem
    - Fazer type casting quando necessário (ex: strings de timestamp → datetime)
    - Parsear colunas JSON se existirem (extrair subcampos)
    - Adicionar colunas de rastreio: _ingested_at (datetime UTC), _source_file (nome do arquivo)
    - Gravar como Delta table em settings["bronze_path"] com mode="overwrite"
      usando write_table(df, settings["bronze_path"], "overwrite")

    ## Schema dos dados de entrada
    {schema_info}

    ## Dicionário de dados
    {dicionario}

    ## Amostra dos dados (primeiras linhas como JSON)
    {amostra}

    Gere uma função `run(read_table, write_table, settings: dict) -> dict`.
    settings conterá: source_path (absoluto), bronze_path (absoluto), formato (parquet/csv).
    Retorne: {{"rows_written": N, "columns": [...], "status": "ok"}}
""")

_SILVER_PROMPT = textwrap.dedent("""\
    Gere o código Python da camada SILVER do pipeline de dados.

    A camada Silver é responsável por:
    1. LIMPEZA: Ler a tabela Bronze de settings["bronze_path"], limpar e deduplicar
    2. EXTRAÇÃO: Extrair entidades dos dados (PII, categorias, etc) baseado no dicionário
    3. AGREGAÇÃO: Agregar dados por settings["coluna_agrupamento"] para criar tabela resumida

    As tabelas de saída:
    - Mensagens limpas → settings["silver_messages_path"]
    - Tabela agregada → settings["silver_conversations_path"]

    ## Schema da tabela Bronze
    {schema_bronze}

    ## Dicionário de dados
    {dicionario}

    ## Análise das colunas
    Colunas com PII: {colunas_pii}
    Colunas JSON: {colunas_json}
    Coluna de timestamp: {coluna_timestamp}
    Coluna de ID único: {coluna_id}
    Coluna de agrupamento: {coluna_agrupamento}
    Transformações sugeridas: {transformacoes}

    Para mascaramento de PII, use hash SHA-256 determinístico.
    Gere regex patterns baseados no dicionário de dados fornecido.

    ═══════════════════════════════════════════════════════════════════
    REGRAS CRÍTICAS PARA A CAMADA SILVER (OBRIGATÓRIAS)
    ═══════════════════════════════════════════════════════════════════

    1. TABELA AGGREGADA POR CONVERSA:
       A tabela silver_conversations DEVE ser agregada por conversa (uma linha
       por conversa). A camada Gold lê APENAS esta tabela e espera que cada
       linha seja uma conversa completa com todas as métricas pré-computadas.
       A Gold NÃO tem acesso a mensagens individuais.

    2. COLUNAS PRÉ-COMPUTADAS OBRIGATÓRIAS:
       A tabela silver_conversations DEVE incluir estas colunas para que
       a camada Gold funcione corretamente:
       - total_mensagens (Int32): total de mensagens na conversa
       - total_mensagens_inbound (Int32): mensagens do lead
       - total_mensagens_outbound (Int32): mensagens do agente
       - primeira_mensagem_at (Datetime): timestamp da primeira mensagem
       - ultima_mensagem_at (Datetime): timestamp da última mensagem
       - duracao_conversa_min (Int64): duração em minutos
       - response_time_sec_mediana (Float64): tempo mediano de resposta
       - response_time_sec_min (Int64): tempo mínimo de resposta
       - conversation_outcome (String): resultado da conversa
       - venda_fechada (Boolean): se houve venda
       - menciona_concorrente (Boolean): se menciona concorrente
       - concorrentes_mencionados (String): nomes dos concorrentes
       - lead_city, lead_state, lead_device, lead_source (String): dados do lead
       - pii_cpf_hash, pii_email_hash (String|null): PII mascarado
       - proporcao_horario_comercial (Float64): proporção em horário comercial
       - veiculo_marca, veiculo_ano, veiculo_placa_hash (String): dados do veículo

    3. NÃO DEIXE COLUNAS JSON SEM EXTRAIR:
       Se a Bronze tem colunas JSON (ex: "metadata"), extraia TODOS os subcampos
       como colunas individuais na Silver. A Gold NÃO deve precisar parsear JSON.

    4. NOMES CONSISTENTES:
       Use exatamente os nomes listados acima. A Gold referencia estas colunas
       pelo nome exato. Não use variações como "timestamp", "direction", "message_body".
    ═══════════════════════════════════════════════════════════════════

    Gere uma função `run(read_table, write_table, settings: dict) -> dict`.
    settings conterá: bronze_path, silver_messages_path, silver_conversations_path,
                      coluna_agrupamento, coluna_id, coluna_timestamp.
    Retorne: {{"rows_written": N, "conversations_written": N, "status": "ok"}}
""")

_GOLD_PROMPT = textwrap.dedent("""\
    Gere o código Python para o módulo Gold "{nome_modulo}" do pipeline de dados.

    A camada Gold gera analytics e KPIs a partir da tabela Silver agregada.

    ## KPI / Análise a implementar
    {descricao_kpi}

    ## Schema da tabela Silver de conversas (colunas reais disponíveis)
    {schema_silver}

    ## Dicionário de dados
    {dicionario}

    ## Todas as colunas disponíveis na Silver
    {colunas_silver}

    ═══════════════════════════════════════════════════════════════════
    REGRAS CRÍTICAS PARA GERAÇÃO DE CÓDIGO GOLD (OBRIGATÓRIAS)
    ═══════════════════════════════════════════════════════════════════

    1. COLUNAS PROIBIDAS — NÃO EXISTEM NA SILVER:
       A tabela Silver NÃO possui estas colunas. NUNCA as use:
       - "metadata" (não existe; subcampos já estão como colunas individuais)
       - "timestamp" (use "primeira_mensagem_at" ou "ultima_mensagem_at")
       - "message_body" / "full_body" / "body" (dados de mensagens individuais
         não existem; use colunas pré-computadas como pii_cpf_hash, pii_email_hash, etc.)
       - "direction" (não existe; use total_mensagens_inbound / total_mensagens_outbound)
       - "message_type" (não existe; use total_audio, total_image, total_document)
       - "message_id" (não existe; a Silver é por CONVERSA, não por mensagem)
       - "sender_phone" (use lead_phone)
       Use APENAS as colunas listadas acima em "Todas as colunas disponíveis na Silver".

    2. A SILVER É POR CONVERSA, NÃO POR MENSAGEM:
       Cada linha na Silver representa UMA conversa inteira.
       - NÃO use group_by("conversation_id") sobre a Silver — os dados já estão
         agrupados por conversa.
       - Em vez de contar mensagens inbound/outbound com filter + group_by, use
         diretamente: pl.col("total_mensagens_inbound"), pl.col("total_mensagens_outbound").
       - Em vez de analisar text de mensagens, use colunas booleanas como
         menciona_concorrente, venda_fechada, tem_historico_sinistro.

    3. TIPOS DE DADOS POLARS — ATENÇÃO AOS DTYPES:
       - Polars dt.weekday(), dt.week(), dt.hour() retornam Int8, NÃO Int32.
         Ao criar literais nulos para essas colunas, use pl.lit(None).cast(pl.Int8).
       - Ao concatenar DataFrames com pl.concat(), garanta que os dtypes correspondam
         exatamente. Use .cast() para alinhar tipos quando necessário.
       - round() do Python NÃO aceita None. Sempre proteja:
         round(valor, 2) if valor is not None else 0.0
       - .mean(), .median(), .sum() em Polars podem retornar None para colunas
         totalmente nulas. Proteja antes de usar round().

    4. VARIÁVEIS E IMPORTS OBRIGATÓRIOS:
       - SEMPRE defina gold_path = settings["gold_{{nome_modulo}}_path"] logo após
         ler a Silver. O código DEVE ter gold_path definido antes de chamar write_table.
       - Se usar json.dumps(), GARANTA que "import json" está no topo do arquivo.
       - Se usar hashlib, re, datetime: garanta os imports correspondentes.
       - TODAS as funções auxiliares (helpers) usadas dentro de map_elements()
         DEVEM estar definidas ANTES da função run(), ou como funções internas.
         NUNCA referencie uma função que não foi definida no arquivo.

    5. CÓDIGO COMPLETO — NÃO TRUNCAR:
       - O código DEVE estar completo. SEMPRE termine com:
         return {{"rows_written": N, "metricas": {{...}}, "status": "ok"}}
       - GARANTA que blocos try/except estão completos com except + raise/return.
       - NUNCA deixe funções auxiliares sem corpo ou com ... (ellipsis).

    6. MAPEAMENTO DE COLUNAS (referência rápida):
       - Timestamp → primeira_mensagem_at (Datetime), ultima_mensagem_at (Datetime)
       - Duração → duracao_conversa_min (Int64)
       - Direction → total_mensagens_inbound (Int32), total_mensagens_outbound (Int32)
       - PII → pii_cpf_hash (String|null), pii_email_hash (String|null), lead_phone (String)
       - Veículo → veiculo_marca (String), veiculo_ano (String), veiculo_placa_hash (String)
       - Concorrência → menciona_concorrente (Boolean), concorrentes_mencionados (String)
       - Resposta → response_time_sec_mediana (Float64), response_time_sec_min (Int64)
       - Resultado → conversation_outcome (String), venda_fechada (Boolean)
       - Localização → lead_city (String), lead_state (String)
       - Canal → channel (String), lead_source (String), lead_device (String)
       - Horário comercial → proporcao_horario_comercial (Float64)
    ═══════════════════════════════════════════════════════════════════

    O módulo deve:
    - Ler a tabela Silver de settings["silver_conversations_path"]
    - Definir gold_path = settings["gold_{nome_modulo}_path"] ANTES de processar
    - Calcular as métricas e KPIs descritos acima
    - Gerar uma tabela Gold com os resultados
    - Gravar em gold_path com mode="overwrite"

    Gere uma função `run(read_table, write_table, settings: dict) -> dict`.
    Retorne: {{"rows_written": N, "metricas": {{...}}, "status": "ok"}}
""")

_TEST_PROMPT = textwrap.dedent("""\
    Gere testes unitários pytest para o seguinte código de pipeline:

    ```python
    {codigo}
    ```

    ## Contexto
    Camada: {camada}
    Schema esperado: {schema}

    Regras para os testes:
    1. Use polars para criar DataFrames de teste (mock data)
    2. Crie funções mock para read_table e write_table
    3. Teste o happy path (dados normais)
    4. Teste edge cases (dados vazios, nulos, tipos inesperados)
    5. Verifique que a função retorna dict com 'rows_written'
    6. Use pytest assert, NÃO use unittest
    7. Retorne APENAS o código Python dos testes, sem markdown
""")


class CodeGenAgent:
    """Agente que analisa especificações e GERA código de pipeline dinamicamente.

    Todo código de pipeline (Bronze, Silver, Gold) é gerado em runtime
    pelo LLM com base nos inputs do usuário:
      1. Analisa o dicionário + KPIs → AnaliseEspecificacao
      2. Gera código Bronze (ingestão adaptada ao schema real)
      3. Gera código Silver (limpeza/extração baseada no dicionário)
      4. Gera código Gold (analytics/KPIs baseados na descrição)
      5. Gera testes para cada camada

    Cada camada é validada sintaticamente e, se falhar, regenerada
    automaticamente até `max_retries` vezes.
    """

    MAX_RETRIES = 3

    def __init__(self, llm: LLMProvider | None = None) -> None:
        self.llm = llm or LLMProvider(config=get_llm_config_for_role("codegen"))

    # ─── Análise da especificação ─────────────────────────────────────────

    def analisar_spec(self, spec: ProjectSpec) -> AnaliseEspecificacao:
        """Analisa a especificação do projeto e retorna insights estruturados.

        Combina análise estática (schema da amostra) com análise LLM
        (dicionário e KPIs) para produzir um plano de implementação.
        """
        logger.info("analisando_especificacao", nome=spec.nome)

        analise = AnaliseEspecificacao()

        if spec.analise:
            analise.colunas_identificadas = [c.nome for c in spec.analise.colunas]

        prompt_sistema = (
            "Você é um engenheiro de dados especialista em pipelines de transformação. "
            "Analise o dicionário de dados e a descrição dos KPIs fornecidos. "
            "Responda em JSON com os campos: "
            "colunas_com_pii (lista de nomes de colunas que podem conter dados sensíveis como CPF, email, telefone, endereço), "
            "colunas_json (lista de colunas que contêm dados em formato JSON), "
            "coluna_timestamp (nome da coluna principal de timestamp), "
            "coluna_id (nome da coluna de ID principal/único), "
            "coluna_agrupamento (coluna para agrupar registros relacionados), "
            "kpis_identificados (lista de KPIs extraídos da descrição, cada um com nome curto), "
            "transformacoes_sugeridas (lista de transformações necessárias para Bronze e Silver), "
            "modulos_gold_recomendados (lista de nomes descritivos para cada módulo Gold necessário). "
            "Responda APENAS o JSON, sem explicações."
        )

        prompt_usuario = (
            f"## Dicionário de Dados\n\n{spec.dicionario_dados}\n\n"
            f"## Descrição dos KPIs\n\n{spec.descricao_kpis}\n\n"
            f"## Colunas Detectadas no Arquivo\n\n{', '.join(analise.colunas_identificadas)}"
        )

        if spec.analise:
            schema_info = "\n".join(
                f"  - {c.nome} ({c.tipo}, {c.nulos_pct:.1f}% nulos, {c.valores_unicos} únicos, ex: {c.exemplo})"
                for c in spec.analise.colunas
            )
            prompt_usuario += f"\n\n## Estatísticas das Colunas\n\n{schema_info}"

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
                analise.transformacoes_sugeridas = resposta.get("transformacoes_sugeridas", [])
                analise.modulos_gold_recomendados = resposta.get("modulos_gold_recomendados", [])

            logger.info(
                "especificacao_analisada",
                kpis=len(analise.kpis_identificados),
                transformacoes=len(analise.transformacoes_sugeridas),
                modulos_gold=analise.modulos_gold_recomendados,
            )

        except Exception as e:
            logger.warning("analise_llm_falhou", erro=str(e))
            analise.resumo = f"Análise LLM não disponível: {e}"

        # ── Fallback: extrair módulos Gold do texto dos KPIs se LLM não retornou ──
        if not analise.modulos_gold_recomendados and spec.descricao_kpis:
            logger.warning(
                "modulos_gold_vazio_tentando_fallback",
                motivo="LLM não retornou modulos_gold_recomendados ou lista vazia",
            )
            analise.modulos_gold_recomendados = self._extrair_modulos_gold_fallback(
                spec.descricao_kpis
            )
            logger.info(
                "modulos_gold_fallback_extraidos",
                modulos=analise.modulos_gold_recomendados,
                quantidade=len(analise.modulos_gold_recomendados),
            )

        if not analise.modulos_gold_recomendados:
            logger.error(
                "modulos_gold_vazio_sem_fallback",
                motivo="Nenhum módulo Gold identificado mesmo após fallback. "
                       "Verifique se a Descrição dos KPIs foi fornecida.",
            )

        return analise

    # ─── Geração de código ────────────────────────────────────────────────

    def _validar_codigo_sintatico(self, codigo: str, label: str) -> tuple[bool, str]:
        """Valida o código gerado sintaticamente e verifica que define run().

        Returns:
            (valido, mensagem_erro) — True/"" se ok, False/erro se falhou.
        """
        if not codigo or not codigo.strip():
            return False, "Código vazio gerado"

        # 1. Compilação (sintaxe)
        try:
            compiled = compile(codigo, f"<validate_{label}>", "exec")
        except SyntaxError as e:
            return False, f"Erro de sintaxe: {e}"

        # 2. Executar definições e verificar que run() existe
        namespace: dict[str, Any] = {"__builtins__": __builtins__}
        try:
            exec(compiled, namespace)
        except Exception as e:
            return False, f"Erro ao executar definições: {type(e).__name__}: {e}"

        if "run" not in namespace or not callable(namespace["run"]):
            return False, "Código não define uma função 'run()' chamável"

        # 3. Verificar assinatura (deve aceitar 3 args)
        import inspect
        sig = inspect.signature(namespace["run"])
        params = list(sig.parameters.keys())
        if len(params) < 3:
            return False, (
                f"Função run() tem {len(params)} parâmetros ({params}), "
                "mas deve ter 3: read_table, write_table, settings"
            )

        return True, ""

    def _executar_e_validar(
        self,
        codigo: str,
        label: str,
        spec: ProjectSpec,
    ) -> tuple[bool, str]:
        """Executa o código gerado contra os dados reais para validar que funciona.

        Após a validação sintática, executa o código usando o executor real
        com os dados de amostra. Garante que o script produz resultados válidos
        antes de avançar para a próxima camada.

        Returns:
            (sucesso, mensagem_erro) — True/"" se ok, False/erro se falhou.
        """
        try:
            from pathlib import Path as _P
            from pipeline.executor import build_settings_dict, execute_generated_code

            source_path = spec.dados_brutos_path if spec.dados_brutos_path else ""
            settings_dict = build_settings_dict(spec_dados_path=source_path)

            # Para módulos Gold, adicionar path específico
            if label.startswith("gold_"):
                nome = label.replace("gold_", "")
                settings_dict[f"gold_{nome}_path"] = str(
                    _P(settings_dict["gold_base_path"]) / nome
                )

            result = execute_generated_code(codigo, settings_dict, label=label)

            rows = result.get("rows_written", 0)
            if rows == 0:
                # Include the actual status/error from the generated code so
                # the LLM can see what went wrong during regeneration.
                status = result.get("status", "")
                detail = f" Detalhes: {status}" if status and status != "ok" else ""
                return False, (
                    f"Execução produziu 0 linhas na camada {label} — "
                    f"nenhum dado processado.{detail}"
                )

            logger.info("execucao_validacao_ok", camada=label, rows=rows)
            return True, ""

        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    def _gerar_com_retry(
        self,
        spec: ProjectSpec,
        analise: AnaliseEspecificacao,
        gerar_fn,
        label: str,
        progress_callback=None,
    ) -> GeneratedCode:
        """Gera código para uma camada, valida e retenta até MAX_RETRIES vezes.

        Args:
            spec: Especificação do projeto.
            analise: Análise da spec.
            gerar_fn: Callable que gera o GeneratedCode (primeira tentativa).
            label: Nome da camada para logs.
            progress_callback: Callback opcional (mensagem: str) -> None.

        Returns:
            GeneratedCode validado ou com erro após todas as tentativas.
        """
        result = gerar_fn()

        for attempt in range(1, self.MAX_RETRIES + 1):
            # Se gerou com erro explícito, tentar regenerar
            if result.erro:
                logger.warning(
                    "camada_gerada_com_erro",
                    camada=label, tentativa=attempt, erro=result.erro,
                )
                if progress_callback:
                    progress_callback(f"⚠️ {label}: erro na tentativa {attempt}, regenerando...")
                result = self.regenerar_camada(
                    spec, analise, result.camada, result.erro, result.codigo,
                )
                continue

            # Validar sintaticamente
            valido, erro_validacao = self._validar_codigo_sintatico(result.codigo, label)
            if not valido:
                logger.warning(
                    "camada_falhou_validacao",
                    camada=label, tentativa=attempt, erro=erro_validacao,
                )
                if progress_callback:
                    progress_callback(f"⚠️ {label}: validação falhou (tentativa {attempt}), regenerando...")
                result = self.regenerar_camada(
                    spec, analise, result.camada, erro_validacao, result.codigo,
                )
                continue

            # Executar contra os dados reais para garantir que funciona
            exec_ok, exec_error = self._executar_e_validar(result.codigo, label, spec)
            if exec_ok:
                logger.info("camada_validada_e_executada", camada=label, tentativa=attempt)
                return result

            # Execução falhou — regenerar com contexto do erro de execução
            logger.warning(
                "camada_falhou_execucao",
                camada=label, tentativa=attempt, erro=exec_error,
            )
            if progress_callback:
                progress_callback(f"⚠️ {label}: execução falhou (tentativa {attempt}), regenerando...")
            result = self.regenerar_camada(
                spec, analise, result.camada, exec_error, result.codigo,
            )

        # Última chance: validar e executar o último resultado
        if not result.erro:
            valido, erro_validacao = self._validar_codigo_sintatico(result.codigo, label)
            if valido:
                exec_ok, exec_error = self._executar_e_validar(result.codigo, label, spec)
                if exec_ok:
                    return result
                result.erro = f"Falhou execução após {self.MAX_RETRIES} tentativas: {exec_error}"
            else:
                result.erro = f"Falhou validação após {self.MAX_RETRIES} tentativas: {erro_validacao}"

        logger.error("camada_falhou_todas_tentativas", camada=label, erro=result.erro)
        return result

    def gerar_pipeline_completo(
        self,
        spec: ProjectSpec,
        progress_callback=None,
    ) -> PipelineGerado:
        """Gera todo o código do pipeline (Bronze + Silver + Gold).

        Fluxo:
          1. Analisa spec → AnaliseEspecificacao
          2. Gera Bronze (com validação + retry automático)
          3. Gera Silver (com validação + retry automático)
          4. Para cada KPI/módulo Gold → gera Gold (com validação + retry)
          5. Gera testes para cada camada

        Args:
            progress_callback: Callback opcional (mensagem: str) -> None para
                reportar progresso ao frontend.

        Returns:
            PipelineGerado com todo o código e testes.
        """
        logger.info("gerando_pipeline_completo", nome=spec.nome)

        pipeline = PipelineGerado()

        # 1. Análise
        if progress_callback:
            progress_callback("📊 Analisando especificação...")
        analise = self.analisar_spec(spec)
        pipeline.analise = analise

        # 2. Bronze — com validação e retry
        if progress_callback:
            progress_callback("🔶 Gerando camada Bronze...")
        pipeline.bronze = self._gerar_com_retry(
            spec, analise,
            gerar_fn=lambda: self.gerar_bronze(spec, analise),
            label="bronze",
            progress_callback=progress_callback,
        )

        # Bronze DEVE executar com sucesso antes de prosseguir
        if pipeline.bronze.erro:
            logger.error("bronze_falhou_abortando_pipeline", erro=pipeline.bronze.erro)
            if progress_callback:
                progress_callback(f"❌ Bronze falhou após todas as tentativas: {pipeline.bronze.erro}")
            return pipeline

        # 3. Silver — com validação e retry
        if progress_callback:
            progress_callback("⬜ Gerando camada Silver...")
        pipeline.silver = self._gerar_com_retry(
            spec, analise,
            gerar_fn=lambda: self.gerar_silver(spec, analise),
            label="silver",
            progress_callback=progress_callback,
        )

        # Silver DEVE executar com sucesso antes de prosseguir para Gold
        if pipeline.silver.erro:
            logger.error("silver_falhou_abortando_pipeline", erro=pipeline.silver.erro)
            if progress_callback:
                progress_callback(f"❌ Silver falhou após todas as tentativas: {pipeline.silver.erro}")
            return pipeline

        # 4. Gold — um módulo por KPI/grupo recomendado, cada um com retry
        if not analise.modulos_gold_recomendados:
            logger.error(
                "gold_geracao_impossivel",
                motivo="Lista de modulos_gold_recomendados está vazia. "
                       "Nenhum módulo Gold será gerado. "
                       "Verifique se a Descrição dos KPIs foi fornecida e se a análise LLM retornou módulos.",
                kpis_existem=bool(spec.descricao_kpis),
                analise_resumo=analise.resumo or "sem resumo",
            )
        else:
            logger.info(
                "gold_geracao_iniciando",
                total_modulos=len(analise.modulos_gold_recomendados),
                modulos=analise.modulos_gold_recomendados,
            )

        for i, modulo_nome in enumerate(analise.modulos_gold_recomendados, 1):
            if progress_callback:
                progress_callback(
                    f"🥇 Gerando Gold módulo {i}/{len(analise.modulos_gold_recomendados)}: {modulo_nome}..."
                )
            kpi_desc = self._encontrar_descricao_kpi(modulo_nome, spec.descricao_kpis, analise)
            gold_code = self._gerar_com_retry(
                spec, analise,
                gerar_fn=lambda mn=modulo_nome, kd=kpi_desc: self.gerar_gold_modulo(
                    spec, analise, mn, kd
                ),
                label=f"gold_{modulo_nome}",
                progress_callback=progress_callback,
            )
            pipeline.gold.append(gold_code)

        logger.info(
            "pipeline_completo_gerado",
            bronze=bool(pipeline.bronze and not pipeline.bronze.erro),
            silver=bool(pipeline.silver and not pipeline.silver.erro),
            gold_modules=len(pipeline.gold),
            gold_nomes=[g.camada for g in pipeline.gold],
        )
        return pipeline

    def gerar_bronze(self, spec: ProjectSpec, analise: AnaliseEspecificacao) -> GeneratedCode:
        """Gera o código da camada Bronze."""
        logger.info("gerando_codigo_bronze")

        schema_info = self._formatar_schema(spec)
        amostra = self._obter_amostra_json(spec)

        prompt = _BRONZE_PROMPT.format(
            schema_info=schema_info,
            dicionario=spec.dicionario_dados,
            amostra=amostra,
        )

        try:
            codigo = self._gerar_codigo("bronze", prompt)
            testes = self._gerar_testes(codigo, "bronze", schema_info)
            return GeneratedCode(
                camada="bronze",
                codigo=codigo,
                nome_funcao="run",
                descricao="Ingestão e validação da camada Bronze",
                testes=testes,
            )
        except Exception as e:
            logger.error("erro_geracao_bronze", erro=str(e))
            return GeneratedCode(
                camada="bronze", codigo="", nome_funcao="run",
                descricao="Erro na geração", erro=str(e),
            )

    def gerar_silver(self, spec: ProjectSpec, analise: AnaliseEspecificacao) -> GeneratedCode:
        """Gera o código da camada Silver."""
        logger.info("gerando_codigo_silver")

        schema_bronze = self._formatar_schema(spec)

        prompt = _SILVER_PROMPT.format(
            schema_bronze=schema_bronze,
            dicionario=spec.dicionario_dados,
            colunas_pii=", ".join(analise.colunas_com_pii) or "nenhuma detectada",
            colunas_json=", ".join(analise.colunas_json) or "nenhuma",
            coluna_timestamp=analise.coluna_timestamp or "não identificada",
            coluna_id=analise.coluna_id or "não identificada",
            coluna_agrupamento=analise.coluna_agrupamento or "não identificada",
            transformacoes=", ".join(analise.transformacoes_sugeridas) or "nenhuma",
        )

        try:
            codigo = self._gerar_codigo("silver", prompt)
            testes = self._gerar_testes(codigo, "silver", schema_bronze)
            return GeneratedCode(
                camada="silver",
                codigo=codigo,
                nome_funcao="run",
                descricao="Limpeza, extração e agregação da camada Silver",
                testes=testes,
            )
        except Exception as e:
            logger.error("erro_geracao_silver", erro=str(e))
            return GeneratedCode(
                camada="silver", codigo="", nome_funcao="run",
                descricao="Erro na geração", erro=str(e),
            )

    def gerar_gold_modulo(
        self,
        spec: ProjectSpec,
        analise: AnaliseEspecificacao,
        nome_modulo: str,
        descricao_kpi: str,
    ) -> GeneratedCode:
        """Gera o código de um módulo Gold específico."""
        logger.info("gerando_codigo_gold", modulo=nome_modulo)

        # Tentar ler o schema real da tabela Silver de conversas
        schema_silver = self._obter_schema_silver_real()
        if not schema_silver:
            schema_silver = self._formatar_schema(spec)

        colunas_silver = self._obter_colunas_silver_real()
        if not colunas_silver:
            colunas_silver = ", ".join(analise.colunas_identificadas) if analise.colunas_identificadas else "ver schema"

        # Normalizar nome para usar como identificador Python
        nome_safe = nome_modulo.lower().replace(" ", "_").replace("-", "_")

        prompt = _GOLD_PROMPT.format(
            nome_modulo=nome_safe,
            descricao_kpi=descricao_kpi,
            schema_silver=schema_silver,
            dicionario=spec.dicionario_dados,
            colunas_silver=colunas_silver,
        )

        try:
            codigo = self._gerar_codigo(f"gold_{nome_safe}", prompt)
            testes = self._gerar_testes(codigo, f"gold_{nome_safe}", schema_silver)
            return GeneratedCode(
                camada=f"gold_{nome_safe}",
                codigo=codigo,
                nome_funcao="run",
                descricao=f"Gold: {nome_modulo} — {descricao_kpi[:100]}",
                testes=testes,
            )
        except Exception as e:
            logger.error("erro_geracao_gold", modulo=nome_modulo, erro=str(e))
            return GeneratedCode(
                camada=f"gold_{nome_safe}", codigo="", nome_funcao="run",
                descricao=f"Erro na geração de {nome_modulo}", erro=str(e),
            )

    def regenerar_camada(
        self,
        spec: ProjectSpec,
        analise: AnaliseEspecificacao,
        camada: str,
        erro_anterior: str,
        codigo_anterior: str,
    ) -> GeneratedCode:
        """Regenera o código de uma camada que falhou, incluindo contexto do erro.

        Usado pelo repair_agent para corrigir automaticamente código com bugs.
        """
        logger.info("regenerando_camada", camada=camada, erro=erro_anterior[:200])

        # Obter schema real da silver para contexto extra em camadas Gold
        extra_contexto = ""
        if camada.startswith("gold"):
            schema_real = self._obter_schema_silver_real()
            if schema_real:
                extra_contexto = (
                    f"\n\nCONTEXTO IMPORTANTE — Colunas REAIS da tabela Silver:\n{schema_real}\n\n"
                    "REGRAS OBRIGATÓRIAS PARA CORREÇÃO:\n"
                    "1. A tabela Silver é POR CONVERSA (uma linha por conversa). NÃO use group_by('conversation_id').\n"
                    "2. Colunas PROIBIDAS (não existem): metadata, timestamp, message_body, full_body, "
                    "direction, message_type, message_id, sender_phone.\n"
                    "3. Mapeamento correto: timestamp→primeira_mensagem_at/ultima_mensagem_at, "
                    "direction→total_mensagens_inbound/total_mensagens_outbound, "
                    "PII→pii_cpf_hash/pii_email_hash/lead_phone, "
                    "state→lead_state, city→lead_city, device→lead_device.\n"
                    "4. Polars dt.weekday()/week()/hour() retornam Int8, NÃO Int32. "
                    "Use pl.lit(None).cast(pl.Int8) para literais nulos dessas colunas.\n"
                    "5. round() do Python NÃO aceita None. Proteja: round(val, 2) if val is not None else 0.0\n"
                    "6. SEMPRE defina gold_path = settings['gold_NOME_path'] ANTES de chamar write_table.\n"
                    "7. Se usar json.dumps(), GARANTA que 'import json' está no topo.\n"
                    "8. TODAS as funções auxiliares usadas (map_elements, etc.) DEVEM estar definidas no arquivo.\n"
                    "9. O código DEVE estar COMPLETO — termine com return {'rows_written': N, 'status': 'ok'}\n"
                )

        # Se o erro envolve colunas faltando, incluir definição do KPI para que
        # o LLM revise o processo de cálculo e encontre colunas alternativas
        erro_lower = erro_anterior.lower()
        if any(kw in erro_lower for kw in [
            "columnnotfound", "column", "not found", "keyerror",
            "0 linhas", "0 rows", "missing", "faltando",
        ]):
            extra_contexto += (
                f"\n\nDEFINIÇÃO COMPLETA DOS KPIs (revise a lógica de cálculo):\n"
                f"{spec.descricao_kpis}\n\n"
                "INSTRUÇÃO: Se alguma coluna necessária para o KPI não existe na tabela Silver,\n"
                "NÃO assuma que ela existe. Em vez disso:\n"
                "  1. Revise a definição do KPI acima\n"
                "  2. Identifique quais colunas DISPONÍVEIS na Silver podem ser usadas\n"
                "  3. Adapte o cálculo usando APENAS colunas que realmente existem\n"
                "  4. Se uma métrica depende de dados indisponíveis, use um valor default\n"
                "     ou calcule uma aproximação com as colunas disponíveis\n"
            )

        prompt_correcao = textwrap.dedent(f"""\
            O código gerado anteriormente para a camada {camada} falhou com o erro:

            {erro_anterior}
            {extra_contexto}
            Código que falhou:
            ```python
            {codigo_anterior}
            ```

            Corrija o código para resolver este erro. Mantenha a mesma assinatura:
            def run(read_table, write_table, settings: dict) -> dict

            Retorne APENAS o código Python corrigido, sem markdown, sem explicações.
        """)

        try:
            codigo = self._gerar_codigo(f"{camada}_fix", prompt_correcao)
            return GeneratedCode(
                camada=camada,
                codigo=codigo,
                nome_funcao="run",
                descricao=f"Código regenerado para {camada} após erro",
            )
        except Exception as e:
            logger.error("erro_regeneracao", camada=camada, erro=str(e))
            return GeneratedCode(
                camada=camada, codigo="", nome_funcao="run",
                descricao=f"Erro na regeneração", erro=str(e),
            )

    def regenerar_camada_do_zero(
        self,
        spec: ProjectSpec,
        analise: AnaliseEspecificacao | None,
        camada: str,
        insights_acumulados: list[str],
    ) -> GeneratedCode:
        """Regenera o código de uma camada COMPLETAMENTE do zero.

        Diferente de regenerar_camada (que tenta corrigir código existente),
        este método gera código novo desde o início, incorporando os insights
        das falhas anteriores diretamente no prompt de geração.

        Usado quando o repair agent esgota suas tentativas sem sucesso.
        """
        logger.info(
            "regenerando_camada_do_zero",
            camada=camada,
            total_insights=len(insights_acumulados),
        )

        if analise is None:
            analise = self.analisar_spec(spec)

        insights_text = "\n".join(f"  - {ins}" for ins in insights_acumulados)
        aviso_insights = textwrap.dedent(f"""\

            ═══════════════════════════════════════════════════════════════════
            REGENERAÇÃO COMPLETA — INSIGHTS DE FALHAS ANTERIORES
            ═══════════════════════════════════════════════════════════════════

            Tentativas anteriores de gerar este código falharam repetidamente.
            Você DEVE evitar os seguintes erros encontrados:
            {insights_text}

            INSTRUÇÕES PARA REGENERAÇÃO:
            1. Revise CUIDADOSAMENTE a definição do KPI e as colunas disponíveis
            2. Use APENAS colunas que REALMENTE existem na tabela de entrada
            3. Se uma coluna necessária não existe, adapte o cálculo usando
               colunas alternativas disponíveis ou valores default
            4. Valide que todas as funções auxiliares estão definidas
            5. Garanta que todos os imports necessários estão presentes
            6. Teste mentalmente o código com dados vazios/nulos

            Definição completa dos KPIs:
            {spec.descricao_kpis}
            ═══════════════════════════════════════════════════════════════════
        """)

        try:
            if camada == "bronze":
                schema_info = self._formatar_schema(spec)
                amostra = self._obter_amostra_json(spec)
                prompt = _BRONZE_PROMPT.format(
                    schema_info=schema_info,
                    dicionario=spec.dicionario_dados,
                    amostra=amostra,
                ) + aviso_insights
                codigo = self._gerar_codigo("bronze_fresh", prompt)

            elif camada == "silver":
                schema_bronze = self._formatar_schema(spec)
                prompt = _SILVER_PROMPT.format(
                    schema_bronze=schema_bronze,
                    dicionario=spec.dicionario_dados,
                    colunas_pii=", ".join(analise.colunas_com_pii) or "nenhuma detectada",
                    colunas_json=", ".join(analise.colunas_json) or "nenhuma",
                    coluna_timestamp=analise.coluna_timestamp or "não identificada",
                    coluna_id=analise.coluna_id or "não identificada",
                    coluna_agrupamento=analise.coluna_agrupamento or "não identificada",
                    transformacoes=", ".join(analise.transformacoes_sugeridas) or "nenhuma",
                ) + aviso_insights
                codigo = self._gerar_codigo("silver_fresh", prompt)

            elif camada.startswith("gold_"):
                nome = camada.replace("gold_", "")
                schema_silver = self._obter_schema_silver_real() or self._formatar_schema(spec)
                colunas_silver = self._obter_colunas_silver_real() or (
                    ", ".join(analise.colunas_identificadas) if analise.colunas_identificadas else "ver schema"
                )
                kpi_desc = self._encontrar_descricao_kpi(nome, spec.descricao_kpis, analise)

                prompt = _GOLD_PROMPT.format(
                    nome_modulo=nome,
                    descricao_kpi=kpi_desc,
                    schema_silver=schema_silver,
                    dicionario=spec.dicionario_dados,
                    colunas_silver=colunas_silver,
                ) + aviso_insights
                codigo = self._gerar_codigo(f"gold_{nome}_fresh", prompt)

            else:
                return GeneratedCode(
                    camada=camada, codigo="", nome_funcao="run",
                    descricao="Camada desconhecida", erro=f"Camada '{camada}' não reconhecida",
                )

            # Validar sintaticamente o código novo
            valido, erro_validacao = self._validar_codigo_sintatico(codigo, f"{camada}_fresh")
            if not valido:
                logger.warning("regeneracao_do_zero_falhou_validacao", camada=camada, erro=erro_validacao)
                return GeneratedCode(
                    camada=camada, codigo=codigo, nome_funcao="run",
                    descricao=f"Código regenerado do zero para {camada}",
                    erro=f"Validação falhou após regeneração do zero: {erro_validacao}",
                )

            logger.info("regeneracao_do_zero_ok", camada=camada, linhas=codigo.count("\n") + 1)
            return GeneratedCode(
                camada=camada,
                codigo=codigo,
                nome_funcao="run",
                descricao=f"Código regenerado do zero para {camada} com insights de falhas anteriores",
            )

        except Exception as e:
            logger.error("erro_regeneracao_do_zero", camada=camada, erro=str(e))
            return GeneratedCode(
                camada=camada, codigo="", nome_funcao="run",
                descricao=f"Erro na regeneração do zero", erro=str(e),
            )

    # ─── Geração de testes ────────────────────────────────────────────────

    def gerar_testes_camada(self, codigo: str, camada: str, schema: str) -> str:
        """Gera testes unitários para código de uma camada."""
        return self._gerar_testes(codigo, camada, schema)

    # ─── Helpers internos ─────────────────────────────────────────────────

    def _gerar_codigo(self, label: str, prompt_usuario: str) -> str:
        """Envia prompt ao LLM e extrai código Python limpo."""
        logger.info("llm_gerando_codigo", label=label)

        response = self.llm.complete(
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT_BASE},
                {"role": "user", "content": prompt_usuario},
            ],
            json_mode=False,
        )

        codigo = response.content.strip()

        # Limpar possíveis delimitadores markdown
        if codigo.startswith("```python"):
            codigo = codigo[len("```python"):].strip()
        if codigo.startswith("```"):
            codigo = codigo[3:].strip()
        if codigo.endswith("```"):
            codigo = codigo[:-3].strip()

        logger.info("codigo_gerado", label=label, linhas=codigo.count("\n") + 1)
        return codigo

    def _gerar_testes(self, codigo: str, camada: str, schema: str) -> str:
        """Gera testes unitários para o código de uma camada."""
        if not codigo:
            return ""

        prompt = _TEST_PROMPT.format(
            codigo=codigo,
            camada=camada,
            schema=schema,
        )

        try:
            response = self.llm.complete(
                messages=[
                    {"role": "system", "content": (
                        "Você é um engenheiro de testes. Gere testes pytest para código Polars. "
                        "Retorne APENAS código Python, sem markdown."
                    )},
                    {"role": "user", "content": prompt},
                ],
                json_mode=False,
            )
            testes = response.content.strip()
            if testes.startswith("```python"):
                testes = testes[len("```python"):].strip()
            if testes.startswith("```"):
                testes = testes[3:].strip()
            if testes.endswith("```"):
                testes = testes[:-3].strip()
            return testes
        except Exception as e:
            logger.warning("erro_geracao_testes", camada=camada, erro=str(e))
            return ""

    def _formatar_schema(self, spec: ProjectSpec) -> str:
        """Formata informações do schema para uso em prompts."""
        if not spec.analise:
            return "Schema não disponível — a amostra não foi analisada."

        linhas = []
        for c in spec.analise.colunas:
            linhas.append(
                f"  - {c.nome}: tipo={c.tipo}, "
                f"nulos={c.nulos_pct:.1f}%, "
                f"únicos={c.valores_unicos}, "
                f"exemplo={c.exemplo!r}"
            )
        return "\n".join(linhas)

    def _obter_schema_silver_real(self) -> str | None:
        """Tenta ler o schema real da tabela Silver conversations.

        Retorna string formatada com colunas e tipos, ou None se a tabela não existir.
        """
        try:
            from config.settings import get_settings
            from core.storage import get_storage_backend

            settings = get_settings()
            backend = get_storage_backend()
            if not backend.table_exists(settings.silver_conversations_path):
                return None

            import polars as pl
            df = backend.read_table(settings.silver_conversations_path)
            linhas = []
            for col in sorted(df.columns):
                linhas.append(f"  - {col}: tipo={df[col].dtype}")
            logger.info("schema_silver_real_lido", colunas=len(df.columns))
            return "\n".join(linhas)
        except Exception as e:
            logger.warning("schema_silver_real_indisponivel", erro=str(e))
            return None

    def _obter_colunas_silver_real(self) -> str | None:
        """Tenta ler as colunas reais da tabela Silver conversations."""
        try:
            from config.settings import get_settings
            from core.storage import get_storage_backend

            settings = get_settings()
            backend = get_storage_backend()
            if not backend.table_exists(settings.silver_conversations_path):
                return None

            df = backend.read_table(settings.silver_conversations_path)
            return ", ".join(sorted(df.columns))
        except Exception:
            return None

    def _obter_amostra_json(self, spec: ProjectSpec, max_rows: int = 3) -> str:
        """Lê as primeiras linhas do arquivo e retorna como JSON."""
        try:
            import polars as pl

            path = spec.dados_brutos_path
            if path.endswith(".csv"):
                df = pl.read_csv(path, n_rows=max_rows)
            else:
                df = pl.read_parquet(path).head(max_rows)

            return df.write_json()
        except Exception as e:
            return f"(amostra não disponível: {e})"

    @staticmethod
    def _extrair_modulos_gold_fallback(descricao_kpis: str) -> list[str]:
        """Extrai nomes de módulos Gold a partir dos cabeçalhos do KPI markdown.

        Fallback quando o LLM não retorna modulos_gold_recomendados.
        Procura por headers ## N. Nome no texto dos KPIs.
        """
        import re

        modulos: list[str] = []
        # Match headers like "## 1. Funil de Conversão" or "## Lead Scoring"
        for match in re.finditer(r"^##\s+(?:\d+\.\s*)?(.+)$", descricao_kpis, re.MULTILINE):
            nome = match.group(1).strip()
            # Skip non-KPI sections like "Notas de Implementação"
            if any(skip in nome.lower() for skip in ["nota", "implementação", "implementacao"]):
                continue
            # Normalize to snake_case identifier
            nome_safe = (
                nome.lower()
                .replace("á", "a").replace("é", "e").replace("í", "i")
                .replace("ó", "o").replace("ú", "u").replace("ã", "a")
                .replace("õ", "o").replace("ç", "c").replace("ê", "e")
                .replace(" ", "_").replace("-", "_")
            )
            # Remove non-alphanumeric (keep underscores)
            nome_safe = re.sub(r"[^a-z0-9_]", "", nome_safe)
            nome_safe = re.sub(r"_+", "_", nome_safe).strip("_")
            if nome_safe:
                modulos.append(nome_safe)

        return modulos

    def _encontrar_descricao_kpi(
        self, nome_modulo: str, descricao_kpis: str, analise: AnaliseEspecificacao
    ) -> str:
        """Encontra a descrição do KPI relevante para um módulo Gold."""
        # O LLM vai receber a descrição completa dos KPIs + o nome do módulo
        # e vai gerar código focado nesse módulo
        kpi_match = [k for k in analise.kpis_identificados if nome_modulo.lower() in k.lower()]
        if kpi_match:
            return f"KPIs específicos: {', '.join(kpi_match)}\n\nDescrição completa:\n{descricao_kpis}"
        return descricao_kpis


# ─── Persistência do código gerado ───────────────────────────────────────────


def salvar_pipeline_gerado(pipeline: PipelineGerado, output_dir: str) -> dict[str, str]:
    """Salva todo o código gerado em arquivos no diretório especificado.

    Estrutura:
      output_dir/
        bronze_generated.py
        silver_generated.py
        gold_<nome>_generated.py  (um por módulo)
        tests/
          test_bronze.py
          test_silver.py
          test_gold_<nome>.py

    Returns:
        Dict mapeando camada → path do arquivo salvo.
    """
    dir_path = Path(output_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    tests_dir = dir_path / "tests"
    tests_dir.mkdir(exist_ok=True)

    paths: dict[str, str] = {}

    # Bronze
    if pipeline.bronze and pipeline.bronze.codigo:
        bp = dir_path / "bronze_generated.py"
        bp.write_text(pipeline.bronze.codigo, encoding="utf-8")
        paths["bronze"] = str(bp)
        if pipeline.bronze.testes:
            tp = tests_dir / "test_bronze.py"
            tp.write_text(pipeline.bronze.testes, encoding="utf-8")
            paths["test_bronze"] = str(tp)

    # Silver
    if pipeline.silver and pipeline.silver.codigo:
        sp = dir_path / "silver_generated.py"
        sp.write_text(pipeline.silver.codigo, encoding="utf-8")
        paths["silver"] = str(sp)
        if pipeline.silver.testes:
            tp = tests_dir / "test_silver.py"
            tp.write_text(pipeline.silver.testes, encoding="utf-8")
            paths["test_silver"] = str(tp)

    # Gold modules
    for gold in pipeline.gold:
        if gold.codigo:
            nome = gold.camada.replace("gold_", "")
            gp = dir_path / f"gold_{nome}_generated.py"
            gp.write_text(gold.codigo, encoding="utf-8")
            paths[gold.camada] = str(gp)
            if gold.testes:
                tp = tests_dir / f"test_gold_{nome}.py"
                tp.write_text(gold.testes, encoding="utf-8")
                paths[f"test_{gold.camada}"] = str(tp)

    # Metadata
    meta = {
        "timestamp": pipeline.timestamp,
        "analise": {
            "colunas_identificadas": pipeline.analise.colunas_identificadas if pipeline.analise else [],
            "kpis_identificados": pipeline.analise.kpis_identificados if pipeline.analise else [],
            "modulos_gold": pipeline.analise.modulos_gold_recomendados if pipeline.analise else [],
        },
        "arquivos": paths,
    }
    meta_path = dir_path / "pipeline_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["meta"] = str(meta_path)

    logger.info("pipeline_gerado_salvo", dir=output_dir, arquivos=len(paths))
    return paths


def carregar_pipeline_gerado(output_dir: str) -> PipelineGerado | None:
    """Carrega um PipelineGerado a partir dos arquivos salvos."""
    dir_path = Path(output_dir)
    meta_path = dir_path / "pipeline_meta.json"

    if not meta_path.exists():
        return None

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    pipeline = PipelineGerado(timestamp=meta.get("timestamp", ""))

    # Bronze
    bp = dir_path / "bronze_generated.py"
    if bp.exists():
        pipeline.bronze = GeneratedCode(
            camada="bronze",
            codigo=bp.read_text(encoding="utf-8"),
            nome_funcao="run",
            descricao="Bronze gerado",
        )
        tp = dir_path / "tests" / "test_bronze.py"
        if tp.exists():
            pipeline.bronze.testes = tp.read_text(encoding="utf-8")

    # Silver
    sp = dir_path / "silver_generated.py"
    if sp.exists():
        pipeline.silver = GeneratedCode(
            camada="silver",
            codigo=sp.read_text(encoding="utf-8"),
            nome_funcao="run",
            descricao="Silver gerado",
        )
        tp = dir_path / "tests" / "test_silver.py"
        if tp.exists():
            pipeline.silver.testes = tp.read_text(encoding="utf-8")

    # Gold modules
    for gp in sorted(dir_path.glob("gold_*_generated.py")):
        nome = gp.stem.replace("_generated", "")
        gold = GeneratedCode(
            camada=nome,
            codigo=gp.read_text(encoding="utf-8"),
            nome_funcao="run",
            descricao=f"Gold: {nome}",
        )
        tp = dir_path / "tests" / f"test_{nome}.py"
        if tp.exists():
            gold.testes = tp.read_text(encoding="utf-8")
        pipeline.gold.append(gold)

    # Análise
    analise_data = meta.get("analise", {})
    if analise_data:
        pipeline.analise = AnaliseEspecificacao(
            colunas_identificadas=analise_data.get("colunas_identificadas", []),
            kpis_identificados=analise_data.get("kpis_identificados", []),
            modulos_gold_recomendados=analise_data.get("modulos_gold", []),
        )

    return pipeline


def pipeline_gerado_existe(output_dir: str) -> bool:
    """Verifica se existe código gerado no diretório."""
    return (Path(output_dir) / "pipeline_meta.json").exists()
