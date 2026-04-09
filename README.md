# Agentic Pipeline

Pipeline de Transformação Agêntica de Dados — Arquitetura Medalhão (Bronze → Silver → Gold) **gerado e gerenciado por agentes de IA autônomos**.

## Visão Geral

O sistema **recebe como entrada** uma amostra de dados brutos, um dicionário de dados e uma descrição dos KPIs desejados. A partir dessas entradas, agentes de IA autônomos **implementam os scripts do pipeline**, gerenciam a execução e auto-corrigem falhas — tudo como infraestrutura persistente.

### Fluxo Principal

```
┌─────────────────────────────────────────────────────────────────────┐
│              Entrada (via Streamlit ou config)                       │
│  1. Amostra de dados brutos (parquet/csv)                           │
│  2. Dicionário de dados (markdown)                                  │
│  3. Descrição dos KPIs desejados (markdown)                         │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Agente Gerador de Código (CodeGen)                  │
│  Analisa dados + dicionário + KPIs → Gera scripts Bronze/Silver/Gold│
│                       LiteLLM (LLM Router)                          │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Pipeline Layer (Python gerado)                     │
│       Bronze (Ingestão) → Silver (Limpeza) → Gold (Analytics)       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│              Storage Layer (Delta Lake)                               │
│         Local: deltalake+polars  │  Databricks: Delta nativo         │
└─────────────────────────────────────────────────────────────────────┘
```

### Arquitetura

```
┌───────────────────────────────────────────────────────────────┐
│                     Frontend (Streamlit)                       │
│  Configuração  │  Pipeline Monitor  │  Agent Monitor  │  Gold │
└───────────────────────────────────────────────────────────────┘
                              │
┌───────────────────────────────────────────────────────────────┐
│                   Agent Layer (LangGraph)                      │
│  CodeGen Agent  │  Pipeline Agent  │  Monitor  │  Repair      │
│                     LiteLLM (LLM Router)                      │
│              OpenAI │ Anthropic │ Google │ Ollama │ ...        │
└───────────────────────────────────────────────────────────────┘
                              │
┌───────────────────────────────────────────────────────────────┐
│                   Pipeline Layer (Python)                      │
│       Bronze (Ingestão) → Silver (Limpeza) → Gold (Analytics) │
└───────────────────────────────────────────────────────────────┘
                              │
┌───────────────────────────────────────────────────────────────┐
│              Storage Layer (Delta Lake)                        │
│         Local: deltalake+polars  │  Databricks: Delta nativo  │
└───────────────────────────────────────────────────────────────┘
```

### Stack Tecnológico

| Componente | Tecnologia | Justificativa |
|---|---|---|
| LLM Abstraction | **LiteLLM** | API unificada para 100+ providers (OpenAI, Anthropic, Google, Ollama, etc.) — troca via `.env` |
| Agent Framework | **LangGraph** | State machine robusto com suporte a auto-correção e fallback |
| Storage | **Delta Lake** | Formato portável local↔Databricks; ACID, time travel, schema enforcement |
| Compute (local) | **Polars + DuckDB** | Rápido para ~150k rows; adapter pattern para PySpark no Databricks |
| Frontend | **Streamlit** | Python nativo, interface de entrada de dados, funciona local e como Databricks App |
| Monitoring | **SQLite** (local) / **Delta tables** (Databricks) | Logs de pipeline runs e ações dos agentes |

## Estrutura do Projeto

```
.
├── pyproject.toml                 # Definição do projeto e dependências
├── .env.example                   # Template de variáveis de ambiente
│
├── config/                        # Configuração
│   ├── settings.py                # Pydantic Settings (runtime, paths, LLM, specs)
│   └── llm_config.py              # Cadeia de fallback de modelos + cost tracking
│
├── core/                          # Infraestrutura
│   ├── storage.py                 # StorageBackend ABC → LocalDelta / Databricks
│   ├── compute.py                 # ComputeBackend ABC → Polars / Spark
│   └── events.py                  # EventBus pub/sub para pipeline events
│
├── pipeline/                      # Transformações de dados
│   ├── specs.py                   # ProjectSpec: dados brutos + dicionário + KPIs
│   ├── orchestrator.py            # Orquestrador spec-aware
│   ├── bronze/                    # Ingestão de dados brutos → Delta
│   │   ├── ingestion.py           # Validação, parse metadata, incremental
│   │   └── simulator.py           # Simula chegada incremental de dados
│   ├── silver/                    # Limpeza, extração e agregação
│   │   ├── cleaning.py            # Dedup status, normalização, flags
│   │   ├── extraction.py          # Regex: PII, veículos, concorrentes
│   │   └── conversations.py       # Agregação por conversa
│   └── gold/                      # Analytics, personas, sentimento, segmentação
│       ├── sentiment.py           # Sentimento por conversa (heurístico)
│       ├── personas.py            # Classificação: Decidido/Pesquisador/Negociador/Fantasma/Indeciso
│       ├── segmentation.py        # Segmentação multidimensional
│       ├── analytics.py           # Funil, lead scoring, performance campanha
│       └── vendor_analysis.py     # Métricas por vendedor
│
├── agents/                        # Agentes de IA
│   ├── llm_provider.py            # LiteLLM wrapper (retry, fallback, budget)
│   ├── codegen_agent.py           # Agente gerador: spec → pipeline code
│   └── tools/
│       └── spec_tools.py          # Ferramentas de análise de especificações
│
├── monitoring/                    # Monitoramento
│   ├── models.py                  # Modelos: PipelineRun, StepRun, AgentAction, Alert
│   └── store.py                   # Persistência SQLite com queries
│
├── frontend/                      # Dashboard Streamlit
│   ├── app.py                     # Entry point principal
│   └── pages/
│       └── 0_configuracao.py      # Upload: dados brutos, dicionário, KPIs
│
├── tests/                         # Testes (128 testes passando)
│   ├── test_phase1.py             # Fundação + specs (34 testes)
│   ├── test_phase2.py             # Bronze + orchestrator (16 testes)
│   ├── test_phase3.py             # Silver + spec tools (33 testes)
│   └── test_phase4.py             # Gold + codegen agent (45 testes)
│
├── data/
│   ├── specs/                     # Especificações do projeto (input do usuário)
│   ├── bronze/                    # Tabela Delta Bronze
│   ├── silver/                    # Tabelas Delta Silver
│   ├── gold/                      # Tabelas Delta Gold
│   └── monitoring/                # SQLite de monitoramento
│
├── docker/                        # (Fase 7) Docker Compose
└── databricks/                    # (Fase 8) Notebooks Databricks
```

## Entradas do Sistema

O pipeline é orientado por **três entradas obrigatórias**, enviadas via Streamlit ou configuração:

| Entrada | Formato | Propósito |
|---------|---------|-----------|
| **Amostra de dados brutos** | Parquet ou CSV | Dados que serão processados na camada Bronze |
| **Dicionário de dados** | Markdown | Descreve colunas, tipos, valores esperados e particularidades |
| **Descrição dos KPIs** | Markdown | Define os indicadores e análises desejados na camada Gold |

Os agentes analisam essas entradas e implementam automaticamente os scripts do pipeline, documentados em português brasileiro.

## Instalação

### Pré-requisitos

- Python 3.11+
- (Opcional) Ollama para modelos locais
- (Opcional) Docker para execução containerizada

### Setup

```bash
# Clonar o repositório
git clone <repo-url>
cd agentic-pipeline

# Criar ambiente virtual
python3 -m venv .venv
source .venv/bin/activate

# Instalar dependências
pip install -e ".[dev]"

# Configurar ambiente
cp .env.example .env
# Editar .env com suas API keys e preferências
```

### Rodar testes

```bash
pytest tests/ -v
```

### Interface Streamlit

```bash
streamlit run frontend/app.py
```

Acesse `http://localhost:8501` e navegue para a página **Configuração** para enviar seus dados, dicionário e KPIs.

## Configuração LLM

O sistema é **agnóstico a plataforma de LLM**. Configure via `.env`:

```bash
# OpenAI
LLM_MODEL=gpt-4o-mini

# Anthropic
LLM_MODEL=anthropic/claude-3-haiku-20240307

# Google
LLM_MODEL=gemini/gemini-1.5-flash

# Ollama (local, sem API key)
LLM_MODEL=ollama/llama3
OLLAMA_BASE_URL=http://localhost:11434
```

A cadeia de fallback é automática: se o modelo primário falha, o sistema tenta o `LLM_FALLBACK_MODEL`. Custos são rastreados automaticamente com budget control (`LLM_MAX_COST_PER_RUN`).

## Status de Implementação

| Fase | Descrição | Status |
|------|-----------|--------|
| **1** | Fundação (config, storage, compute, events, LLM provider, **specs**) | ✅ Completa |
| **2** | Camada Bronze (ingestão, incremental, orchestrator **spec-aware**) | ✅ Completa |
| **3** | Camada Silver (limpeza, extração, agregação, **spec tools**) | ✅ Completa |
| **4** | Camada Gold (sentimento, personas, segmentação, analytics, vendedores) | ✅ Completa |
| **5** | Sistema de Agentes (CodeGen, pipeline, monitor, repair) | 🔲 Pendente |
| **6** | Frontend Streamlit (**configuração** + 3 dashboards) | 🟡 Parcial |
| **7** | Docker Compose | 🔲 Pendente |
| **8** | Migração Databricks | 🔲 Pendente |

## Fase 1 — Fundação

### O que foi implementado

**`config/settings.py`** — Configuração centralizada via Pydantic Settings:
- `RUNTIME_ENV` (local / databricks) — determina backends de storage e compute
- Paths derivados automáticos para Bronze/Silver/Gold/Monitoring/**Specs**
- Todas as variáveis de LLM, pipeline e frontend
- Singleton via `get_settings()` com cache

**`config/llm_config.py`** — Gestão de modelos LLM:
- Registry de modelos conhecidos com custos por token
- `ModelSpec` dataclass com metadata de cada modelo
- `LLMConfig` com cadeia de fallback e tracking de custo acumulado
- Controle de orçamento (`budget_exceeded`, `budget_remaining`)

**`core/storage.py`** — Abstração de storage Delta Lake:
- `StorageBackend` ABC: `read_table`, `write_table`, `table_exists`, `get_table_version`, `get_table_row_count`
- `LocalDeltaBackend`: deltalake + Polars (para execução local/Docker)
- `DatabricksBackend`: PySpark + Delta (para Databricks) — mesma interface
- Factory singleton via `get_storage_backend()`

**`core/compute.py`** — Abstração de compute:
- `ComputeBackend` ABC: `sql()` e `read_parquet()`
- `PolarsCompute`: Polars + DuckDB para SQL (execução local)
- `SparkCompute`: PySpark (Databricks)
- Factory singleton via `get_compute_backend()`

**`core/events.py`** — Event bus in-process:
- Pub/sub síncrono com subscriber por tipo de evento ou wildcard
- 10 tipos de evento (pipeline, step, data, agent, alert)
- Funções helper: `emit_pipeline_started`, `emit_step_completed`, `emit_step_failed`, `emit_agent_action`
- Erros em subscribers não propagam (sistema resiliente)

**`agents/llm_provider.py`** — Wrapper unificado sobre LiteLLM:
- Retry com exponential backoff (tenacity) em erros transientes
- Fallback automático pela cadeia de modelos
- Suporte a JSON mode (structured output)
- Tracking de tokens/custo por chamada e acumulado
- `BudgetExceededError` quando orçamento é ultrapassado
- Funciona com OpenAI, Anthropic, Google, Ollama, e 100+ providers

**`pipeline/specs.py`** — Especificação do projeto:
- `ProjectSpec`: modelo que armazena as 3 entradas obrigatórias (dados brutos, dicionário, KPIs)
- `analisar_amostra()`: análise automática do schema dos dados (linhas, colunas, tipos, nulos)
- `salvar_spec()` / `carregar_spec()`: persistência em disco (JSON + markdown)
- Validação: verifica se todas as entradas estão presentes e acessíveis

**`agents/codegen_agent.py`** — Agente gerador de código (scaffold):
- `analisar_spec()`: análise LLM do dicionário + KPIs → identificação de colunas PII, JSON, timestamps
- `gerar_plano_gold()`: gera plano de implementação para a camada Gold a partir dos KPIs

**`agents/tools/spec_tools.py`** — Ferramentas de análise de specs:
- `obter_preview_dados()`: preview das primeiras N linhas
- `obter_estatisticas_colunas()`: estatísticas descritivas por coluna
- `validar_spec_completa()`: validação detalhada para uso pelos agentes

**`monitoring/models.py`** — Modelos de dados:
- `PipelineRun`: registro de execução completa com steps
- `StepRun`: registro de um step individual (Bronze/Silver/Gold)
- `AgentAction`: registro de ação de agente (com tokens/custo)
- `Alert`: alerta com severidade, fonte, e estado de resolução

**`monitoring/store.py`** — Persistência SQLite:
- Schema auto-criado com 4 tabelas (pipeline_runs, step_runs, agent_actions, alerts)
- CRUD completo para todos os modelos
- Queries de agregação: custo total LLM, tokens totais
- Singleton via `get_monitoring_store()`

## Fase 2 — Camada Bronze

### O que foi implementado

**`pipeline/bronze/ingestion.py`** — Ingestão completa Bronze:
- Validação de schema contra dicionário de dados (14 colunas esperadas)
- Parse da coluna `metadata` (JSON string) → 6 colunas tipadas
- Cast de `timestamp` string → `Datetime`
- Adição de `_ingested_at` e `_source_file` para rastreabilidade
- 3 modos: `full` (overwrite), `incremental` (append novos), `auto` (detecta automaticamente)
- Path de dados pode vir da `ProjectSpec` (spec-aware)

**`pipeline/bronze/simulator.py`** — Simulador de dados incrementais

**`pipeline/orchestrator.py`** — Orquestrador de pipeline:
- `run_pipeline(layers)`: executa steps em sequência
- **Aceita `ProjectSpec`** para configurar paths e parâmetros dinamicamente
- Carrega spec automaticamente do diretório configurado se disponível

## Fase 3 — Camada Silver

### O que foi implementado

**`pipeline/silver/cleaning.py`** — Limpeza e deduplicação:
- Dedup por status (sent+delivered → mantém delivered)
- Flag `is_audio_transcription` com normalização do body

**`pipeline/silver/extraction.py`** — Extração de entidades via regex:
- CPF, email, CEP, telefone, placa de veículo, concorrentes
- Mascaramento de PII com hash SHA-256 determinístico

**`pipeline/silver/conversations.py`** — Agregação por conversa (~15k rows):
- Contagens, tempos, entidades extraídas, outcome, identidade do lead

**`frontend/pages/0_configuracao.py`** — Interface de configuração:
- Upload de amostra de dados brutos (parquet/csv) com preview e schema
- Editor de dicionário de dados (markdown)
- Editor de descrição dos KPIs (markdown)
- Salvar especificação e disparar pipeline

## Fase 4 — Camada Gold

### O que foi implementado

**`pipeline/gold/sentiment.py`** — Análise de sentimento por conversa:
- Score heurístico baseado em 4 fatores: outcome (peso 0.5), engajamento do lead (0.2), tempo de resposta (0.15), duração (0.15)
- Classificação em positivo/neutro/negativo com score contínuo [-1, 1]
- Coluna `sentimento_fatores` para explicabilidade
- Output: tabela Gold ~15k rows com sentimento por conversa

**`pipeline/gold/personas.py`** — Classificação de personas de leads:
- 5 personas: **Decidido** (fecha rápido), **Pesquisador** (muitas perguntas), **Negociador** (longo, compara), **Fantasma** (ghosting), **Indeciso** (sem decisão)
- Sistema de scoring multi-dimensão: cada conversa recebe score em todas as personas, a maior ganha
- Confiança (0-1) e fatores transparentes por conversa

**`pipeline/gold/segmentation.py`** — Segmentação multidimensional:
- **Engajamento**: alto (≥15 msgs) / médio (≥6) / baixo
- **Velocidade de resposta**: rápido (<2min) / moderado / lento
- **Veículo**: com/sem veículo mencionado
- **Região**: sudeste/sul/nordeste/centro-oeste/norte
- **Origem**: google/facebook/instagram/indicação/outros
- **Duração**: flash/curta/média/longa
- **Qualificação do lead**: alta (≥2 PII) / média / baixa

**`pipeline/gold/analytics.py`** — Analytics e KPIs:
- **Funil de conversão**: contagem e percentual por outcome
- **Lead scoring** (0-100): engajamento (30pts), tempo resposta (25pts), PII compartilhada (20pts), profundidade conversa (15pts), veículo (10pts)
- **Performance por campanha**: conversas, vendas, taxa conversão, tempo resposta

**`pipeline/gold/vendor_analysis.py`** — Métricas por vendedor:
- Contagens: total conversas, vendas, ghosting, perdidos por preço/concorrente
- Taxa de conversão e taxa de ghosting
- Média de mensagens, tempo de resposta, duração
- **Score do vendedor** (0-100): conversão (50%), retenção (20%), velocidade (30%)

**`agents/codegen_agent.py`** — Agente gerador de código (aprimorado):
- `recomendar_modulos_gold()`: recomendação heurística sem LLM — analisa palavras-chave nos KPIs
- `GOLD_MODULES` registry: catálogo de módulos disponíveis com metadata
- `analisar_spec()`: agora inclui campo `modulos_gold_recomendados`
- Fallback inteligente: se LLM indisponível, recomenda todos os módulos

**`pipeline/orchestrator.py`** — Gold step integrado:
- `_run_gold()`: executa todos os 5 módulos Gold em sequência
- Paths configuráveis via `settings.gold_*_path`
- Retorno consolidado com stats de cada módulo

### Testes (128 passando)

```
tests/test_phase1.py — 34 testes (fundação + specs)
tests/test_phase2.py — 16 testes (bronze + orchestrator + spec integration)
tests/test_phase3.py — 33 testes (silver + spec tools)
tests/test_phase4.py — 45 testes (gold + codegen agent)
```
