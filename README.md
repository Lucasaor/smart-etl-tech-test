# Agentic Pipeline

Pipeline de Transformação Agêntica de Dados — Arquitetura Medalhão (Bronze → Silver → Gold) gerenciado por agentes de IA autônomos.

## Visão Geral

Sistema que constrói e gerencia autonomamente um pipeline de dados em 3 camadas (Bronze → Silver → Gold) para dados transacionais de conversas de vendas via WhatsApp (~15k conversas, ~150k mensagens). Agentes de IA monitoram, executam e auto-corrigem o pipeline como infraestrutura persistente.

### Arquitetura

```
┌───────────────────────────────────────────────────────────────┐
│                     Frontend (Streamlit)                       │
│  Pipeline Monitor  │  Agent Monitor  │  Gold Dashboard         │
└───────────────────────────────────────────────────────────────┘
                              │
┌───────────────────────────────────────────────────────────────┐
│                   Agent Layer (LangGraph)                      │
│  Pipeline Agent  │  Monitor Agent  │  Repair Agent             │
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
| Frontend | **Streamlit** | Python nativo, funciona local e como Databricks App |
| Monitoring | **SQLite** (local) / **Delta tables** (Databricks) | Logs de pipeline runs e ações dos agentes |

## Estrutura do Projeto

```
.
├── pyproject.toml                 # Definição do projeto e dependências
├── .env.example                   # Template de variáveis de ambiente
├── conversations_bronze.parquet   # Dados fonte (~15k conversas)
│
├── config/                        # Configuração
│   ├── settings.py                # Pydantic Settings (runtime, paths, LLM)
│   └── llm_config.py              # Cadeia de fallback de modelos + cost tracking
│
├── core/                          # Infraestrutura
│   ├── storage.py                 # StorageBackend ABC → LocalDelta / Databricks
│   ├── compute.py                 # ComputeBackend ABC → Polars / Spark
│   └── events.py                  # EventBus pub/sub para pipeline events
│
├── agents/                        # Agentes de IA
│   ├── llm_provider.py            # LiteLLM wrapper (retry, fallback, budget)
│   └── tools/                     # (Fase 5) Ferramentas dos agentes
│
├── pipeline/                      # Transformações de dados
│   ├── bronze/                    # (Fase 2) Ingestão
│   ├── silver/                    # (Fase 3) Limpeza e extração
│   └── gold/                      # (Fase 4) Analytics e insights
│
├── monitoring/                    # Monitoramento
│   ├── models.py                  # Modelos: PipelineRun, StepRun, AgentAction, Alert
│   └── store.py                   # Persistência SQLite com queries
│
├── tests/                         # Testes
│   └── test_phase1.py             # 19 testes da Fase 1 (foundation)
│
├── frontend/                      # (Fase 6) Streamlit dashboards
├── docker/                        # (Fase 7) Docker Compose
└── databricks/                    # (Fase 8) Notebooks Databricks
```

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
| **1** | Fundação (config, storage, compute, events, LLM provider) | ✅ Completa |
| **2** | Camada Bronze (ingestão, incremental, orchestrator) | 🔲 Pendente |
| **3** | Camada Silver (limpeza, extração LLM, agregação) | 🔲 Pendente |
| **4** | Camada Gold (sentimento, personas, segmentação, analytics) | 🔲 Pendente |
| **5** | Sistema de Agentes (pipeline, monitor, repair) | 🔲 Pendente |
| **6** | Frontend Streamlit (3 dashboards) | 🔲 Pendente |
| **7** | Docker Compose | 🔲 Pendente |
| **8** | Migração Databricks | 🔲 Pendente |

## Fase 1 — Implementação Atual

### O que foi implementado

**`config/settings.py`** — Configuração centralizada via Pydantic Settings:
- `RUNTIME_ENV` (local / databricks) — determina backends de storage e compute
- Paths derivados automáticos para Bronze/Silver/Gold/Monitoring
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

### Testes (19 passing)

- `TestSettings`: configurações default, paths local, paths Databricks
- `TestLLMConfig`: config default, cost tracking, budget exceeded
- `TestLocalDeltaBackend`: write/read, append, versioning, not-exists, row count
- `TestPolarsCompute`: SQL query via DuckDB, read parquet
- `TestEventBus`: emit/subscribe, wildcard, error isolation
- `TestMonitoringStore`: pipeline runs, agent actions, alerts
