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

### Entradas do Sistema

O pipeline é orientado por **três entradas obrigatórias**, enviadas via Streamlit ou configuração:

| Entrada | Formato | Propósito |
|---------|---------|-----------|
| **Amostra de dados brutos** | Parquet ou CSV | Dados que serão processados na camada Bronze |
| **Dicionário de dados** | Markdown | Descreve colunas, tipos, valores esperados e particularidades |
| **Descrição dos KPIs** | Markdown | Define os indicadores e análises desejados na camada Gold |

Os agentes analisam essas entradas e implementam automaticamente os scripts do pipeline.

---

## Início Rápido

Existem **três formas** de executar a solução:

| Método | Quando usar |
|--------|------------|
| **Docker Compose** | Forma mais simples. Recomendado para avaliação e demonstrações |
| **Local (Python)** | Para desenvolvimento e testes |
| **Databricks Free Edition** | Produção com serverless e Unity Catalog |

---

## Opção 1: Docker Compose (Recomendado)

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose instalados
- Uma API key de LLM (OpenAI, Anthropic ou Google) **ou** Ollama para execução 100% local

### Passo a Passo

**1. Clonar o repositório:**

```bash
git clone https://github.com/Lucasaor/smart-etl-tech-test.git
cd smart-etl-tech-test
```

**2. Configurar variáveis de ambiente:**

```bash
cp .env.example .env
```

Edite o arquivo `.env` e configure pelo menos o modelo LLM e uma API key:

```bash
# Escolha um dos providers:

# OpenAI
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...

# OU Anthropic
LLM_MODEL=anthropic/claude-3-haiku-20240307
ANTHROPIC_API_KEY=sk-ant-...

# OU Google
LLM_MODEL=gemini/gemini-1.5-flash
GOOGLE_API_KEY=...

# OU Ollama (sem API key — veja passo 3b)
LLM_MODEL=ollama/llama3
```

**3a. Executar com API externa (OpenAI, Anthropic, Google):**

```bash
docker-compose -f docker/docker-compose.yml up --build
```

**3b. Executar com Ollama local (sem API key):**

```bash
docker-compose -f docker/docker-compose.yml --profile ollama up --build
```

> O serviço `ollama-setup` baixa automaticamente o modelo `llama3` na primeira execução.

**4. Acessar o dashboard:**

Abra `http://localhost:8501` no navegador.

**5. Usar a interface:**

1. Navegue para a página **Configuração** (barra lateral)
2. Faça upload da amostra de dados (`.parquet` ou `.csv`)
3. Cole ou edite o **Dicionário de Dados** (markdown)
4. Cole ou edite a **Descrição dos KPIs** (markdown)
5. Clique em **Salvar Especificação** e depois **Executar Pipeline**
6. Acompanhe a execução em **Pipeline Monitor** e **Agent Monitor**
7. Visualize os resultados em **Gold Dashboard**

### Serviços Docker

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| `streamlit` | 8501 | Dashboard + Pipeline (sempre ativo) |
| `ollama` | 11434 | LLM local (apenas com perfil `ollama`) |
| `ollama-setup` | — | Download automático do modelo llama3 |

### Comandos Úteis

```bash
# Parar os serviços
docker-compose -f docker/docker-compose.yml down

# Rebuild após alterações no código
docker-compose -f docker/docker-compose.yml up --build

# Ver logs em tempo real
docker-compose -f docker/docker-compose.yml logs -f streamlit

# Limpar dados e recomeçar
rm -rf data/bronze data/silver data/gold data/monitoring
```

---

## Opção 2: Execução Local (Python)

### Pré-requisitos

- Python 3.11+
- (Opcional) [Ollama](https://ollama.ai) para modelos LLM locais

### Passo a Passo

**1. Clonar e configurar o ambiente:**

```bash
git clone https://github.com/Lucasaor/smart-etl-tech-test.git
cd smart-etl-tech-test

# Criar ambiente virtual
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# Instalar dependências
pip install -e ".[dev]"
```

**2. Configurar variáveis de ambiente:**

```bash
cp .env.example .env
# Editar .env com suas API keys (mesma configuração descrita na seção Docker)
```

**3. Executar os testes (opcional):**

```bash
pytest tests/ -v
```

> 224 testes cobrindo todas as camadas: fundação, Bronze, Silver, Gold e agentes.

**4. Iniciar a interface Streamlit:**

```bash
streamlit run frontend/app.py
```

Acesse `http://localhost:8501` e siga os passos da seção "Usar a interface" acima.

---

## Opção 3: Databricks Free Edition

O pipeline roda na **Databricks Free Edition** usando compute **serverless** e **Unity Catalog Volumes**. Não é necessário criar ou gerenciar clusters.

### Pré-requisitos

- Conta [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Python 3.11+ local (para os scripts de setup)
- Uma API key de LLM

### Passo a Passo

**1. Instalar o SDK do Databricks localmente:**

```bash
pip install databricks-sdk
```

**2. Configurar credenciais:**

```bash
export DATABRICKS_HOST=https://seu-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

> O token pode ser gerado em **Settings → Developer → Access Tokens** no Databricks.

**3. Fazer upload dos dados para UC Volumes:**

```bash
# Setup completo: cria volume + faz upload dos dados e specs
python databricks/setup_volumes.py

# Opcional: incluir código gerado (se já existir em data/specs/generated)
python databricks/setup_volumes.py --include-generated
```

**4. Enviar API keys via Databricks Secrets:**

```bash
# Enviar variáveis do .env como secrets
python databricks/push_secrets.py

# Prévia sem enviar (dry run)
python databricks/push_secrets.py --dry-run
```

**5. Importar notebooks no Databricks:**

No Workspace do Databricks:
1. Clique em **Workspace** → **Repos** → **Add Repo**
2. Cole a URL do repositório: `https://github.com/Lucasaor/smart-etl-tech-test.git`
3. Os notebooks ficam em `databricks/notebooks/`

**6. Executar o pipeline:**

Existem duas abordagens:

| Notebook | Abordagem | Descrição |
|----------|-----------|-----------|
| `04_agent_orchestrator.py` | **Determinístico** | Executa Bronze → Silver → Gold sequencialmente usando o código já implementado |
| `05_agentic_pipeline.py` | **Agêntico (LLM)** | Gera código dinamicamente via LLM e executa — abordagem recomendada |

Execute o notebook escolhido diretamente no Databricks. O compute serverless é provisionado automaticamente.

### Notebooks Disponíveis

| Notebook | Camada | Descrição |
|----------|--------|-----------|
| `01_bronze.py` | Bronze | Ingestão com PySpark, validação de schema, Delta incremental |
| `02_silver.py` | Silver | Dedup, extração de entidades (UDFs), agregação por conversa |
| `03_gold.py` | Gold | Sentimento, personas, segmentação, lead scoring, vendedores |
| `04_agent_orchestrator.py` | Orquestração | Execução sequencial com verificação de saúde e detecção de novos dados |
| `05_agentic_pipeline.py` | **Agêntico** | Pipeline completo LLM-driven (recomendado) |

### Arquitetura Serverless

- **Compute**: serverless (provisionado automaticamente, sem gerenciamento)
- **Storage**: Unity Catalog Volumes (`/Volumes/<catalog>/default/pipeline_data/`)
- **Governance**: Unity Catalog built-in
- **Jobs**: até 5 Jobs agendáveis (substitui a limitação de Workflows do Community Edition)
- **LLM API keys**: via Databricks Secrets ou notebook widgets (sem env vars de cluster)

> Consulte [databricks/RUNBOOK.md](databricks/RUNBOOK.md) para o guia operacional completo.

---

## Configuração do LLM

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

---

## Estrutura do Projeto

```
.
├── pyproject.toml                 # Definição do projeto e dependências
├── .env.example                   # Template de variáveis de ambiente
│
├── config/                        # Configuração
│   ├── settings.py                # Pydantic Settings (runtime, paths, LLM, specs)
│   ├── llm_config.py              # Cadeia de fallback de modelos + cost tracking
│   └── databricks_notebook.py     # Helpers para bootstrap nos notebooks Databricks
│
├── core/                          # Infraestrutura
│   ├── storage.py                 # StorageBackend ABC → LocalDelta / Databricks
│   ├── compute.py                 # ComputeBackend ABC → Polars / Spark
│   └── events.py                  # EventBus pub/sub para pipeline events
│
├── pipeline/                      # Transformações de dados
│   ├── specs.py                   # ProjectSpec: dados brutos + dicionário + KPIs
│   ├── orchestrator.py            # Orquestrador spec-aware
│   ├── executor.py                # Execução segura de código gerado
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
│   ├── pipeline_agent.py          # LangGraph: execução end-to-end do pipeline
│   ├── monitor_agent.py           # LangGraph: monitoramento contínuo + alertas
│   ├── repair_agent.py            # LangGraph: diagnóstico e auto-correção de falhas
│   └── tools/
│       ├── spec_tools.py          # Ferramentas de análise de especificações
│       ├── data_tools.py          # Leitura, validação e amostragem de tabelas Delta
│       ├── pipeline_tools.py      # Disparar runs, status, histórico
│       └── quality_tools.py       # Nulos, duplicatas, schema, integridade
│
├── monitoring/                    # Monitoramento
│   ├── models.py                  # Modelos: PipelineRun, StepRun, AgentAction, Alert
│   └── store.py                   # Persistência SQLite com queries
│
├── frontend/                      # Dashboard Streamlit
│   ├── app.py                     # Entry point principal
│   ├── theme.py                   # Tema e estilização da UI
│   ├── viz_config.py              # Especificações de visualização dos KPIs
│   └── pages/
│       ├── 0_configuracao.py      # Upload: dados brutos, dicionário, KPIs
│       ├── 1_pipeline_monitor.py  # Status, histórico, timeline, alertas
│       ├── 2_agent_monitor.py     # Feed de ações, custos LLM, tokens
│       └── 3_gold_dashboard.py    # KPIs, gráficos, export de dados
│
├── tests/                         # Testes (224 testes passando)
│   ├── test_phase1.py             # Fundação + specs (34 testes)
│   ├── test_phase2.py             # Bronze + orchestrator (16 testes)
│   ├── test_phase3.py             # Silver + spec tools (33 testes)
│   ├── test_phase4.py             # Gold + codegen agent (45 testes)
│   └── test_phase5.py             # Agent system + LangGraph (96 testes)
│
├── docker/                        # Containerização
│   ├── Dockerfile                 # Imagem Python 3.11 + deps
│   └── docker-compose.yml         # Streamlit + Ollama (opcional)
│
└── databricks/                    # Integração Databricks (Free Edition)
    ├── push_secrets.py            # Envio de secrets do .env para Databricks
    ├── setup_volumes.py           # Upload de dados para UC Volumes
    ├── setup_dbfs.py              # Setup alternativo via DBFS
    ├── RUNBOOK.md                 # Guia operacional completo
    └── notebooks/
        ├── 01_bronze.py           # Ingestão (PySpark + Delta)
        ├── 02_silver.py           # Limpeza + Extração + Agregação
        ├── 03_gold.py             # Sentimento, Personas, Segmentação, Analytics, Vendedores
        ├── 04_agent_orchestrator.py  # Orquestração completa + detecção de novos dados
        └── 05_agentic_pipeline.py # Pipeline agêntico completo (LLM-driven)
```

---

## Detalhes da Implementação

### Camada Bronze — Ingestão

- Validação de schema contra dicionário de dados (14 colunas esperadas)
- Parse da coluna `metadata` (JSON string) → 6 colunas tipadas
- Cast de `timestamp` string → `Datetime`
- Adição de `_ingested_at` e `_source_file` para rastreabilidade
- 3 modos: `full` (overwrite), `incremental` (append novos), `auto` (detecção automática)

### Camada Silver — Limpeza e Extração

- **Deduplicação** por status (sent+delivered → mantém delivered)
- **Extração de entidades** via regex: CPF, email, CEP, telefone, placa de veículo, concorrentes
- **Mascaramento de PII** com hash SHA-256 determinístico
- **Agregação por conversa** (~15k rows): contagens, tempos, entidades extraídas, outcome

### Camada Gold — Analytics

- **Sentimento**: score heurístico baseado em outcome (50%), engajamento (20%), tempo de resposta (15%), duração (15%)
- **Personas**: 5 tipos (Decidido, Pesquisador, Negociador, Fantasma, Indeciso) com system de scoring multi-dimensão
- **Segmentação**: 7 dimensões (engajamento, velocidade, veículo, região, origem, duração, qualificação)
- **Funil de conversão**: contagem e percentual por outcome
- **Lead scoring** (0-100): engajamento (30pts), tempo resposta (25pts), PII (20pts), profundidade (15pts), veículo (10pts)
- **Performance por campanha**: conversas, vendas, taxa de conversão, tempo de resposta
- **Métricas por vendedor**: conversão, ghosting, velocidade, score (0-100)

### Sistema de Agentes

- **CodeGen Agent**: analisa spec → gera código Python/Polars para cada camada do pipeline
- **Pipeline Agent** (LangGraph): state machine — `load_spec` → `analyze` → `plan` → `execute` (B/S/G) → `validate` → `complete`
- **Monitor Agent** (LangGraph): loop cíclico — detecta novos dados, verifica saúde, dispara pipeline automaticamente
- **Repair Agent** (LangGraph): diagnóstico e auto-correção — 5 estratégias (retry, skip, regenerate, data fix, escalate)

### Monitoramento

- `PipelineRun`: registro de cada execução com steps, duração, status
- `AgentAction`: registro de ações dos agentes com tokens/custo LLM
- `Alert`: alertas com severidade, fonte e estado de resolução
- Persistência via SQLite (local) ou Delta tables (Databricks)

---

## Testes

224 testes automatizados organizados por fase:

```
tests/test_phase1.py — 34 testes (fundação: config, storage, compute, events, specs)
tests/test_phase2.py — 16 testes (bronze: ingestão, orchestrator, spec integration)
tests/test_phase3.py — 33 testes (silver: limpeza, extração, conversas, spec tools)
tests/test_phase4.py — 45 testes (gold: sentimento, personas, segmentação, analytics, vendedores)
tests/test_phase5.py — 96 testes (agentes: data tools, quality tools, pipeline/monitor/repair agents)
```

Para executar:

```bash
# Todos os testes
pytest tests/ -v

# Apenas uma fase
pytest tests/test_phase1.py -v

# Com cobertura
pytest tests/ --cov=pipeline --cov=agents --cov=core --cov=config --cov=monitoring
```

---

## Status de Implementação

| Fase | Descrição | Status |
|------|-----------|--------|
| **1** | Fundação (config, storage, compute, events, LLM provider, specs) | ✅ Completa |
| **2** | Camada Bronze (ingestão, incremental, orchestrator spec-aware) | ✅ Completa |
| **3** | Camada Silver (limpeza, extração, agregação, spec tools) | ✅ Completa |
| **4** | Camada Gold (sentimento, personas, segmentação, analytics, vendedores) | ✅ Completa |
| **5** | Sistema de Agentes (tools, pipeline, monitor, repair) | ✅ Completa |
| **6** | Frontend Streamlit (configuração + 3 dashboards) | ✅ Completa |
| **7** | Docker Compose (Streamlit + Ollama opcional) | ✅ Completa |
| **8** | Migração Databricks (5 notebooks + setup UC Volumes) | ✅ Completa |
