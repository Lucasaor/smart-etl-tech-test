## Plano: Pipeline de Transformação Agêntica de Dados

Pipeline de dados em camadas (Bronze → Silver → Gold) gerenciado por agentes de IA autônomos. Usa **LiteLLM** para abstração de LLMs (agnóstico a provider), **Delta Lake** como storage portável local↔Databricks, **Streamlit** para frontend, e **LangGraph** para orquestração dos agentes. Desenvolvimento começa em Docker Compose local, migra para Databricks free tier.

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

### Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| LLM Abstraction | **LiteLLM** | API unificada 100+ providers, incluindo Ollama; hot-swap via `.env` |
| Agent Framework | **LangGraph** | State machine robusto, funciona com qualquer LLM, bom para auto-correção |
| Storage | **Delta Lake** (`deltalake` Python pkg) | Funciona identicamente local e Databricks; ACID, time travel |
| Frontend | **Streamlit** | Python nativo, funciona local e como Databricks App |
| Compute local | **Polars + DuckDB** | Rápido para ~150k rows; adapter pattern para PySpark no Databricks |
| Monitoring | **SQLite** (local) / **Delta table** (Databricks) | Logs de agentes e pipeline runs |

### Estrutura do Projeto

```
agentic-pipeline/
├── pyproject.toml
├── .env.example
├── config/
│   ├── __init__.py
│   ├── settings.py            # Pydantic Settings: runtime env (local|databricks)
│   └── llm_config.py          # LLM provider/model config with fallback chain
├── core/
│   ├── __init__.py
│   ├── storage.py             # StorageBackend ABC → LocalDeltaBackend / DatabricksBackend
│   ├── compute.py             # ComputeBackend ABC → PolarsBackend / SparkBackend
│   └── events.py              # Event bus: pipeline events, agent actions
├── pipeline/
│   ├── __init__.py
│   ├── orchestrator.py        # Pipeline run sequencing, dependency tracking
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingestion.py       # Parquet → Delta bronze table
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── cleaning.py        # Dedup status, remove empty msgs, normalize names
│   │   ├── extraction.py      # LLM-assisted: PII, vehicle, competitor, claims extraction
│   │   └── conversations.py   # Aggregate messages into conversation-level records
│   └── gold/
│       ├── __init__.py
│       ├── personas.py        # LLM-assisted persona classification
│       ├── sentiment.py       # LLM-assisted sentiment analysis
│       ├── segmentation.py    # Audience segmentation rules + LLM
│       ├── analytics.py       # Email providers, conversion funnel, lead scoring
│       └── vendor_analysis.py # Agent/vendor performance metrics
├── agents/
│   ├── __init__.py
│   ├── llm_provider.py        # LiteLLM wrapper with retry, fallback, cost tracking
│   ├── pipeline_agent.py      # Creates and launches pipeline runs (LangGraph)
│   ├── monitor_agent.py       # Watches pipeline health, detects anomalies
│   ├── repair_agent.py        # Diagnoses failures, attempts auto-correction
│   └── tools/
│       ├── __init__.py
│       ├── data_tools.py      # Read/write/validate Delta tables
│       ├── pipeline_tools.py  # Trigger steps, check status, get logs
│       └── quality_tools.py   # Data quality checks, schema validation
├── monitoring/
│   ├── __init__.py
│   ├── models.py              # PipelineRun, StepRun, AgentAction data models
│   ├── store.py               # Persistence (SQLite local / Delta Databricks)
│   └── alerting.py            # Alert rules and notification
├── frontend/
│   ├── app.py                 # Streamlit main entry point
│   └── pages/
│       ├── 1_pipeline_monitor.py
│       ├── 2_agent_monitor.py
│       └── 3_gold_dashboard.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── databricks/
│   ├── notebooks/             # Auto-generated from pipeline/ modules
│   │   ├── 01_bronze.py
│   │   ├── 02_silver.py
│   │   ├── 03_gold.py
│   │   └── 04_agent_orchestrator.py
│   └── setup_dbfs.py          # Upload data and configure DBFS paths
├── tests/
│   ├── test_pipeline/
│   ├── test_agents/
│   └── test_integration/
├── data/
│   └── conversations_bronze.parquet
└── README.md
```

---

### FASE 1: Fundação (Infraestrutura e Abstrações)

1.1. Inicializar projeto com `pyproject.toml` — deps: `deltalake`, `polars`, `litellm`, `langgraph`, `streamlit`, `pydantic-settings`, `duckdb`

1.2. `config/settings.py` — Pydantic Settings com `RUNTIME_ENV` (local|databricks), `DATA_ROOT`, paths derivados para Bronze/Silver/Gold, LLM provider/model/base_url

1.3. `config/llm_config.py` — cadeia de fallback de modelos (ex: gpt-4o-mini → claude-haiku → ollama/llama3), config de custo máximo por run

1.4. `core/storage.py` — ABC `StorageBackend` com `read_table()`, `write_table()`, `table_exists()`, `get_table_version()`. Duas implementações: `LocalDeltaBackend` (deltalake+polars) e `DatabricksBackend` (spark.read.format("delta"))

1.5. `core/compute.py` — ABC `ComputeBackend` abstraindo operações de DataFrame. `PolarsCompute` local, `SparkCompute` Databricks. Transformações no pipeline são funções puras que recebem/retornam DataFrames

1.6. `core/events.py` — EventBus pub/sub in-process para `PipelineStarted`, `StepCompleted`, `StepFailed`, `AgentAction`, `DataIngested`

1.7. `agents/llm_provider.py` — wrapper LiteLLM com retry exponential backoff, logging de tokens/custo, structured output (JSON mode)

**Verificação**: unit tests para storage read/write, LLM mock call, settings init

---

### FASE 2: Camada Bronze

2.1. `pipeline/bronze/ingestion.py` — ler parquet com Polars, validar schema (13 colunas), parsear `metadata` JSON → colunas expandidas, adicionar `_ingested_at`, escrever Delta

2.2. Detecção incremental: comparar `max(timestamp)` da Bronze vs novos dados; modo append. Script auxiliar que simula chegada de novos dados (split parquet em batches)

2.3. `pipeline/orchestrator.py` — esqueleto: `run_pipeline(layers)`, tracking de status por step, emissão de eventos

**Verificação**: Bronze table com ~120-150k rows, schema validado, incremental ingestion funciona, Delta version history

---

### FASE 3: Camada Silver

3.1. `pipeline/silver/cleaning.py` — dedup status (sent+delivered → keep delivered), tratar body nulo/vazio, normalizar sender_name (lowercase+strip, agrupar por sender_phone), flag `is_audio_transcription`

3.2. `pipeline/silver/extraction.py` — **LLM-assisted**: extrair de message_body CPF/email/telefone/CEP/placa/veículo/concorrentes/sinistros. **Hybrid**: regex para padrões claros, LLM apenas para ambíguos. Batch por conversa. Mascarar dados sensíveis com hash determinístico

3.3. `pipeline/silver/conversations.py` — agregar mensagens → nível conversa: message_count, duration, response_times, extracted entities, outcome. Gera tabela Silver-mensagem (limpa) + Silver-conversa (agregada, ~15k rows)

**Verificação**: sem duplicatas de status, Silver-conversa ~15k rows, spot check de extrações, dados mascarados

---

### FASE 4: Camada Gold

4.1. `pipeline/gold/sentiment.py` — **LLM-assisted**: sentimento por conversa (positivo/neutro/negativo + score), batch com cache

4.2. `pipeline/gold/personas.py` — **LLM-assisted**: classificar leads (Pesquisador, Decidido, Negociador, Fantasma, Indeciso) baseado em comportamento conversacional

4.3. `pipeline/gold/segmentation.py` — segmentação multidimensional: por veículo (popular/médio/premium), engajamento (frio/morno/quente), origem (campaign+lead_source), geografia, horário

4.4. `pipeline/gold/analytics.py` — tabelas: `gold_email_providers`, `gold_conversion_funnel`, `gold_lead_scoring`, `gold_competitor_analysis`, `gold_vehicle_demand`

4.5. `pipeline/gold/vendor_analysis.py` — métricas por vendedor: taxa conversão, tempo resposta, sentimento médio, distribuição outcomes

**Verificação**: tabelas Gold populadas, distribuição de personas coerente, sentimento alinhado com spot check, atualização automática quando Silver muda

---

### FASE 5: Sistema de Agentes

5.1. `agents/tools/` — `data_tools.py` (read/validate/sample Delta), `pipeline_tools.py` (trigger/status/history), `quality_tools.py` (nulls/duplicates/schema/compare)

5.2. `agents/pipeline_agent.py` — **LangGraph state machine**: analyze_source → plan → execute_bronze → execute_silver → execute_gold → validate → complete. Falha → transita para diagnose_failure

5.3. `agents/monitor_agent.py` — **LangGraph loop**: polling periódico, detecta novos dados em Bronze, verifica saúde das tabelas, triggera pipeline_agent, gera alertas

5.4. `agents/repair_agent.py` — invocado em falha: get_error → analyze (LLM) → propose_fix → apply → retry → validate. Exemplos: schema mismatch, LLM timeout (fallback model), data quality drop. Se não corrige, gera relatório

5.5. `monitoring/models.py` + `monitoring/store.py` — modelos `PipelineRun`, `StepRun`, `AgentAction`, `Alert`. SQLite local, Delta Databricks. Cada ação registrada com timestamp, tokens, custo

**Verificação**: pipeline end-to-end sem intervenção, simular falha → repair agent corrige, simular novos dados → monitor detecta e re-run, trocar LLM provider → continua funcionando

---

### FASE 6: Frontend (Streamlit)

6.1. `frontend/app.py` — layout com sidebar, conexão a monitoring store e Delta tables, auto-refresh

6.2. `frontend/pages/1_pipeline_monitor.py` — status atual, histórico de runs, timeline visual Bronze→Silver→Gold, métricas (rows, tempo, erros), alertas

6.3. `frontend/pages/2_agent_monitor.py` — feed de ações por agente, detalhes (input/decisão/output), métricas de tokens/custo, filtros

6.4. `frontend/pages/3_gold_dashboard.py` — KPIs (total conversas, taxa conversão), gráficos: distribuição personas, funnel, sentimento por campanha, email providers, veículos top, performance vendedores, scoring distribution. Filtros por período/campanha/vendedor

**Verificação**: 3 páginas sem erro, dados refletem estado real do pipeline, filtros funcionais

---

### FASE 7: Docker Compose

7.1. `docker/Dockerfile` — python:3.11-slim, deps do pyproject.toml, volume mount para data/

7.2. `docker/docker-compose.yml` — serviços: `pipeline-agent` (long-running), `streamlit` (port 8501), `ollama` (opcional). Volumes compartilhados

7.3. Teste integração: `docker-compose up` → pipeline executa → dashboard disponível → simular novos dados → atualização automática

**Verificação**: `docker-compose up` sobe tudo, pipeline roda automaticamente, dashboard em localhost:8501, modo Ollama funciona sem API key

---

### FASE 8: Migração Databricks

8.1. Notebooks `databricks/notebooks/01-04_*.py` — importam módulos do pipeline com `SparkBackend`

8.2. Upload parquet para DBFS via `databricks/setup_dbfs.py`

8.3. Scheduling: `dbutils.notebook.run()` com triggers ou notebook com loop (Community Edition não tem Workflows)

8.4. Frontend: notebook dashboards com `displayHTML()`/widgets, ou Streamlit separado lendo de DBFS

**Verificação**: pipeline roda no cluster, Delta tables no DBFS versionadas, agent funciona com API LLM, dashboard visualiza Gold

---

### Considerações Importantes

1. **Modelo LLM recomendado**: GPT-4o-mini ou Claude Haiku para produção (custo-benefício); Ollama+Llama3 para testes locais. Troca via `.env` sem mudança de código

2. **Volume de chamadas LLM**: ~15k conversas. Hybrid regex+LLM reduz chamadas. Estimativa: ~1500 chamadas na Silver, ~800 na Gold. Batch de 10-20 conversas por chamada + cache

3. **Databricks Community Edition**: sem Workflows, cluster auto-termina após 2h de inatividade. "Pipeline vivo" será simulado via notebook periódico ou trigger manual — documentar essa limitação
