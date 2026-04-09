## Plano: Pipeline de Transformação Agêntica de Dados

Pipeline de dados em camadas (Bronze → Silver → Gold) **gerado e gerenciado por agentes de IA autônomos**. O sistema **recebe como entrada**: uma amostra dos dados brutos, um dicionário de dados e uma descrição dos KPIs desejados. Os agentes analisam essas entradas e **implementam automaticamente** os scripts do pipeline, em código Python limpo e documentado em português brasileiro.

Usa **LiteLLM** para abstração de LLMs (agnóstico a provider), **Delta Lake** como storage portável local↔Databricks, **Streamlit** para frontend (incluindo interface de entrada de dados), e **LangGraph** para orquestração dos agentes. Desenvolvimento começa em Docker Compose local, migra para Databricks free tier.

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
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Frontend (Streamlit)                              │
│  Configuração  │  Pipeline Monitor  │  Agent Monitor  │  Dashboard   │
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

### Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| LLM Abstraction | **LiteLLM** | API unificada 100+ providers, incluindo Ollama; hot-swap via `.env` |
| Agent Framework | **LangGraph** | State machine robusto, funciona com qualquer LLM, bom para auto-correção |
| Storage | **Delta Lake** (`deltalake` Python pkg) | Funciona identicamente local e Databricks; ACID, time travel |
| Frontend | **Streamlit** | Python nativo, funciona local e como Databricks App; interface de entrada de dados |
| Compute local | **Polars + DuckDB** | Rápido para ~150k rows; adapter pattern para PySpark no Databricks |
| Monitoring | **SQLite** (local) / **Delta table** (Databricks) | Logs de agentes e pipeline runs |
| Código gerado | **Português brasileiro** | Documentação, docstrings e comentários em pt-BR |

### Estrutura do Projeto

```
agentic-pipeline/
├── pyproject.toml
├── .env.example
├── config/
│   ├── __init__.py
│   ├── settings.py            # Pydantic Settings: runtime env (local|databricks), paths
│   └── llm_config.py          # LLM provider/model config with fallback chain
├── core/
│   ├── __init__.py
│   ├── storage.py             # StorageBackend ABC → LocalDeltaBackend / DatabricksBackend
│   ├── compute.py             # ComputeBackend ABC → PolarsBackend / SparkBackend
│   └── events.py              # Event bus: pipeline events, agent actions
├── pipeline/
│   ├── __init__.py
│   ├── orchestrator.py        # Pipeline run sequencing, dependency tracking
│   ├── specs.py               # ProjectSpec: dados brutos + dicionário + KPIs
│   ├── bronze/
│   │   ├── __init__.py
│   │   ├── ingestion.py       # Parquet → Delta bronze table (spec-aware)
│   │   └── simulator.py       # Simula chegada incremental de dados
│   ├── silver/
│   │   ├── __init__.py
│   │   ├── cleaning.py        # Dedup status, normalização, flags de conteúdo
│   │   ├── extraction.py      # Regex + LLM: PII, veículos, concorrentes
│   │   └── conversations.py   # Agregar mensagens → nível conversa
│   └── gold/
│       ├── __init__.py
│       ├── personas.py        # LLM: classificação de personas
│       ├── sentiment.py       # LLM: análise de sentimento
│       ├── segmentation.py    # Segmentação multidimensional
│       ├── analytics.py       # KPIs: email, funnel, scoring, concorrentes
│       └── vendor_analysis.py # Métricas por vendedor
├── agents/
│   ├── __init__.py
│   ├── llm_provider.py        # LiteLLM wrapper com retry, fallback, budget
│   ├── codegen_agent.py       # Agente gerador: analisa spec → gera pipeline code
│   ├── pipeline_agent.py      # Executa pipeline runs (LangGraph)
│   ├── monitor_agent.py       # Monitora saúde do pipeline
│   ├── repair_agent.py        # Auto-correção em caso de falha
│   └── tools/
│       ├── __init__.py
│       ├── spec_tools.py      # Ferramentas para análise de especificações
│       ├── data_tools.py      # Read/write/validate Delta tables
│       ├── pipeline_tools.py  # Trigger steps, check status, get logs
│       └── quality_tools.py   # Data quality checks, schema validation
├── monitoring/
│   ├── __init__.py
│   ├── models.py              # PipelineRun, StepRun, AgentAction, Alert
│   ├── store.py               # Persistência SQLite / Delta
│   └── alerting.py            # Regras de alerta e notificação
├── frontend/
│   ├── __init__.py
│   ├── app.py                 # Streamlit main entry point
│   └── pages/
│       ├── 0_configuracao.py  # Upload: dados brutos, dicionário, KPIs
│       ├── 1_pipeline_monitor.py
│       ├── 2_agent_monitor.py
│       └── 3_gold_dashboard.py
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze.py
│   │   ├── 02_silver.py
│   │   ├── 03_gold.py
│   │   └── 04_agent_orchestrator.py
│   └── setup_dbfs.py
├── tests/
│   ├── __init__.py
│   ├── test_phase1.py
│   ├── test_phase2.py
│   └── test_phase3.py
├── data/
│   ├── specs/                 # Especificações do projeto (gerado pelo usuário)
│   │   ├── dados_brutos.parquet
│   │   ├── dicionario_dados.md
│   │   └── descricao_kpis.md
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── monitoring/
└── README.md
```

---

### FASE 1: Fundação (Infraestrutura, Abstrações e Especificações)

1.1. Inicializar projeto com `pyproject.toml` — deps: `deltalake`, `polars`, `litellm`, `langgraph`, `streamlit`, `pydantic-settings`, `duckdb`

1.2. `config/settings.py` — Pydantic Settings com `RUNTIME_ENV` (local|databricks), `DATA_ROOT`, paths derivados para Bronze/Silver/Gold, **`SPEC_DIR`** para especificações do projeto, LLM provider/model/base_url

1.3. `config/llm_config.py` — cadeia de fallback de modelos (ex: gpt-4o-mini → claude-haiku → ollama/llama3), config de custo máximo por run

1.4. `core/storage.py` — ABC `StorageBackend` com `read_table()`, `write_table()`, `table_exists()`, `get_table_version()`. Duas implementações: `LocalDeltaBackend` (deltalake+polars) e `DatabricksBackend` (spark.read.format("delta"))

1.5. `core/compute.py` — ABC `ComputeBackend` abstraindo operações de DataFrame. `PolarsCompute` local, `SparkCompute` Databricks. Transformações no pipeline são funções puras que recebem/retornam DataFrames

1.6. `core/events.py` — EventBus pub/sub in-process para `PipelineStarted`, `StepCompleted`, `StepFailed`, `AgentAction`, `DataIngested`

1.7. `agents/llm_provider.py` — wrapper LiteLLM com retry exponential backoff, logging de tokens/custo, structured output (JSON mode)

1.8. `pipeline/specs.py` — **Modelo de especificação do projeto** (`ProjectSpec`): armazena e valida as 3 entradas obrigatórias:
  - **Amostra de dados brutos**: caminho para arquivo parquet/csv com dados que irão para a camada Bronze
  - **Dicionário de dados**: markdown descrevendo cada coluna, tipos, valores esperados
  - **Descrição dos KPIs**: markdown descrevendo os indicadores desejados na camada Gold
  - Funções para salvar, carregar e analisar automaticamente o schema dos dados de entrada

**Verificação**: unit tests para storage read/write, LLM mock call, settings init, **spec salvar/carregar/validar/analisar**

---

### FASE 2: Camada Bronze

2.1. `pipeline/bronze/ingestion.py` — ler dados brutos (path definido na spec) com Polars, validar schema conforme dicionário de dados, parsear colunas JSON → colunas expandidas, adicionar `_ingested_at`, escrever Delta

2.2. Detecção incremental: comparar `max(timestamp)` da Bronze vs novos dados; modo append. Script auxiliar que simula chegada de novos dados (split parquet em batches)

2.3. `pipeline/orchestrator.py` — esqueleto: `run_pipeline(layers, spec)`, tracking de status por step, emissão de eventos. **Aceita ProjectSpec** para configurar paths e parâmetros dinamicamente

**Verificação**: Bronze table com ~120-150k rows, schema validado, incremental ingestion funciona, Delta version history, **spec carregada corretamente pelo orchestrator**

---

### FASE 3: Camada Silver

3.1. `pipeline/silver/cleaning.py` — dedup status (sent+delivered → keep delivered), tratar body nulo/vazio, normalizar sender_name (lowercase+strip, agrupar por sender_phone), flag `is_audio_transcription`

3.2. `pipeline/silver/extraction.py` — **Hybrid regex+LLM**: extrair de message_body CPF/email/telefone/CEP/placa/veículo/concorrentes/sinistros. Regex para padrões claros, LLM apenas para ambíguos. Batch por conversa. Mascarar dados sensíveis com hash determinístico

3.3. `pipeline/silver/conversations.py` — agregar mensagens → nível conversa: message_count, duration, response_times, extracted entities, outcome. Gera tabela Silver-mensagem (limpa) + Silver-conversa (agregada, ~15k rows)

**Verificação**: sem duplicatas de status, Silver-conversa ~15k rows, spot check de extrações, dados mascarados

---

### FASE 4: Camada Gold (Agente Gerador de Código)

4.1. `agents/codegen_agent.py` — **Agente que gera os scripts da camada Gold** a partir da descrição dos KPIs. Analisa:
  - A spec do projeto (dicionário + KPIs)
  - O schema das tabelas Silver já criadas
  - Gera código Python documentado em pt-BR para cada módulo Gold

4.2. `pipeline/gold/sentiment.py` — **Gerado pelo agente**: sentimento por conversa (positivo/neutro/negativo + score), batch com cache

4.3. `pipeline/gold/personas.py` — **Gerado pelo agente**: classificar leads (Pesquisador, Decidido, Negociador, Fantasma, Indeciso) baseado em comportamento conversacional

4.4. `pipeline/gold/segmentation.py` — **Gerado pelo agente**: segmentação multidimensional: por veículo, engajamento, origem, geografia, horário

4.5. `pipeline/gold/analytics.py` — **Gerado pelo agente**: tabelas conforme KPIs descritos (email providers, funnel, lead scoring, etc.)

4.6. `pipeline/gold/vendor_analysis.py` — **Gerado pelo agente**: métricas por vendedor: taxa conversão, tempo resposta, sentimento médio

**Verificação**: agente gera código Gold válido a partir das descrições, tabelas Gold populadas, KPIs alinhados com a descrição fornecida, atualização automática quando Silver muda

---

### FASE 5: Sistema de Agentes

5.1. `agents/tools/spec_tools.py` — ferramentas para análise de especificações: ler spec, analisar schema da amostra, extrair lista de colunas, sugerir transformações
5.2. `agents/tools/data_tools.py` — read/validate/sample Delta tables
5.3. `agents/tools/pipeline_tools.py` — trigger/status/history
5.4. `agents/tools/quality_tools.py` — nulls/duplicates/schema/compare

5.5. `agents/pipeline_agent.py` — **LangGraph state machine**: load_spec → analyze_data → plan → execute_bronze → execute_silver → generate_gold → execute_gold → validate → complete

5.6. `agents/monitor_agent.py` — **LangGraph loop**: polling periódico, detecta novos dados em Bronze, verifica saúde das tabelas, triggera pipeline_agent, gera alertas

5.7. `agents/repair_agent.py` — invocado em falha: get_error → analyze (LLM) → propose_fix → apply → retry → validate

5.8. `monitoring/models.py` + `monitoring/store.py` — modelos `PipelineRun`, `StepRun`, `AgentAction`, `Alert`. SQLite local, Delta Databricks

**Verificação**: pipeline end-to-end sem intervenção, simular falha → repair agent corrige, simular novos dados → monitor detecta e re-run, trocar LLM provider → continua funcionando

---

### FASE 6: Frontend (Streamlit)

6.1. `frontend/app.py` — layout com sidebar, conexão a monitoring store e Delta tables, auto-refresh

6.2. `frontend/pages/0_configuracao.py` — **Interface de configuração do projeto**:
  - Upload de amostra de dados brutos (parquet/csv)
  - Editor de dicionário de dados (markdown)
  - Editor de descrição dos KPIs (markdown)
  - Botão "Iniciar Pipeline" que salva a spec e dispara o agente
  - Visualização do schema detectado e preview dos dados

6.3. `frontend/pages/1_pipeline_monitor.py` — status atual, histórico de runs, timeline visual Bronze→Silver→Gold, métricas (rows, tempo, erros), alertas

6.4. `frontend/pages/2_agent_monitor.py` — feed de ações por agente (incluindo CodeGen), detalhes (input/decisão/output), métricas de tokens/custo, filtros

6.5. `frontend/pages/3_gold_dashboard.py` — KPIs dinâmicos conforme descrição fornecida, gráficos: distribuição personas, funnel, sentimento, performance. Filtros por período/campanha/vendedor

**Verificação**: 4 páginas sem erro, upload funcional, dados refletem estado real do pipeline, filtros funcionais

---

### FASE 7: Docker Compose

7.1. `docker/Dockerfile` — python:3.11-slim, deps do pyproject.toml, volume mount para data/

7.2. `docker/docker-compose.yml` — serviços: `pipeline-agent` (long-running), `streamlit` (port 8501), `ollama` (opcional). Volumes compartilhados

7.3. Teste integração: `docker-compose up` → dashboard em localhost:8501 → upload dados + dicionário + KPIs → agente gera pipeline → pipeline executa → dashboard atualizado

**Verificação**: `docker-compose up` sobe tudo, upload funciona, pipeline roda automaticamente, dashboard em localhost:8501, modo Ollama funciona sem API key

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

4. **Código documentado em pt-BR**: docstrings, comentários, nomes de variáveis descritivos e logs em português brasileiro. O agente CodeGen gera código seguindo este padrão

5. **Entradas do sistema**: o pipeline é genérico — recebe dados brutos, dicionário e KPIs e adapta o processamento. O agente CodeGen analisa as entradas e gera os scripts adequados para cada caso de uso
