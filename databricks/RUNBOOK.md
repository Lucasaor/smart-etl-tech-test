# Databricks Community Edition — Runbook Operacional

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────┐
│                LOCAL (Docker)                   │
│  ┌──────────────┐    ┌────────────────────────┐ │
│  │  Streamlit   │    │  CodeGen + Agents      │ │
│  │  Dashboard   │    │  (LiteLLM/LangGraph)   │ │
│  └──────────────┘    └────────────────────────┘ │
│        │                       │                │
│  ┌─────┴───────────────────────┴──────────────┐ │
│  │     Pipeline Local (Polars + DuckDB)       │ │
│  │     Delta Lake local (./data/)             │ │
│  │     SQLite monitoring (./data/monitoring/) │ │
│  └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│            DATABRICKS (Community Edition)               │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │  05_agentic_pipeline.py (RECOMMENDED)              │ │
│  │  ┌───────────┐  ┌──────────┐  ┌────────────────┐   │ │
│  │  │ CodeGen   │→ │ Executor │→ │ Delta Tables   │   │ │
│  │  │ Agent     │  │ (Polars) │  │ (DBFS FUSE)    │   │ │
│  │  │ (LiteLLM) │  │          │  │                │   │ │
│  │  └───────────┘  └──────────┘  └────────────────┘   │ │
│  │  RUNTIME_ENV=local | DATA_ROOT=/dbfs/delta         │ │
│  │  LocalDeltaBackend (deltalake) via FUSE mount      │ │
│  └────────────────────────────────────────────────────┘ │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │  04_agent_orchestrator.py (PySpark fallback)       │ │
│  └──────┬──────────┬──────────┬───────────────────────┘ │
│         ▼          ▼          ▼                         │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                 │
│  │01_bronze │ │02_silver │ │03_gold   │                 │
│  │(PySpark) │ │(PySpark) │ │(PySpark) │                 │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                 │
│       ▼            ▼            ▼                       │
│  ┌────────────────────────────────────────────────────┐ │
│  │  Delta Lake DBFS                                   │ │
│  │  PySpark: /delta/bronze  |  FUSE: /dbfs/delta/...  │ │
│  │  ├── bronze/    ├── silver/   ├── gold/            │ │
│  │  ├── specs/     └── monitoring/                    │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Pré-requisitos

1. **Conta Databricks Community Edition** ativa em [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. **Cluster criado** (Community Edition permite 1 cluster)
3. **Databricks CLI** configurado localmente:
   ```bash
   pip install databricks-sdk
   databricks configure --profile DEFAULT
   ```
4. **Variáveis de ambiente** (ou perfil CLI):
   - `DATABRICKS_HOST` — URL do workspace (ex: `https://community.cloud.databricks.com`)
   - `DATABRICKS_TOKEN` — Token de acesso pessoal

---

## Setup Inicial

### 1. Preparar DBFS

```bash
# Criar diretórios + upload de dados e specs
python databricks/setup_dbfs.py --profile DEFAULT

# Incluir artefatos gerados (se já existirem)
python databricks/setup_dbfs.py --profile DEFAULT --include-generated
```

### 2. Importar Repo no Workspace

Opção A — **Repos** (recomendado):
1. Workspace → Repos → Add Repo
2. URL: `https://github.com/Lucasaor/smart-etl-tech-test`
3. O repo estará em `/Workspace/Repos/Lucasaor/smart-etl-tech-test`

Opção B — **Upload manual de notebooks**:
1. Workspace → Import
2. Importar `databricks/notebooks/*.py` como notebooks Python

### 3. Configurar `PROJECT_REPO_PATH` (se necessário)

Se o caminho do repo no workspace diferir do padrão, defina no cluster:
- Cluster → Edit → Spark Config → Environment Variables:
  ```
  PROJECT_REPO_PATH=/Workspace/Repos/<seu-usuario>/smart-etl-tech-test
  ```

---

## Execução do Pipeline

### Pipeline Agêntico — `05_agentic_pipeline.py` (RECOMENDADO)

O notebook `05_agentic_pipeline.py` executa o **pipeline agêntico completo** no Databricks,
replicando exatamente o que o Streamlit faz localmente:

1. Carrega os **mesmos 3 inputs** (parquet, dicionário de dados, descrição KPIs)
2. Analisa os dados e cria um `ProjectSpec`
3. Usa o `CodeGenAgent` (LLM) para gerar código Python/Polars dinamicamente
4. Executa o código gerado → escreve Delta tables em DBFS
5. Valida e exibe resultados

**Como funciona:**

```
RUNTIME_ENV=local + DATA_ROOT=/dbfs/delta + FORCE_DELTA_MONITORING=true

   ┌─────────────┐    ┌──────────────┐    ┌───────────────┐
   │ LLM (API)   │ ←─ │ CodeGenAgent │ ─→ │ Generated     │
   │ gpt-4o-mini │    │  + Repair    │    │ Python/Polars │
   │ (outbound)  │    │  Agent       │    │ ETL code      │
   └─────────────┘    └──────────────┘    └───────┬───────┘
                                                   │ exec()
                                                   ▼
                                          ┌───────────────┐
                                          │ Executor      │
                                          │ read_table()  │
                                          │ write_table() │
                                          └───────┬───────┘
                                                   │ deltalake library
                                                   ▼
                                          ┌───────────────┐
                                          │ DBFS (FUSE)   │
                                          │ /dbfs/delta/  │
                                          │ ├── bronze/   │
                                          │ ├── silver/   │
                                          │ └── gold/     │
                                          └───────────────┘
```

- Usa `LocalDeltaBackend` (deltalake library) via FUSE mount `/dbfs/`
- NÃO requer PySpark para o pipeline (apenas para verificação final opcionalmente)
- Tabelas Delta criadas são acessíveis via PySpark: `spark.read.format("delta").load("/delta/bronze")`
- Monitoring usa Delta tables (não SQLite) via `FORCE_DELTA_MONITORING=true`

**Setup:**

1. **LLM API key** — uma das opções:
   - Cluster Environment Variables: `OPENAI_API_KEY=sk-...`
   - Databricks Secrets: `dbutils.secrets.get("llm", "openai_api_key")`
2. **Upload dos 3 arquivos de input** para DBFS (instruções no notebook)
3. **Executar o notebook** célula por célula ou Run All

**Re-execução:**

| Cenário | Ação |
|---------|------|
| Regenerar código | Deletar `/dbfs/delta/specs/generated/pipeline_meta.json` e re-executar do Cell 5 |
| Re-executar com código existente | Re-executar do Cell 6 |
| Novos dados | Substituir arquivos em `/dbfs/delta/specs/` e re-executar do Cell 4 |
| Cleanup total | `dbutils.fs.rm("/delta/", recurse=True)` |

### Pipeline PySpark — notebooks 01-04 

Os notebooks 01-04 são uma alternativa PySpark pré-codificada. Útil quando:
- Não há API key de LLM disponível

- Para debug de camadas individuais

**Execução Completa:**

1. Abrir `04_agent_orchestrator.py` no workspace
2. Attach ao cluster ativo
3. **Run All** — executa Bronze → Silver → Gold em sequência
4. Verificar resumo na célula de resultados

**Execução por Camada (debug/reprocessamento):**

```
01_bronze.py  → Ingestão de Parquet bruto → Delta Bronze
02_silver.py  → Limpeza + Extração + Agregação
03_gold.py    → Analytics, Sentimento, Personas, Segmentação, Vendedores
```

Cada notebook emite um resultado estruturado JSON via `dbutils.notebook.exit()`.

---

## Restrições do Community Edition

| Restrição | Impacto | Mitigação |
|-----------|---------|-----------|
| **Sem Workflows/Jobs** | Impossível agendar execução automática | Execução manual via notebook |
| **Cluster auto-stop (2h idle)** | Cluster desliga após 2h sem atividade | Reiniciar manualmente antes de executar |
| **1 cluster por vez** | Sem paralelismo de clusters | Pipeline sequencial já é o design |
| **Sem Unity Catalog** | Sem governance avançado de tabelas | Usar paths DBFS diretos |
| **Sem Structured Streaming** | Sem ingestão em tempo real | Pipeline batch-only (compatível) |

---

## Troubleshooting

### Cluster parou durante execução

**Sintoma**: Notebook falha com "cluster terminated" ou "connection lost"

**Solução**:
1. Ir a Compute → selecionar cluster → Start
2. Aguardar cluster ficar "Running" (2-5 min)
3. Re-executar o notebook que falhou
4. Se dados parciais foram escritos, executar do início da camada (modo overwrite limpa dados anteriores)

### Erro "No active SparkSession"

**Sintoma**: `RuntimeError: No active SparkSession`

**Solução**: O notebook não está attached a um cluster. Clicar em "Attach" no topo do notebook.

### Erro de path "/mnt/delta/..."

**Sintoma**: `FileNotFoundException` ou `AnalysisException` com path DBFS

**Solução**:
1. Verificar se `setup_dbfs.py` foi executado com sucesso
2. Verificar se dados de entrada existem em `/mnt/delta/specs/`:
   ```python
   dbutils.fs.ls("/mnt/delta/specs/")
   ```
3. Se necessário, re-executar setup:
   ```bash
   python databricks/setup_dbfs.py --profile DEFAULT
   ```

### Erro de import "config.*"

**Sintoma**: `ModuleNotFoundError: No module named 'config'`

**Solução**:
1. Verificar se o repo está importado no workspace (Repos)
2. Verificar `PROJECT_REPO_PATH` no env do cluster
3. Na célula de bootstrap do notebook, verificar se `repo_path` está correto

### Dados locais vs Databricks divergem

**Diagnóstico**:
1. Localmente:
   ```python
   import polars as pl
   from deltalake import DeltaTable
   dt = DeltaTable("./data/bronze")
   print(f"Local Bronze: {len(dt.to_pyarrow_table())} rows")
   ```
2. No Databricks:
   ```python
   df = spark.read.format("delta").load("/mnt/delta/bronze")
   print(f"Databricks Bronze: {df.count()} rows")
   ```
3. Comparar row counts e schemas

---

## Monitoramento

### No Databricks
- Cada notebook salva metadados de execução em `/mnt/delta/monitoring/notebook_runs`
- O orquestrador salva resumo do pipeline completo
- Verificar via:
  ```python
  df = spark.read.format("delta").load("/mnt/delta/monitoring/notebook_runs")
  display(df.orderBy(F.desc("timestamp")))
  ```

### Verificação de Saúde das Tabelas
- Executar o bloco "Verificação de Saúde" em `04_agent_orchestrator.py`
- Mostra row count, colunas e versão Delta de cada tabela

### Local (Docker)
- Dashboard Streamlit em `http://localhost:8501`
- Pipeline Monitor mostra runs locais (SQLite)
- Não inclui runs do Databricks (ambientes independentes)

---

## Fluxo Híbrido: Local + Databricks

### Cenário A: Desenvolvimento 100% Local
```bash
docker-compose -f docker/docker-compose.yml up
# Acessar http://localhost:8501
# Upload de specs + executar pipeline via UI
```

### Cenário B: Local UI + Databricks ETL
1. Gerar specs e código localmente via Streamlit
2. Sincronizar artefatos para DBFS:
   ```bash
   python databricks/setup_dbfs.py --include-generated
   ```
3. Executar notebooks manualmente no Databricks
4. Resultados ficam em Delta no DBFS

### Cenário C: Fallback quando Databricks indisponível
- Pipeline local funciona independentemente
- Sem dependência do Databricks para desenvolvimento ou testes
- Todos os testes (`pytest`) rodam localmente sem cluster

---

## Checklist de Validação Pós-Setup

### Pipeline Agêntico (05_agentic_pipeline.py)
- [ ] Cluster Databricks ativo (status "Running")
- [ ] Repo importado e visível no workspace
- [ ] LLM API key configurada (env var ou secrets)
- [ ] 3 input files presentes em `/dbfs/delta/specs/`
- [ ] `%pip install` executou sem erros
- [ ] `CodeGenAgent` gerou código para Bronze, Silver e Gold modules
- [ ] Pipeline executou com sucesso (todos steps "completed")
- [ ] Bronze Delta table contém dados (`spark.read.format("delta").load("/delta/bronze")`)
- [ ] Silver tables contêm dados
- [ ] Gold tables contêm dados
- [ ] `/dbfs/delta/monitoring/` contém registros Delta

### Pipeline PySpark (01-04, fallback)
- [ ] `python databricks/setup_dbfs.py --profile DEFAULT` executou sem erros
- [ ] `01_bronze.py` executa e mostra row count > 0
- [ ] `02_silver.py` executa e cria Silver Messages + Conversations
- [ ] `03_gold.py` executa e cria 5 tabelas Gold
- [ ] `04_agent_orchestrator.py` "Verificação de Saúde" mostra todas tabelas ✅
