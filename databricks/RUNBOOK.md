# Databricks Free Edition — Runbook Operacional

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
│          DATABRICKS (Free Edition — Serverless)         │
│                                                         │
│  ┌────────────────────────────────────────────────────┐ │
│  │  05_agentic_pipeline.py (RECOMMENDED)              │ │
│  │  ┌───────────┐  ┌──────────┐  ┌────────────────┐   │ │
│  │  │ CodeGen   │→ │ Executor │→ │ Delta Tables   │   │ │
│  │  │ Agent     │  │ (Polars) │  │ (UC Volumes)   │   │ │
│  │  │ (LiteLLM) │  │          │  │                │   │ │
│  │  └───────────┘  └──────────┘  └────────────────┘   │ │
│  │  RUNTIME_ENV=databricks | DATA_ROOT=/Volumes/.../data │ │
│  │  DatabricksBackend (PySpark) → Delta on UC Volumes  │ │
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
│  │  Unity Catalog Volumes                             │ │
│  │  /Volumes/main/default/pipeline_data/              │ │
│  │  ├── specs/     ├── bronze/   ├── silver/          │ │
│  │  ├── gold/      └── monitoring/                    │ │
│  └────────────────────────────────────────────────────┘ │
│                                                         │
│  Compute: Serverless (auto-provisioned, no management)  │
│  Storage: Unity Catalog Volumes (POSIX paths)           │
│  Governance: Unity Catalog built-in                     │
└─────────────────────────────────────────────────────────┘
```

## Pré-requisitos

1. **Conta Databricks Free Edition** ativa em [accounts.cloud.databricks.com](https://accounts.cloud.databricks.com)
2. **Databricks SDK** configurado localmente:
   ```bash
   pip install databricks-sdk
   ```
3. **Variáveis de ambiente**:
   - `DATABRICKS_HOST` — URL do workspace (ex: `https://your-workspace.cloud.databricks.com`)
   - `DATABRICKS_TOKEN` — Token de acesso pessoal

> **Nota:** Não é necessário criar ou gerenciar clusters. A Free Edition usa compute serverless
> que é provisionado automaticamente quando você executa notebooks ou jobs.

---

## Setup Inicial

### 1. Preparar UC Volumes

```bash
# Criar volume + diretórios + upload de dados e specs
python databricks/setup_volumes.py

# Incluir artefatos gerados (se já existirem)
python databricks/setup_volumes.py --include-generated

# Usando perfil específico do CLI
python databricks/setup_volumes.py --profile DEFAULT

# Customizar catálogo/schema/volume
python databricks/setup_volumes.py --catalog main --schema default --volume pipeline_data
```

### 2. Importar Repo no Workspace

Opção A — **Repos** (recomendado):
1. Workspace → Repos → Add Repo
2. URL: `https://github.com/Lucasaor/smart-etl-tech-test`
3. O repo estará em `/Workspace/Repos/Lucasaor/smart-etl-tech-test`

Opção B — **Upload manual de notebooks**:
1. Workspace → Import
2. Importar `databricks/notebooks/*.py` como notebooks Python

### 3. Configurar LLM API Key

Na Free Edition, **não há variáveis de ambiente de cluster**. Use uma das opções:

**Opção A — Push automático do `.env` (recomendado):**

O script `push_secrets.py` lê o arquivo `.env` local, muda `RUNTIME_ENV` para `"databricks"`,
e envia todas as variáveis como Databricks Secrets:

```bash
# Preview do que será enviado (sem alterar nada)
python databricks/push_secrets.py --dry-run

# Enviar secrets para o scope "pipeline"
python databricks/push_secrets.py

# Scope customizado
python databricks/push_secrets.py --scope meu_scope

# Incluir variáveis com valor vazio
python databricks/push_secrets.py --include-empty
```

Para carregar os secrets nos notebooks:
```python
# Carregar todas as configs do pipeline
import os
scope = "pipeline"
for s in dbutils.secrets.list(scope):
    os.environ[s.key] = dbutils.secrets.get(scope, s.key)
```

> **Nota:** `DATABRICKS_HOST` e `DATABRICKS_TOKEN` são automaticamente excluídos
> (já estão configurados no ambiente). Variáveis com valor vazio são ignoradas por padrão.

**Opção B — Secrets manuais (via CLI):**
```bash
# Criar secret scope
databricks secrets create-scope pipeline

# Adicionar chaves individualmente
databricks secrets put-secret pipeline ANTHROPIC_API_KEY --string-value "sk-..."
databricks secrets put-secret pipeline LLM_MODEL --string-value "anthropic/claude-haiku-4-5"
databricks secrets put-secret pipeline RUNTIME_ENV --string-value "databricks"
```

**Opção C — Widgets interativos:**
```python
dbutils.widgets.text("llm_api_key", "", "LLM API Key")
os.environ["OPENAI_API_KEY"] = dbutils.widgets.get("llm_api_key")
```

**Opção D — Diretamente no código (⚠️ não commitar):**
```python
os.environ["OPENAI_API_KEY"] = "sk-..."
```

---

## Execução do Pipeline

### Pipeline Agêntico — `05_agentic_pipeline.py` (RECOMENDADO)

O notebook `05_agentic_pipeline.py` executa o **pipeline agêntico completo** no Databricks,
replicando exatamente o que o Streamlit faz localmente:

1. Carrega os **mesmos 3 inputs** (parquet, dicionário de dados, descrição KPIs)
2. Analisa os dados e cria um `ProjectSpec`
3. Usa o `CodeGenAgent` (LLM) para gerar código Python/Polars dinamicamente
4. Executa o código gerado → escreve Delta tables em UC Volumes
5. Valida e exibe resultados

**Como funciona:**

```
RUNTIME_ENV=databricks + DATA_ROOT=/Volumes/<catalog>/default/pipeline_data + FORCE_DELTA_MONITORING=true

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
                                          │ UC Volumes    │
                                          │ /Volumes/     │
                                          │   main/       │
                                          │   default/    │
                                          │   pipeline_   │
                                          │   data/       │
                                          │ ├── bronze/   │
                                          │ ├── silver/   │
                                          │ └── gold/     │
                                          └───────────────┘
```

- Usa `DatabricksBackend` (PySpark) para escrever Delta tables em UC Volumes
- Não usa `deltalake` library para escrita (UC Volumes não suporta `rename()` atômico)
- Tabelas Delta criadas são acessíveis via PySpark: `spark.read.format("delta").load("/Volumes/main/default/pipeline_data/bronze")`
- Monitoring usa Delta tables (não SQLite) via `FORCE_DELTA_MONITORING=true`

**Setup:**

1. **LLM API key** — via Secrets ou widgets (veja seção acima)
2. **Upload dos 3 arquivos de input** para UC Volume (instruções no notebook)
3. **Executar o notebook** célula por célula ou Run All

**Re-execução:**

| Cenário | Ação |
|---------|------|
| Regenerar código | Deletar `/Volumes/.../specs/generated/pipeline_meta.json` e re-executar do Cell 5 |
| Re-executar com código existente | Re-executar do Cell 6 |
| Novos dados | Substituir arquivos em `/Volumes/.../specs/` e re-executar do Cell 4 |
| Cleanup total | `dbutils.fs.rm("/Volumes/main/default/pipeline_data/", recurse=True)` |

### Pipeline PySpark — notebooks 01-04

Os notebooks 01-04 são uma alternativa PySpark pré-codificada. Útil quando:
- Não há API key de LLM disponível
- Para debug de camadas individuais

**Execução Completa:**

1. Abrir `04_agent_orchestrator.py` no workspace
2. **Run All** — compute serverless será provisionado automaticamente
3. Executa Bronze → Silver → Gold em sequência
4. Verificar resumo na célula de resultados

**Execução por Camada (debug/reprocessamento):**

```
01_bronze.py  → Ingestão de Parquet bruto → Delta Bronze
02_silver.py  → Limpeza + Extração + Agregação
03_gold.py    → Analytics, Sentimento, Personas, Segmentação, Vendedores
```

Cada notebook emite um resultado estruturado JSON via `dbutils.notebook.exit()`.

---

## Características da Free Edition

| Característica | Detalhe |
|----------------|---------|
| **Compute** | Serverless (auto-provisionado, sem gerenciamento) |
| **Storage** | Unity Catalog Volumes (paths POSIX: `/Volumes/...`) |
| **Governance** | Unity Catalog built-in (catálogo `main`, schema `default`) |
| **Jobs** | Até 5 Jobs concorrentes |
| **SQL Warehouse** | 1 SQL warehouse (tamanho limitado) |
| **Notebooks** | Python e SQL |
| **Bibliotecas** | `%pip install` para instalar pacotes notebook-scoped |

### Restrições da Free Edition

| Restrição | Impacto | Mitigação |
|-----------|---------|-----------|
| **Sem clusters custom** | Não é possível definir recursos de compute | Serverless é suficiente para este pipeline |
| **Sem DBFS direto** | DBFS não acessível; usar Volumes | Todos os paths apontam para `/Volumes/...` |
| **Sem Spark RDD API** | Apenas Spark Connect (DataFrame API) | Pipeline já usa apenas DataFrame API |
| **Sem df.cache()/persist()** | Caching não disponível | Datasets são pequenos (~150k rows), sem impacto |
| **Sem env vars de cluster** | Não há where definir variáveis de ambiente gerais | Usar Secrets ou widgets |
| **Internet restrita** | Outbound limitado a domínios confiáveis | Pip install funciona; LLM APIs devem ser acessíveis |
| **Sem R ou Scala** | Apenas Python e SQL | Pipeline já é 100% Python |
| **UDFs: limite 1GB** | UDFs Python têm limite de memória | UDFs do pipeline são leves (regex, heurísticas) |

---

## Troubleshooting

### Erro "ModuleNotFoundError: No module named 'delta'"

**Contexto**: Na Free Edition (serverless), o pacote `delta` (delta-spark) pode não estar disponível
da mesma forma que em clusters tradicionais.

**Solução**: Os notebooks 01-04 **não** importam `from delta.tables import DeltaTable`.
Toda verificação de tabelas é feita via `spark.read.format("delta")` ou SQL `DESCRIBE HISTORY`.

### Erro de path "/Volumes/..."

**Sintoma**: `FileNotFoundException` ao acessar paths de Volume

**Solução**:
1. Verificar se `setup_volumes.py` foi executado com sucesso
2. Verificar se dados de entrada existem:
   ```python
   dbutils.fs.ls("/Volumes/main/default/pipeline_data/specs/")
   ```
3. Verificar se o volume existe:
   ```python
   spark.sql("SHOW VOLUMES IN main.default").show()
   ```
4. Se necessário, criar o volume manualmente:
   ```sql
   CREATE VOLUME IF NOT EXISTS main.default.pipeline_data
   ```

### Erro de import "config.*"

**Sintoma**: `ModuleNotFoundError: No module named 'config'`

**Solução**:
1. Verificar se o repo está importado no workspace (Repos)
2. Na célula de bootstrap do notebook, verificar se `repo_path` está correto
3. Definir manualmente se necessário:
   ```python
   import sys
   sys.path.insert(0, "/Workspace/Repos/<seu-usuario>/smart-etl-tech-test")
   ```

### LLM API key não funciona

**Sintoma**: Erro de autenticação ao chamar LLM no notebook 05

**Solução**:
1. **Secrets**: Verifique se o scope e a key existem:
   ```python
   dbutils.secrets.list("llm")
   ```
2. **Outbound**: Verifique se a API do LLM é acessível (Free Edition tem internet restrita):
   ```python
   import urllib.request
   urllib.request.urlopen("https://api.openai.com/v1/models", timeout=5)
   ```
3. **Alternativa**: Use modelos que estejam em domínios permitidos, ou tente Anthropic/Google.

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
   df = spark.read.format("delta").load("/Volumes/main/default/pipeline_data/bronze")
   print(f"Databricks Bronze: {df.count()} rows")
   ```
3. Comparar row counts e schemas

---

## Monitoramento

### No Databricks
- Cada notebook salva metadados de execução em `/Volumes/.../monitoring/notebook_runs`
- O orquestrador salva resumo do pipeline completo
- Verificar via:
  ```python
  df = spark.read.format("delta").load("/Volumes/main/default/pipeline_data/monitoring/notebook_runs")
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
2. Sincronizar artefatos para UC Volumes:
   ```bash
   python databricks/setup_volumes.py --include-generated
   ```
3. Executar notebooks no Databricks (serverless)
4. Resultados ficam em Delta nos Volumes

### Cenário C: Fallback quando Databricks indisponível
- Pipeline local funciona independentemente
- Sem dependência do Databricks para desenvolvimento ou testes
- Todos os testes (`pytest`) rodam localmente sem cluster

---

## Agendamento via Jobs

Na Free Edition, você pode agendar notebooks como Jobs (até 5 concorrentes):

1. **Workflows** → **Create Job**
2. Selecionar notebook (ex: `04_agent_orchestrator.py`)
3. Definir schedule (ex: diário às 08:00)
4. Compute: Serverless (automático)

Isso substitui a limitação do Community Edition que não tinha Workflows/Jobs.

---

## Checklist de Validação Pós-Setup

### Pipeline Agêntico (05_agentic_pipeline.py)
- [ ] Repo importado e visível no workspace
- [ ] LLM API key configurada (secrets ou widgets)
- [ ] 3 input files presentes em `/Volumes/main/default/pipeline_data/specs/`
- [ ] `%pip install` executou sem erros
- [ ] `CodeGenAgent` gerou código para Bronze, Silver e Gold modules
- [ ] Pipeline executou com sucesso (todos steps "completed")
- [ ] Bronze Delta table contém dados
- [ ] Silver tables contêm dados
- [ ] Gold tables contêm dados

### Pipeline PySpark (01-04, fallback)
- [ ] `python databricks/setup_volumes.py` executou sem erros
- [ ] `01_bronze.py` executa e mostra row count > 0
- [ ] `02_silver.py` executa e cria Silver Messages + Conversations
- [ ] `03_gold.py` executa e cria 5 tabelas Gold
- [ ] `04_agent_orchestrator.py` "Verificação de Saúde" mostra todas tabelas ✅
