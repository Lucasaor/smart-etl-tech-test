# Databricks

O pipeline roda na **Databricks Free Edition** usando compute **serverless** e **Unity Catalog Volumes**.

## Pré-requisitos

- Conta [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Python 3.11+ local (para os scripts de setup)
- Uma API key de LLM

## Setup

**1. Instalar o SDK do Databricks:**

```bash
pip install databricks-sdk
```

**2. Configurar credenciais:**

```bash
export DATABRICKS_HOST=https://seu-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

**3. Upload dos dados para UC Volumes:**

```bash
python databricks/setup_volumes.py
# Incluir código gerado (se existir):
python databricks/setup_volumes.py --include-generated
```

**4. Enviar API keys via Databricks Secrets:**

```bash
python databricks/push_secrets.py
# Prévia sem enviar:
python databricks/push_secrets.py --dry-run
```

**5. Importar notebooks no Databricks:**

1. **Workspace** → **Repos** → **Add Repo**
2. Cole a URL: `https://github.com/Lucasaor/smart-etl-tech-test.git`
3. Os notebooks ficam em `databricks/notebooks/`

## Notebooks Disponíveis

| Notebook | Camada | Descrição |
|----------|--------|-----------|
| `01_bronze.py` | Bronze | Ingestão com PySpark, validação de schema, Delta incremental |
| `02_silver.py` | Silver | Dedup, extração de entidades (UDFs), agregação por conversa |
| `03_gold.py` | Gold | Sentimento, personas, segmentação, lead scoring, vendedores |
| `04_agent_orchestrator.py` | Orquestração | Execução sequencial com verificação de saúde |
| `05_agentic_pipeline.py` | **Agêntico** | Pipeline completo LLM-driven (recomendado) |

## Arquitetura Serverless

- **Compute**: serverless (provisionado automaticamente)
- **Storage**: Unity Catalog Volumes (`/Volumes/<catalog>/default/pipeline_data/`)
- **Governance**: Unity Catalog built-in
- **Jobs**: até 5 Jobs agendáveis
- **LLM API keys**: via Databricks Secrets ou notebook widgets

Para mais detalhes, consulte o [RUNBOOK](https://github.com/Lucasaor/smart-etl-tech-test/blob/main/databricks/RUNBOOK.md).
