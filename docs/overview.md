# Visão Geral

O **Agentic Pipeline** é um sistema que recebe como entrada uma amostra de dados brutos, um dicionário de dados e uma descrição dos KPIs desejados. A partir dessas entradas, agentes de IA autônomos **implementam os scripts do pipeline**, gerenciam a execução e auto-corrigem falhas — tudo como infraestrutura persistente.

## Fluxo Principal

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

## Stack Tecnológico

| Componente | Tecnologia | Justificativa |
|---|---|---|
| LLM Abstraction | **LiteLLM** | API unificada para 100+ providers (OpenAI, Anthropic, Google, Ollama, etc.) |
| Agent Framework | **LangGraph** | State machine robusto com suporte a auto-correção e fallback |
| Storage | **Delta Lake** | Formato portável local↔Databricks; ACID, time travel, schema enforcement |
| Compute (local) | **Polars + DuckDB** | Rápido para ~150k rows; adapter pattern para PySpark no Databricks |
| Frontend | **Streamlit** | Python nativo, interface de entrada de dados |
| Monitoring | **SQLite** (local) / **Delta tables** (Databricks) | Logs de pipeline runs e ações dos agentes |

## Entradas do Sistema

O pipeline é orientado por **três entradas obrigatórias**:

| Entrada | Formato | Propósito |
|---------|---------|-----------|
| **Amostra de dados brutos** | Parquet ou CSV | Dados que serão processados na camada Bronze |
| **Dicionário de dados** | Markdown | Descreve colunas, tipos, valores esperados e particularidades |
| **Descrição dos KPIs** | Markdown | Define os indicadores e análises desejados na camada Gold |

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
