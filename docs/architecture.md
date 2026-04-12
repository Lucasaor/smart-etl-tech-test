# Arquitetura

## Visão Geral

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

## Camada Bronze — Ingestão

- Validação de schema contra dicionário de dados (14 colunas esperadas)
- Parse da coluna `metadata` (JSON string) → 6 colunas tipadas
- Cast de `timestamp` string → `Datetime`
- Adição de `_ingested_at` e `_source_file` para rastreabilidade
- 3 modos: `full` (overwrite), `incremental` (append novos), `auto` (detecção automática)

## Camada Silver — Limpeza e Extração

- **Deduplicação** por status (sent+delivered → mantém delivered)
- **Extração de entidades** via regex: CPF, email, CEP, telefone, placa de veículo, concorrentes
- **Mascaramento de PII** com hash SHA-256 determinístico
- **Agregação por conversa** (~15k rows): contagens, tempos, entidades extraídas, outcome

## Camada Gold — Analytics

- **Sentimento**: score heurístico baseado em outcome (50%), engajamento (20%), tempo de resposta (15%), duração (15%)
- **Personas**: 5 tipos (Decidido, Pesquisador, Negociador, Fantasma, Indeciso) com scoring multi-dimensão
- **Segmentação**: 7 dimensões (engajamento, velocidade, veículo, região, origem, duração, qualificação)
- **Funil de conversão**: contagem e percentual por outcome
- **Lead scoring** (0-100): engajamento (30pts), tempo resposta (25pts), PII (20pts), profundidade (15pts), veículo (10pts)
- **Performance por campanha**: conversas, vendas, taxa de conversão, tempo de resposta
- **Métricas por vendedor**: conversão, ghosting, velocidade, score (0-100)

## Sistema de Agentes

- **CodeGen Agent**: analisa spec → gera código Python/Polars para cada camada do pipeline
- **Pipeline Agent** (LangGraph): state machine — `load_spec` → `analyze` → `plan` → `execute` (B/S/G) → `validate` → `complete`
- **Monitor Agent** (LangGraph): loop cíclico — detecta novos dados, verifica saúde, dispara pipeline automaticamente
- **Repair Agent** (LangGraph): diagnóstico e auto-correção — 5 estratégias (retry, skip, regenerate, data fix, escalate)

## Monitoramento

- `PipelineRun`: registro de cada execução com steps, duração, status
- `AgentAction`: registro de ações dos agentes com tokens/custo LLM
- `Alert`: alertas com severidade, fonte e estado de resolução
- Persistência via SQLite (local) ou Delta tables (Databricks)

## Estrutura do Projeto

```
.
├── pyproject.toml                 # Definição do projeto e dependências
├── config/                        # Configuração (Pydantic Settings, LLM fallback)
├── core/                          # Infraestrutura (Storage, Compute, Events)
├── pipeline/                      # Transformações de dados
│   ├── bronze/                    # Ingestão de dados brutos → Delta
│   ├── silver/                    # Limpeza, extração e agregação
│   └── gold/                      # Analytics, personas, sentimento, segmentação
├── agents/                        # Agentes de IA (CodeGen, Pipeline, Monitor, Repair)
│   └── tools/                     # Ferramentas dos agentes
├── monitoring/                    # Monitoramento (SQLite / Delta)
├── frontend/                      # Dashboard Streamlit
│   └── pages/                     # Páginas do dashboard
├── tests/                         # 224 testes automatizados
├── docker/                        # Containerização
└── databricks/                    # Integração Databricks
    └── notebooks/                 # Notebooks para execução no Databricks
```
