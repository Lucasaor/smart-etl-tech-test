# Início Rápido

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
# OpenAI
LLM_MODEL=gpt-4o-mini
OPENAI_API_KEY=sk-...

# OU Anthropic
LLM_MODEL=anthropic/claude-3-haiku-20240307
ANTHROPIC_API_KEY=sk-ant-...

# OU Google
LLM_MODEL=gemini/gemini-1.5-flash
GOOGLE_API_KEY=...

# OU Ollama (sem API key)
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
python3 -m venv .venv
source .venv/bin/activate  # Linux/macOS
pip install -e ".[dev]"
```

**2. Configurar variáveis de ambiente:**

```bash
cp .env.example .env
# Editar .env com suas API keys
```

**3. Executar os testes (opcional):**

```bash
pytest tests/ -v
```

**4. Iniciar a interface Streamlit:**

```bash
streamlit run frontend/app.py
```

---

## Opção 3: Databricks Free Edition

Consulte a seção {doc}`databricks` para o guia completo de configuração no Databricks.

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
