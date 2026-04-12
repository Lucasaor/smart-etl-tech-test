# =============================================================================
# Agentic Pipeline — Dockerfile
# =============================================================================
# Imagem base para execução do pipeline agêntico com Streamlit frontend.
# Suporta execução dos agentes, pipeline de dados e interface web.
#
# Build:   docker build -f docker/Dockerfile -t agentic-pipeline .
# Run:     docker run -p 8501:8501 agentic-pipeline
# =============================================================================

FROM python:3.11-slim AS base

# Evitar interações durante instalação
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# Dependências de sistema mínimas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ─── Instalar dependências Python ─────────────────────────────────────────────

COPY pyproject.toml README.md ./
RUN pip install --upgrade pip && \
    mkdir -p config core pipeline agents monitoring && \
    touch config/__init__.py core/__init__.py pipeline/__init__.py agents/__init__.py monitoring/__init__.py && \
    pip install . && \
    rm -rf config core pipeline agents monitoring

# ─── Copiar código-fonte ──────────────────────────────────────────────────────

COPY config/ config/
COPY core/ core/
COPY pipeline/ pipeline/
COPY agents/ agents/
COPY monitoring/ monitoring/
COPY frontend/ frontend/
COPY logo_dark.png logo_dark.png
COPY .env.example .env.example

# ─── Volume para dados persistentes ──────────────────────────────────────────

VOLUME /app/data

# ─── Porta do Streamlit ──────────────────────────────────────────────────────

EXPOSE 8501

# ─── Health check ─────────────────────────────────────────────────────────────

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# ─── Ponto de entrada ─────────────────────────────────────────────────────────

CMD ["streamlit", "run", "frontend/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
