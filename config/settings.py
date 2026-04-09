"""Application settings managed via Pydantic Settings.

Reads from environment variables and .env file. Provides all
configuration needed across local (Docker) and Databricks runtimes.
"""

from __future__ import annotations

import os
from enum import Enum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeEnv(str, Enum):
    LOCAL = "local"
    DATABRICKS = "databricks"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- Runtime ---
    runtime_env: RuntimeEnv = RuntimeEnv.LOCAL

    # --- Data Paths ---
    data_root: str = "./data"
    bronze_source_path: str = "./conversations_bronze.parquet"
    spec_dir: str = ""  # Diretório das especificações do projeto; padrão: {data_root}/specs

    # --- LLM ---
    llm_model: str = "gpt-4o-mini"
    llm_fallback_model: str = "ollama/llama3"
    llm_temperature: float = 0.1
    llm_max_cost_per_run: float = 5.0
    llm_batch_size: int = 15

    # API keys (optional — set only the provider you use)
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    google_api_key: str = ""

    # Ollama
    ollama_base_url: str = "http://localhost:11434"

    # --- Pipeline ---
    monitor_poll_interval: int = 60

    # --- Frontend ---
    streamlit_port: int = 8501

    # --- Derived Paths ---
    @computed_field  # type: ignore[prop-decorator]
    @property
    def spec_path(self) -> str:
        if self.spec_dir:
            return self.spec_dir
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return "/mnt/delta/specs"
        return str(Path(self.data_root) / "specs")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def bronze_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return f"/mnt/delta/bronze"
        return str(Path(self.data_root) / "bronze")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def silver_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return f"/mnt/delta/silver"
        return str(Path(self.data_root) / "silver")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def silver_messages_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return "/mnt/delta/silver/messages"
        return str(Path(self.data_root) / "silver" / "messages")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def silver_conversations_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return "/mnt/delta/silver/conversations"
        return str(Path(self.data_root) / "silver" / "conversations")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def gold_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return f"/mnt/delta/gold"
        return str(Path(self.data_root) / "gold")

    @computed_field  # type: ignore[prop-decorator]
    @property
    def monitoring_db_path(self) -> str:
        if self.runtime_env == RuntimeEnv.DATABRICKS:
            return f"/mnt/delta/monitoring"
        return str(Path(self.data_root) / "monitoring" / "pipeline.db")

    @property
    def is_local(self) -> bool:
        return self.runtime_env == RuntimeEnv.LOCAL

    @property
    def is_databricks(self) -> bool:
        return self.runtime_env == RuntimeEnv.DATABRICKS


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton factory for Settings — cached after first call."""
    return Settings()
