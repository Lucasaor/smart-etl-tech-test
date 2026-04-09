"""LLM provider wrapper — unified interface over LiteLLM.

Handles retry with exponential backoff, model fallback chain,
structured output (JSON mode), token/cost tracking, and budget control.
Works transparently with OpenAI, Anthropic, Google, Ollama, and 100+ others.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.llm_config import LLMConfig, ModelSpec, get_llm_config
from config.settings import get_settings

logger = structlog.get_logger(__name__)


@dataclass
class LLMResponse:
    """Structured response from an LLM call."""

    content: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost: float = 0.0
    raw_response: Any = None

    def as_json(self) -> dict[str, Any] | list[Any]:
        """Parse content as JSON. Raises ValueError on failure."""
        return json.loads(self.content)


class LLMProvider:
    """Unified LLM provider with fallback chain and cost tracking."""

    def __init__(self, config: LLMConfig | None = None) -> None:
        self.config = config or get_llm_config()
        self._settings = get_settings()

    def _get_api_params(self, model_spec: ModelSpec) -> dict[str, Any]:
        """Build LiteLLM kwargs for the given model."""
        params: dict[str, Any] = {
            "model": model_spec.model,
            "max_tokens": model_spec.max_tokens,
            "temperature": model_spec.temperature,
        }

        # Ollama-specific: set api_base
        if model_spec.model.startswith("ollama/"):
            params["api_base"] = self._settings.ollama_base_url

        return params

    def complete(
        self,
        messages: list[dict[str, str]],
        json_mode: bool = False,
        model_override: str | None = None,
    ) -> LLMResponse:
        """Send a completion request with automatic fallback.

        Args:
            messages: Chat messages in OpenAI format [{"role": ..., "content": ...}].
            json_mode: If True, request JSON-formatted output.
            model_override: Force a specific model, bypassing fallback chain.
        """
        if self.config.budget_exceeded:
            raise BudgetExceededError(
                f"LLM budget exceeded: ${self.config.accumulated_cost:.4f} "
                f">= ${self.config.max_cost_per_run:.2f}"
            )

        chain = self.config.fallback_chain
        if model_override:
            from config.llm_config import MODELS

            spec = MODELS.get(model_override, ModelSpec(model=model_override))
            chain = [spec]

        last_error: Exception | None = None

        for spec in chain:
            try:
                return self._call_with_retry(messages, spec, json_mode)
            except Exception as e:
                logger.warning(
                    "llm_call_failed",
                    model=spec.model,
                    error=str(e),
                )
                last_error = e
                continue

        raise LLMProviderError(
            f"All models in fallback chain failed. Last error: {last_error}"
        )

    @retry(
        retry=retry_if_exception_type((TimeoutError, ConnectionError, OSError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _call_with_retry(
        self,
        messages: list[dict[str, str]],
        spec: ModelSpec,
        json_mode: bool,
    ) -> LLMResponse:
        """Single model call with retry on transient errors."""
        import litellm

        params = self._get_api_params(spec)
        params["messages"] = messages

        if json_mode and spec.supports_json_mode:
            params["response_format"] = {"type": "json_object"}

        logger.info("llm_call_start", model=spec.model, json_mode=json_mode)

        response = litellm.completion(**params)

        content = response.choices[0].message.content or ""
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0

        cost = self.config.track_cost(input_tokens, output_tokens, spec.model)

        logger.info(
            "llm_call_complete",
            model=spec.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=f"${cost:.6f}",
            total_cost=f"${self.config.accumulated_cost:.6f}",
        )

        return LLMResponse(
            content=content,
            model=spec.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=cost,
            raw_response=response,
        )

    def complete_json(
        self,
        messages: list[dict[str, str]],
        model_override: str | None = None,
    ) -> dict[str, Any] | list[Any]:
        """Convenience: complete and parse as JSON."""
        resp = self.complete(messages, json_mode=True, model_override=model_override)
        return resp.as_json()

    @property
    def accumulated_cost(self) -> float:
        return self.config.accumulated_cost

    @property
    def budget_remaining(self) -> float:
        return self.config.budget_remaining


class LLMProviderError(Exception):
    """Raised when all models in the fallback chain fail."""


class BudgetExceededError(Exception):
    """Raised when the LLM cost budget is exceeded."""
