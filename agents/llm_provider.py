"""LLM provider wrapper — unified interface over LiteLLM.

Handles retry with exponential backoff, model fallback chain,
structured output (JSON mode), token/cost tracking, budget control,
and rate limit enforcement (tokens per minute).
Works transparently with OpenAI, Anthropic, Google, Ollama, and 100+ others.
"""

from __future__ import annotations

import json
import time
from collections import deque
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

# ─── Rate limiter ────────────────────────────────────────────────────────────

# Default: 10,000 tokens per minute
_DEFAULT_TPM_LIMIT = 10_000
_WINDOW_SECONDS = 60


class _TokenRateLimiter:
    """Sliding-window token-per-minute rate limiter.

    Tracks token usage within a 60-second rolling window and pauses
    execution when the limit would be exceeded.
    """

    def __init__(self, tpm_limit: int = _DEFAULT_TPM_LIMIT) -> None:
        self.tpm_limit = tpm_limit
        # deque of (timestamp, tokens_used)
        self._usage: deque[tuple[float, int]] = deque()

    def _purge_old(self) -> None:
        """Remove entries older than the window."""
        cutoff = time.monotonic() - _WINDOW_SECONDS
        while self._usage and self._usage[0][0] < cutoff:
            self._usage.popleft()

    @property
    def tokens_used_in_window(self) -> int:
        self._purge_old()
        return sum(t for _, t in self._usage)

    @property
    def tokens_remaining(self) -> int:
        return max(0, self.tpm_limit - self.tokens_used_in_window)

    def wait_if_needed(self, estimated_tokens: int = 0) -> None:
        """Block until there is room in the window for the estimated tokens.

        If current usage + estimated_tokens would exceed the limit,
        sleeps until enough old entries fall out of the window.
        """
        while True:
            self._purge_old()
            used = self.tokens_used_in_window
            if used + estimated_tokens <= self.tpm_limit:
                return

            # Calculate how long to wait for the oldest entry to expire
            if self._usage:
                oldest_ts = self._usage[0][0]
                wait_sec = (oldest_ts + _WINDOW_SECONDS) - time.monotonic() + 0.5
                wait_sec = max(0.1, wait_sec)
            else:
                wait_sec = 1.0

            logger.warning(
                "rate_limit_pausing",
                tokens_used=used,
                tpm_limit=self.tpm_limit,
                wait_seconds=round(wait_sec, 1),
                estimated_tokens=estimated_tokens,
            )
            time.sleep(wait_sec)

    def record(self, tokens: int) -> None:
        """Record token usage."""
        self._usage.append((time.monotonic(), tokens))


# Module-level singleton so all LLMProvider instances share the same limiter
_rate_limiter = _TokenRateLimiter(_DEFAULT_TPM_LIMIT)


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

        # Estimate tokens for rate limiting (rough: 4 chars ≈ 1 token)
        estimated_input = sum(len(m.get("content", "")) for m in messages) // 4
        _rate_limiter.wait_if_needed(estimated_input)

        chain = self.config.fallback_chain
        if model_override:
            from config.llm_config import MODELS

            spec = MODELS.get(model_override, ModelSpec(model=model_override))
            chain = [spec]

        last_error: Exception | None = None

        for spec in chain:
            try:
                return self._call_with_retry(messages, spec, json_mode)
            except RateLimitError as e:
                # Rate limit hit from the API — wait and retry with same model
                logger.warning(
                    "api_rate_limit_hit",
                    model=spec.model,
                    error=str(e),
                )
                _rate_limiter.wait_if_needed(estimated_input)
                try:
                    return self._call_with_retry(messages, spec, json_mode)
                except Exception as retry_err:
                    last_error = retry_err
                    continue
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

        try:
            response = litellm.completion(**params)
        except Exception as e:
            # Detect rate limit errors from the API
            err_str = str(e).lower()
            if "rate" in err_str and "limit" in err_str:
                raise RateLimitError(str(e)) from e
            if "429" in str(e):
                raise RateLimitError(str(e)) from e
            raise

        content = response.choices[0].message.content or ""
        usage = response.usage
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0
        total_tokens = input_tokens + output_tokens

        # Record tokens consumed for rate limiting
        _rate_limiter.record(total_tokens)

        cost = self.config.track_cost(input_tokens, output_tokens, spec.model)

        logger.info(
            "llm_call_complete",
            model=spec.model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=f"${cost:.6f}",
            total_cost=f"${self.config.accumulated_cost:.6f}",
            tpm_used=_rate_limiter.tokens_used_in_window,
            tpm_remaining=_rate_limiter.tokens_remaining,
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


class RateLimitError(Exception):
    """Raised when the API returns a rate limit (429) error."""
