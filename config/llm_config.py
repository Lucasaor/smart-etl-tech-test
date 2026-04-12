"""LLM model configuration with fallback chain and cost tracking.

Defines which models to use, in which order, and spending limits.
All models are addressed via LiteLLM model strings so they work
across OpenAI, Anthropic, Google, Ollama, and any other provider.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from config.settings import get_settings


@dataclass(frozen=True)
class ModelSpec:
    """Specification for a single LLM model."""

    model: str  # LiteLLM model string, e.g. "gpt-4o-mini", "ollama/llama3"
    max_tokens: int = 4096
    temperature: float = 0.1
    supports_json_mode: bool = True
    cost_per_1k_input: float = 0.0  # USD — 0 means unknown/free
    cost_per_1k_output: float = 0.0


# --- Well-known model specs ---

MODELS: dict[str, ModelSpec] = {
    "gpt-4o-mini": ModelSpec(
        model="gpt-4o-mini",
        cost_per_1k_input=0.00015,
        cost_per_1k_output=0.0006,
    ),
    "gpt-4o": ModelSpec(
        model="gpt-4o",
        cost_per_1k_input=0.0025,
        cost_per_1k_output=0.01,
    ),
    "anthropic/claude-3-haiku-20240307": ModelSpec(
        model="anthropic/claude-3-haiku-20240307",
        cost_per_1k_input=0.00025,
        cost_per_1k_output=0.00125,
    ),
    "anthropic/claude-sonnet-4-20250514": ModelSpec(
        model="anthropic/claude-sonnet-4-20250514",
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
    ),
    "gemini/gemini-1.5-flash": ModelSpec(
        model="gemini/gemini-1.5-flash",
        cost_per_1k_input=0.000075,
        cost_per_1k_output=0.0003,
    ),
    # Ollama models (local — zero cost)
    "ollama/llama3": ModelSpec(
        model="ollama/llama3",
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        supports_json_mode=True,
    ),
    "ollama/mistral": ModelSpec(
        model="ollama/mistral",
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        supports_json_mode=True,
    ),
    # Claude 4.x models
    "anthropic/claude-haiku-4-5": ModelSpec(
        model="anthropic/claude-haiku-4-5",
        max_tokens=8192,
        cost_per_1k_input=0.0008,
        cost_per_1k_output=0.004,
    ),
    "anthropic/claude-opus-4-6": ModelSpec(
        model="anthropic/claude-opus-4-6",
        max_tokens=8192,
        cost_per_1k_input=0.015,
        cost_per_1k_output=0.075,
    ),
    "anthropic/claude-sonnet-4-6": ModelSpec(
        model="anthropic/claude-sonnet-4-6",
        max_tokens=8192,
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
    ),
}


@dataclass
class LLMConfig:
    """Runtime LLM configuration with ordered fallback chain."""

    primary: ModelSpec
    fallbacks: list[ModelSpec] = field(default_factory=list)
    max_cost_per_run: float = 5.0
    accumulated_cost: float = 0.0

    @property
    def fallback_chain(self) -> list[ModelSpec]:
        """Primary + fallbacks in order."""
        return [self.primary, *self.fallbacks]

    def track_cost(self, input_tokens: int, output_tokens: int, model: str) -> float:
        """Track cost for a completion call. Returns cost of this call."""
        spec = MODELS.get(model)
        # Also check the specs in our own fallback chain (for custom/unknown models)
        if spec is None:
            for s in self.fallback_chain:
                if s.model == model:
                    spec = s
                    break
        if spec is None:
            return 0.0
        cost = (input_tokens / 1000) * spec.cost_per_1k_input + (
            output_tokens / 1000
        ) * spec.cost_per_1k_output
        self.accumulated_cost += cost
        return cost

    @property
    def budget_remaining(self) -> float:
        return max(0.0, self.max_cost_per_run - self.accumulated_cost)

    @property
    def budget_exceeded(self) -> bool:
        return self.accumulated_cost >= self.max_cost_per_run


def _resolve_model(model_str: str) -> ModelSpec:
    """Resolve a model string to a ModelSpec."""
    if model_str in MODELS:
        return MODELS[model_str]
    settings = get_settings()
    return ModelSpec(model=model_str, temperature=settings.llm_temperature)


def get_llm_config() -> LLMConfig:
    """Build default LLMConfig from application settings."""
    settings = get_settings()

    primary = _resolve_model(settings.llm_model)
    fallbacks = [_resolve_model(settings.llm_fallback_model)] if settings.llm_fallback_model else []

    return LLMConfig(
        primary=primary,
        fallbacks=fallbacks,
        max_cost_per_run=settings.llm_max_cost_per_run,
    )


def get_llm_config_for_role(role: str) -> LLMConfig:
    """Build a role-specific LLMConfig.

    Roles:
      - 'codegen': uses llm_codegen_model (falls back to llm_model)
      - 'repair':  uses llm_repair_model  (falls back to llm_model)
      - anything else: uses default llm_model
    """
    settings = get_settings()

    model_map = {
        "codegen": settings.llm_codegen_model,
        "repair": settings.llm_repair_model,
    }

    role_model = model_map.get(role, "") or settings.llm_model
    primary = _resolve_model(role_model)

    # Fallback chain: role-specific primary → default primary → fallback
    fallbacks: list[ModelSpec] = []
    if role_model != settings.llm_model:
        fallbacks.append(_resolve_model(settings.llm_model))
    if settings.llm_fallback_model:
        fb = _resolve_model(settings.llm_fallback_model)
        if fb.model != primary.model:
            fallbacks.append(fb)

    return LLMConfig(
        primary=primary,
        fallbacks=fallbacks,
        max_cost_per_run=settings.llm_max_cost_per_run,
    )
