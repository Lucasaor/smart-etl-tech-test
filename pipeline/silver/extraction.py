"""Silver Layer - PII Extraction and Masking

Extracts structured entities from message bodies using regex patterns.
Masks sensitive PII with deterministic SHA-256 hashing to preserve
analytical utility while protecting individual privacy.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Any

import polars as pl

from core.storage import get_storage_backend

logger = logging.getLogger(__name__)

# ─── Regex patterns ───────────────────────────────────────────────────────────

CPF_RE = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")
EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[a-z]{2,}\b", re.IGNORECASE)
CEP_RE = re.compile(r"\b\d{5}-?\d{3}\b")
# Mercosul plate: 3 letters + digit + letter-or-digit + 2 digits (e.g. SYL8V26)
PLATE_RE = re.compile(r"\b[A-Z]{3}\d[A-Z0-9]\d{2}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}\b")

COMPETITORS = [
    "porto seguro",
    "bradesco seguros",
    "bradesco seguro",
    "itaú seguros",
    "itau seguros",
    "allianz",
    "hdi seguros",
    "hdi seguro",
    "mapfre",
    "tokio marine",
    "suhai",
    "azul seguros",
    "azul seguro",
]
COMPETITOR_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(c) for c in COMPETITORS) + r")\b",
    re.IGNORECASE,
)

_EXTRACTION_SCHEMA: dict[str, type] = {
    "extracted_cpf_hash": pl.Utf8,
    "extracted_email_hash": pl.Utf8,
    "extracted_cep": pl.Utf8,
    "extracted_plate": pl.Utf8,
    "extracted_competitor": pl.Utf8,
    "pii_cpf_found": pl.Boolean,
    "pii_email_found": pl.Boolean,
    "pii_cep_found": pl.Boolean,
    "pii_phone_found": pl.Boolean,
}


def _sha256_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _extract_from_body(body: str | None) -> dict[str, Any]:
    """Extract all structured entities from a single message body string."""
    result: dict[str, Any] = {
        "extracted_cpf_hash": None,
        "extracted_email_hash": None,
        "extracted_cep": None,
        "extracted_plate": None,
        "extracted_competitor": None,
        "pii_cpf_found": False,
        "pii_email_found": False,
        "pii_cep_found": False,
        "pii_phone_found": False,
    }
    if not body:
        return result

    if m := CPF_RE.search(body):
        result["extracted_cpf_hash"] = _sha256_hash(m.group(0))
        result["pii_cpf_found"] = True

    if m := EMAIL_RE.search(body):
        result["extracted_email_hash"] = _sha256_hash(m.group(0).lower())
        result["pii_email_found"] = True

    if m := CEP_RE.search(body):
        result["extracted_cep"] = re.sub(r"[^\d]", "", m.group(0))
        result["pii_cep_found"] = True

    if m := PLATE_RE.search(body):
        result["extracted_plate"] = re.sub(r"[- ]", "", m.group(0)).upper()

    if m := COMPETITOR_RE.search(body):
        result["extracted_competitor"] = m.group(0).lower()

    if PHONE_RE.search(body):
        result["pii_phone_found"] = True

    return result


def mask_pii_in_body(body: str | None) -> str | None:
    """Replace PII tokens in a message body with placeholder tags."""
    if not body:
        return body
    body = CPF_RE.sub("[CPF_MASKED]", body)
    body = EMAIL_RE.sub("[EMAIL_MASKED]", body)
    body = CEP_RE.sub("[CEP_MASKED]", body)
    body = PHONE_RE.sub("[PHONE_MASKED]", body)
    return body


def extract_entities(silver_messages_path: str | Path) -> dict[str, Any]:
    """Read Silver messages, extract entities, mask PII, overwrite table.

    Args:
        silver_messages_path: Path to the Silver messages Delta table (in-place update).

    Returns:
        Stats dict with extraction counts.
    """
    silver_messages_path = str(silver_messages_path)
    storage = get_storage_backend()

    logger.info(
        "reading_silver_messages_for_extraction",
        extra={"path": silver_messages_path},
    )
    df = storage.read_table(silver_messages_path)
    bodies = df["message_body_normalized"].to_list()

    extracted = [_extract_from_body(b) for b in bodies]
    extraction_df = pl.DataFrame(extracted, schema=_EXTRACTION_SCHEMA)

    masked_bodies = [mask_pii_in_body(b) for b in bodies]
    df = df.with_columns(
        pl.Series("message_body_masked", masked_bodies, dtype=pl.Utf8)
    )
    df = pl.concat([df, extraction_df], how="horizontal")

    storage.write_table(df, silver_messages_path, mode="overwrite", schema_mode="overwrite")

    stats = {
        "total_messages": len(df),
        "messages_with_cpf": int(extraction_df["pii_cpf_found"].sum()),
        "messages_with_email": int(extraction_df["pii_email_found"].sum()),
        "messages_with_cep": int(extraction_df["pii_cep_found"].sum()),
        "messages_with_phone": int(extraction_df["pii_phone_found"].sum()),
        "messages_with_plate": int(extraction_df["extracted_plate"].drop_nulls().len()),
        "messages_with_competitor": int(
            extraction_df["extracted_competitor"].drop_nulls().len()
        ),
        "rows_written": len(df),
    }
    logger.info("extraction_complete", extra=stats)
    return stats
