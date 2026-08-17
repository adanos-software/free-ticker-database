"""Stable UUID identifiers for canonical market-data entities."""

from __future__ import annotations

import uuid

NAMESPACE = uuid.UUID("541dfcb0-b565-5e2c-b9f3-7e00c1a3177f")


def stable_id(kind: str, value: str) -> str:
    kind = str(kind).strip()
    value = str(value).strip()
    if not kind or not value:
        raise ValueError("stable IDs require non-empty kind and value")
    return str(uuid.uuid5(NAMESPACE, f"{kind}:{value}"))
