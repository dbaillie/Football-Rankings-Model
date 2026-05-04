"""SQLAlchemy Postgres connection for optional DATABASE_URL mode."""

from __future__ import annotations

import os
from collections.abc import Generator
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_engine: Engine | None = None


def database_url() -> str | None:
    raw = os.environ.get("DATABASE_URL", "").strip()
    return raw or None


def use_database() -> bool:
    return database_url() is not None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = database_url()
        if not url:
            raise RuntimeError("DATABASE_URL is not set")
        # Supabase pooler sometimes uses port 6543 (transaction mode); session pool works with SQLAlchemy.
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
            pool_reset_on_return="rollback",
            connect_args={"client_encoding": "utf8"},
        )
    return _engine


def ping_database() -> bool:
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


def get_db() -> Generator[Any, None, None]:
    """FastAPI dependency — yields a SQLAlchemy Connection (use with context)."""
    eng = get_engine()
    with eng.connect() as conn:
        yield conn


def dispose_engine() -> None:
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
