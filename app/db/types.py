"""PostgreSQL `ltree` support for SQLAlchemy 2.0 + asyncpg.

asyncpg has no built-in codec for `ltree`, so three independent mechanisms keep
values flowing as plain Python strings:

1. ``app.db.session`` registers an asyncpg type codec on every new connection.
2. :meth:`LtreeType.bind_expression` sends parameters as ``text`` and converts
   them server-side with ``text2ltree()``, so the driver never needs to encode
   the ltree wire format.
3. :meth:`LtreeType.column_expression` casts results back to ``varchar`` so the
   driver never needs to decode it either.

Comparisons still run against the raw column, so the GiST index is used.
"""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import String, cast, func, type_coerce
from sqlalchemy.types import UserDefinedType

LABEL_PATTERN = re.compile(r"^[A-Za-z0-9_]{1,255}$")

MAX_LABEL_LENGTH = 60
MAX_DEPTH = 32


class LtreeType(UserDefinedType):
    """Maps the Postgres ``ltree`` type to Python ``str``."""

    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        return "LTREE"

    @property
    def python_type(self) -> type:
        return str

    def bind_expression(self, bindvalue: Any) -> Any:
        return func.text2ltree(type_coerce(bindvalue, String))

    def column_expression(self, colexpr: Any) -> Any:
        return cast(colexpr, String)

    class comparator_factory(UserDefinedType.Comparator):
        """ltree operators, usable directly on mapped attributes."""

        def descendant_of(self, other: Any) -> Any:
            """``self <@ other`` - self is at or below `other`."""
            return self.op("<@", is_comparison=True)(_as_ltree(other))

        def ancestor_of(self, other: Any) -> Any:
            """``self @> other`` - self is at or above `other`."""
            return self.op("@>", is_comparison=True)(_as_ltree(other))

        def matches_lquery(self, other: Any) -> Any:
            """``self ~ other`` - lquery pattern match (e.g. ``root.*{1}``)."""
            return self.op("~", is_comparison=True)(cast(other, LQueryType()))

        def concat_label(self, other: Any) -> Any:
            """``self || other`` - append a label or path."""
            return self.op("||", return_type=LtreeType())(_as_ltree(other))


class LQueryType(UserDefinedType):
    """Maps the Postgres ``lquery`` type (patterns matched against ltree)."""

    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        return "LQUERY"

    def bind_expression(self, bindvalue: Any) -> Any:
        return cast(cast(type_coerce(bindvalue, String), String), self)


def _as_ltree(value: Any) -> Any:
    """Wrap raw Python strings so they reach Postgres as ``ltree``."""
    if isinstance(value, str):
        return func.text2ltree(value)
    return value


def slugify_label(name: str) -> str:
    """Turn an arbitrary folder name into a valid, lowercase ltree label."""
    normalised = re.sub(r"[^A-Za-z0-9]+", "_", name.strip()).strip("_").lower()
    normalised = re.sub(r"_{2,}", "_", normalised)
    if not normalised:
        normalised = "folder"
    if normalised[0].isdigit():
        normalised = f"f_{normalised}"
    return normalised[:MAX_LABEL_LENGTH].rstrip("_") or "folder"


def is_valid_label(label: str) -> bool:
    return bool(LABEL_PATTERN.match(label))


def path_depth(path: str) -> int:
    return len(path.split(".")) if path else 0


def join_path(parent_path: str | None, label: str) -> str:
    return f"{parent_path}.{label}" if parent_path else label


def parent_path(path: str) -> str | None:
    head, _, _ = path.rpartition(".")
    return head or None
