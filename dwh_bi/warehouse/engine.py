"""
Warehouse engine.

Local engine  : DuckDB (embedded, columnar, free, Apache-2.0-friendly MIT).
Scale-out path: the same SQL runs on ClickHouse (Apache 2.0). See deploy/.

The engine is intentionally thin: it owns a connection, applies PRAGMAs that
matter at scale (threads, memory, temp spill), and exposes small helpers so the
rest of the code never touches the raw cursor. Everything downstream (schema,
normalization, marts, BI) is plain SQL, which is what makes the ClickHouse
port a matter of dialect, not rewrite.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

import duckdb


class Warehouse:
    """A handle to the analytical store."""

    def __init__(
        self,
        db_path: str | os.PathLike[str] = "dwh.duckdb",
        *,
        threads: int | None = None,
        memory_limit: str = "4GB",
        temp_dir: str | None = None,
        read_only: bool = False,
    ) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(self.db_path, read_only=read_only)

        threads = threads or (os.cpu_count() or 4)
        # These PRAGMAs are what let a single node chew through large batches:
        # all cores, a memory ceiling, and disk spill so big joins never OOM.
        self.con.execute(f"PRAGMA threads={threads}")
        self.con.execute(f"PRAGMA memory_limit='{memory_limit}'")
        if temp_dir:
            Path(temp_dir).mkdir(parents=True, exist_ok=True)
            self.con.execute(f"PRAGMA temp_directory='{temp_dir}'")
        self.con.execute("PRAGMA enable_object_cache")

    # ---- execution helpers -------------------------------------------------

    def sql(self, query: str, params: Iterable[Any] | None = None) -> duckdb.DuckDBPyConnection:
        """Execute a statement, returning the cursor for chaining/fetch."""
        return self.con.execute(query, list(params) if params else None)

    def script(self, statements: str) -> None:
        """Execute a semicolon-separated batch of DDL/DML."""
        for stmt in _split_sql(statements):
            self.con.execute(stmt)

    def rows(self, query: str, params: Iterable[Any] | None = None) -> list[tuple]:
        return self.sql(query, params).fetchall()

    def one(self, query: str, params: Iterable[Any] | None = None) -> Any:
        row = self.sql(query, params).fetchone()
        return row[0] if row else None

    def records(self, query: str, params: Iterable[Any] | None = None) -> list[dict]:
        cur = self.sql(query, params)
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def count(self, table: str) -> int:
        return int(self.one(f"SELECT count(*) FROM {table}") or 0)

    def table_exists(self, name: str) -> bool:
        return bool(
            self.one(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = ?",
                [name],
            )
        )

    # ---- lake I/O (Parquet on object storage in production) ---------------

    def export_parquet(self, query: str, target: str, *, partition_by: str | None = None) -> str:
        """Materialize a query to Parquet — the on-disk/object-store format
        used for the 2000 TB lake tier."""
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        opts = "FORMAT PARQUET, COMPRESSION ZSTD"
        if partition_by:
            opts += f", PARTITION_BY ({partition_by}), OVERWRITE_OR_IGNORE"
        self.con.execute(f"COPY ({query}) TO '{target}' ({opts})")
        return target

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> "Warehouse":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _split_sql(script: str) -> list[str]:
    """Split a SQL script on ';' while ignoring separators inside strings."""
    out, buf, quote = [], [], None
    for ch in script:
        if quote:
            if ch == quote:
                quote = None
            buf.append(ch)
        elif ch in "'\"":
            quote = ch
            buf.append(ch)
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out
