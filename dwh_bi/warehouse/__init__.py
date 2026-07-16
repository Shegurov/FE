"""Free data warehouse core (DuckDB local engine, ClickHouse-ready design)."""
from .engine import Warehouse
from .schema import build_schema
from .normalize import normalize_all
from .ingest import ingest_batch

__all__ = ["Warehouse", "build_schema", "normalize_all", "ingest_batch"]
