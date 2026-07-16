"""Embedded BI: semantic metrics, mart builder, and a zero-dependency server."""
from .metrics import build_marts, MARTS, query_metric

__all__ = ["build_marts", "MARTS", "query_metric"]
