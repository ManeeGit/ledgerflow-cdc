"""LedgerFlow CDC: a deterministic, local-first reliability reference."""

from .engine import Ledger, PipelineError

__all__ = ["Ledger", "PipelineError"]
__version__ = "0.1.0"
