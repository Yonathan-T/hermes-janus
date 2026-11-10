"""hermes-janus — Bi-directional temporal state engine & MVCC workspace for Hermes Agent.

Provides zero-copy Content-Addressable Storage (CAS), Write-Ahead Logging (WAL),
and Merkle-DAG branching for atomic workspace rollbacks and hypothesis exploration.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure package modules can be imported directly
_pkg_dir = Path(__file__).parent
if str(_pkg_dir) not in sys.path:
    sys.path.insert(0, str(_pkg_dir))

from hermes_janus import register, _handle_slash

__all__ = ["register", "_handle_slash"]
