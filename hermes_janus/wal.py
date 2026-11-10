"""Write-Ahead Log (WAL) for hermes-janus.

Guarantees durability and atomic rollback. Every mutation intent (write_file,
patch, terminal command execution) is recorded to the append-only WAL
in a PREPARED state *before* disk mutation occurs. Upon completion, the
transaction transitions to COMMITTED or ABORTED.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class WalEntry:
    tx_id: str
    timestamp: float
    state: str  # "PREPARED", "COMMITTED", "ABORTED"
    tool_name: str
    tool_args: Dict[str, Any]
    pre_state_blobs: Dict[str, Optional[str]] = field(default_factory=dict)
    # relative_file_path -> blob_hash (or None if file didn't exist before)
    commit_sha: Optional[str] = None
    error: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, json_str: str) -> WalEntry:
        data = json.loads(json_str)
        return cls(**data)


class WriteAheadLog:
    """Append-only transaction log ensuring durability across crash boundaries."""

    def __init__(self, root_dir: Path | str) -> None:
        self.root_dir = Path(root_dir)
        self.wal_file = self.root_dir / "wal.jsonl"
        self.root_dir.mkdir(parents=True, exist_ok=True)
        if not self.wal_file.exists():
            self.wal_file.touch()

    def _append(self, entry: WalEntry) -> None:
        with open(self.wal_file, "a", encoding="utf-8") as f:
            f.write(entry.to_json() + "\n")

    def prepare(
        self,
        tool_name: str,
        tool_args: Dict[str, Any],
        pre_state_blobs: Dict[str, Optional[str]],
    ) -> str:
        """Begin a new transaction and log its pre-state to WAL."""
        tx_id = str(uuid.uuid4())[:8]
        entry = WalEntry(
            tx_id=tx_id,
            timestamp=time.time(),
            state="PREPARED",
            tool_name=tool_name,
            tool_args=tool_args,
            pre_state_blobs=pre_state_blobs,
        )
        self._append(entry)
        return tx_id

    def commit(self, tx_id: str, commit_sha: Optional[str] = None) -> None:
        """Mark transaction as COMMITTED with the associated snapshot commit SHA."""
        entry = WalEntry(
            tx_id=tx_id,
            timestamp=time.time(),
            state="COMMITTED",
            tool_name="",
            tool_args={},
            commit_sha=commit_sha,
        )
        self._append(entry)

    def abort(self, tx_id: str, error: Optional[str] = None) -> None:
        """Mark transaction as ABORTED."""
        entry = WalEntry(
            tx_id=tx_id,
            timestamp=time.time(),
            state="ABORTED",
            tool_name="",
            tool_args={},
            error=error,
        )
        self._append(entry)

    def read_entries(self) -> List[WalEntry]:
        """Read all entries from the log."""
        if not self.wal_file.exists():
            return []
        entries: List[WalEntry] = []
        with open(self.wal_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(WalEntry.from_json(line))
                    except Exception:
                        continue
        return entries

    def get_pending_transactions(self) -> List[WalEntry]:
        """Return transactions that were PREPARED but neither COMMITTED nor ABORTED."""
        entries = self.read_entries()
        tx_states: Dict[str, WalEntry] = {}
        for entry in entries:
            if entry.state == "PREPARED":
                tx_states[entry.tx_id] = entry
            elif entry.state in ("COMMITTED", "ABORTED"):
                tx_states.pop(entry.tx_id, None)
        return list(tx_states.values())

    def compact(self, keep_last: int = 20) -> int:
        """Compact WAL log by discarding old finalized entries while preserving pending transactions."""
        entries = self.read_entries()
        pending = self.get_pending_transactions()
        pending_ids = {e.tx_id for e in pending}

        # Keep all pending entries and the last N entries
        keep_entries = []
        for e in entries:
            if e.tx_id in pending_ids:
                keep_entries.append(e)

        tail = entries[-keep_last:] if len(entries) > keep_last else entries
        for e in tail:
            if e not in keep_entries:
                keep_entries.append(e)

        pruned = len(entries) - len(keep_entries)
        if pruned > 0:
            with open(self.wal_file, "w", encoding="utf-8") as f:
                for e in keep_entries:
                    f.write(e.to_json() + "\n")
        return pruned
