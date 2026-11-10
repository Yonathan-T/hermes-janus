"""Content-Addressable Storage (CAS) for hermes-janus.

Stores immutable file blobs indexed by their cryptographic SHA-256 hash.
Unchanged files across snapshots reuse identical blobs, ensuring near-zero
storage overhead and instant snapshot creation.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Optional, Tuple


class ContentAddressableStorage:
    """Zero-copy, content-addressed blob store using SHA-256."""

    def __init__(self, root_dir: Path | str) -> None:
        self.root_dir = Path(root_dir)
        self.objects_dir = self.root_dir / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """Compute the hex SHA-256 hash of arbitrary bytes."""
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def hash_file(file_path: Path | str) -> str:
        """Compute SHA-256 hash of a file streaming in 64KB chunks."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def get_blob_path(self, blob_hash: str) -> Path:
        """Get the filesystem path for a given blob hash with fanout (ab/cdef...)."""
        prefix = blob_hash[:2]
        rest = blob_hash[2:]
        return self.objects_dir / prefix / rest

    def has_blob(self, blob_hash: str) -> bool:
        """Check whether a blob already exists in CAS."""
        return self.get_blob_path(blob_hash).exists()

    def put_bytes(self, data: bytes) -> str:
        """Store bytes into CAS, returning its SHA-256 hash."""
        blob_hash = self.hash_bytes(data)
        target_path = self.get_blob_path(blob_hash)
        if not target_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = target_path.with_suffix(".tmp")
            temp_path.write_bytes(data)
            temp_path.replace(target_path)
        return blob_hash

    def put_file(self, file_path: Path | str) -> Tuple[str, int]:
        """Store a file into CAS. Returns (blob_hash, size_in_bytes)."""
        file_path = Path(file_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"Cannot put non-file into CAS: {file_path}")

        blob_hash = self.hash_file(file_path)
        size = file_path.stat().st_size
        target_path = self.get_blob_path(blob_hash)

        if not target_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            # Fast copy
            temp_path = target_path.with_suffix(".tmp")
            shutil.copy2(file_path, temp_path)
            temp_path.replace(target_path)

        return blob_hash, size

    def get_bytes(self, blob_hash: str) -> bytes:
        """Retrieve the raw bytes of a blob by its hash."""
        blob_path = self.get_blob_path(blob_hash)
        if not blob_path.exists():
            raise KeyError(f"Blob not found in CAS: {blob_hash}")
        return blob_path.read_bytes()

    def restore_file(self, blob_hash: str, destination_path: Path | str) -> None:
        """Copy blob directly to target destination path."""
        blob_path = self.get_blob_path(blob_hash)
        if not blob_path.exists():
            raise KeyError(f"Blob not found in CAS: {blob_hash}")
        dest = Path(destination_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(blob_path, dest)

    def prune_unreferenced(self, referenced_hashes: set[str]) -> Tuple[int, int]:
        """Sweep and delete any blob not in referenced_hashes. Returns (blobs_removed, bytes_freed)."""
        removed = 0
        bytes_freed = 0
        if not self.objects_dir.exists():
            return 0, 0

        for prefix_dir in list(self.objects_dir.iterdir()):
            if not prefix_dir.is_dir():
                continue
            for blob_file in list(prefix_dir.iterdir()):
                if not blob_file.is_file():
                    continue
                blob_hash = prefix_dir.name + blob_file.name
                if blob_hash not in referenced_hashes:
                    try:
                        bytes_freed += blob_file.stat().st_size
                        blob_file.unlink()
                        removed += 1
                    except Exception:
                        pass
            try:
                if not any(prefix_dir.iterdir()):
                    prefix_dir.rmdir()
            except Exception:
                pass
        return removed, bytes_freed
