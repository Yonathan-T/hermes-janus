"""The Janus Engine for Hermes Agent.

Coordinates Content-Addressable Storage (CAS), Write-Ahead Logging (WAL),
and the Merkle DAG to deliver transactional workspace snapshots, atomic rollback,
and speculative execution branching.
"""

from __future__ import annotations

import difflib
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .cas import ContentAddressableStorage
from .dag import JanusCommit, MerkleDag
from .wal import WriteAheadLog


DEFAULT_IGNORE_DIRS = {
    ".git",
    ".janus",
    ".venv",
    "venv",
    "vendor",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".idea",
    ".vscode",
    "dist",
    "build",
    "target",
    "bin",
    "obj",
    "AppData",
    ".cache",
    ".npm",
    ".cargo",
    ".rustup",
}

DEFAULT_IGNORE_EXTENSIONS = {
    ".pyc",
    ".pyo",
    ".pyd",
    ".tmp",
    ".log",
    ".swp",
    ".lock",
}


class JanusEngine:
    """High-level transactional workspace manager for AI agent execution."""

    def __init__(self, workspace_dir: Path | str, janus_home: Optional[Path | str] = None) -> None:
        self.workspace_dir = Path(workspace_dir).resolve()
        self.janus_dir = Path(janus_home) if janus_home else self.workspace_dir / ".janus"
        self.janus_dir.mkdir(parents=True, exist_ok=True)

        self.cas = ContentAddressableStorage(self.janus_dir / "store")
        self.wal = WriteAheadLog(self.janus_dir / "wal")
        self.dag = MerkleDag(self.janus_dir / "dag")

        self._ensure_gitignore()

        # Automatically create initial root snapshot if brand new
        if not self.dag.get_head_sha():
            self.snapshot(message="Initial repository state", author="system")

    def _ensure_gitignore(self) -> None:
        """Ensure .janus is added to the workspace's .gitignore to prevent accidental git commits."""
        try:
            gitignore_path = self.workspace_dir / ".gitignore"
            if gitignore_path.exists():
                content = gitignore_path.read_text(encoding="utf-8", errors="ignore")
                lines = {line.strip() for line in content.splitlines()}
                if ".janus" not in lines and ".janus/" not in lines:
                    with open(gitignore_path, "a", encoding="utf-8") as f:
                        if content and not content.endswith("\n"):
                            f.write("\n")
                        f.write("\n# Hermes Janus temporal engine\n.janus/\n")
            elif (self.workspace_dir / ".git").is_dir():
                gitignore_path.write_text("# Hermes Janus temporal engine\n.janus/\n", encoding="utf-8")
        except Exception:
            pass

    def _should_ignore(self, path: Path) -> bool:
        """Filter out transient, virtualenv, and build artifacts."""
        try:
            rel = path.relative_to(self.workspace_dir)
            rel_parts = set(rel.parts[:-1])
        except Exception:
            rel_parts = set()

        if any(ignored in rel_parts for ignored in DEFAULT_IGNORE_DIRS):
            return True
        name_lower = path.name.lower()
        if name_lower.startswith("ntuser") or name_lower.endswith((".dat", ".log", ".blf", ".regtrans-ms", ".ini")):
            return True
        if path.suffix in DEFAULT_IGNORE_EXTENSIONS:
            return True
        # Do not scan large binaries > 25MB
        try:
            if path.is_file() and path.stat().st_size > 25 * 1024 * 1024:
                return True
        except Exception:
            return True
        return False

    def scan_workspace_files(self) -> Dict[str, Path]:
        """Scan workspace and return relative_path -> absolute_path mapping."""
        files: Dict[str, Path] = {}
        if not self.workspace_dir.exists():
            return files

        is_home_root = (self.workspace_dir == Path.home())

        for root, dirs, filenames in os.walk(self.workspace_dir):
            # If the user launched directly in their home root (e.g. C:\Users\hp),
            # do not deep-walk all personal folders (OneDrive, Downloads, AppData, etc.).
            if is_home_root and Path(root) != self.workspace_dir:
                dirs[:] = []
                continue

            # Prune ignored directories in-place for fast traversal
            dirs[:] = [d for d in dirs if d not in DEFAULT_IGNORE_DIRS and not d.startswith(".")]

            for filename in filenames:
                file_path = Path(root) / filename
                if not self._should_ignore(file_path):
                    rel_path = file_path.relative_to(self.workspace_dir).as_posix()
                    files[rel_path] = file_path

        return files

    def snapshot(self, message: str = "Automatic checkpoint", author: str = "agent") -> JanusCommit:
        """Take an atomic, zero-copy snapshot of the workspace."""
        current_files = self.scan_workspace_files()
        manifest: Dict[str, str] = {}

        # Store each file into CAS
        for rel_path, abs_path in current_files.items():
            try:
                blob_hash, _ = self.cas.put_file(abs_path)
                manifest[rel_path] = blob_hash
            except (PermissionError, OSError):
                continue

        # Commit into DAG
        commit = self.dag.save_commit(
            manifest=manifest,
            message=message,
            author=author,
        )
        return commit

    def rewind(self, target: int | str = 1, force: bool = False, update_branch_ref: bool = True) -> Tuple[bool, str]:
        """Rewind the workspace state.

        Args:
            target: Either an integer (number of steps to rewind) or a commit SHA/prefix.
            force: If True, restore workspace files even if HEAD already matches target SHA.
            update_branch_ref: If True, updates the branch ref to point to target commit.

        Returns:
            (success: bool, status_message: str)
        """
        history = self.dag.get_history(limit=100)
        if not history:
            return False, "No commits available to rewind to."

        target_commit: Optional[JanusCommit] = None

        if isinstance(target, int):
            if target <= 0:
                return False, "Rewind steps must be at least 1."
            if target >= len(history):
                target_commit = history[-1]
            else:
                target_commit = history[target]
        else:
            target_commit = self.dag.get_commit(str(target))

        if not target_commit:
            return False, f"Target commit '{target}' not found in DAG."

        current_head_sha = self.dag.get_head_sha()
        if not force and target_commit.sha == current_head_sha:
            return True, f"Workspace is already at target commit [{target_commit.sha[:7]}]."

        # Apply snapshot manifest back to workspace
        target_manifest = target_commit.manifest
        current_files = self.scan_workspace_files()

        # 1. Delete files present now but absent in target snapshot
        deleted_count = 0
        for rel_path, abs_path in current_files.items():
            if rel_path not in target_manifest:
                try:
                    abs_path.unlink(missing_ok=True)
                    deleted_count += 1
                except Exception:
                    pass

        # 2. Restore files from CAS
        restored_count = 0
        for rel_path, blob_hash in target_manifest.items():
            dest_path = self.workspace_dir / rel_path
            current_hash = self.cas.hash_file(dest_path) if dest_path.exists() else None
            if current_hash != blob_hash:
                self.cas.restore_file(blob_hash, dest_path)
                restored_count += 1

        # Move branch ref only when explicitly requested
        if update_branch_ref:
            current_branch = self.dag.get_current_branch()
            if current_branch != "detached":
                self.dag.set_branch_sha(current_branch, target_commit.sha)
            else:
                self.dag._write_head(target_commit.sha)

        # Log rollback to WAL
        self.wal.commit(tx_id=f"rewind-{target_commit.sha[:7]}", commit_sha=target_commit.sha)

        msg = (
            f"Successfully rewound to [{target_commit.sha[:7]}] '{target_commit.message}'. "
            f"({restored_count} files restored, {deleted_count} deleted)"
        )
        return True, msg

    def branch(self, branch_name: str) -> Tuple[bool, str]:
        """Create or switch to a branch."""
        clean_name = branch_name.strip().replace(" ", "-")
        head_sha = self.dag.get_head_sha()
        if not head_sha:
            return False, "Cannot branch from an empty timeline."

        existing_sha = self.dag.get_branch_sha(clean_name)
        if existing_sha:
            # 1. Switch branch ref
            self.dag.switch_branch(clean_name)
            # 2. Restore workspace to target commit without overwriting branch ref
            self.rewind(existing_sha, force=True, update_branch_ref=False)
            return True, f"Switched to existing branch '{clean_name}' at [{existing_sha[:7]}]."

        # Create new branch pointing at current HEAD
        self.dag.set_branch_sha(clean_name, head_sha)
        self.dag.switch_branch(clean_name)
        return True, f"Created and checked out new branch '{clean_name}' from [{head_sha[:7]}]."

    def diff(self, sha_a: Optional[str] = None, sha_b: Optional[str] = None) -> str:
        """Generate a unified diff between two commits, or between current files and HEAD."""
        commit_b = self.dag.get_commit(sha_b) if sha_b else None
        manifest_b: Dict[str, str] = {}

        if sha_b and commit_b:
            manifest_b = commit_b.manifest
        else:
            # Current workspace
            current_files = self.scan_workspace_files()
            for r, a in current_files.items():
                manifest_b[r] = self.cas.hash_file(a)

        commit_a = self.dag.get_commit(sha_a) if sha_a else self.dag.get_commit(self.dag.get_head_sha() or "")
        manifest_a = commit_a.manifest if commit_a else {}

        all_keys = sorted(set(manifest_a.keys()).union(set(manifest_b.keys())))
        diff_outputs: List[str] = []

        for k in all_keys:
            hash_a = manifest_a.get(k)
            hash_b = manifest_b.get(k)
            if hash_a != hash_b:
                lines_a = self.cas.get_bytes(hash_a).decode("utf-8", errors="replace").splitlines(keepends=True) if hash_a else []
                lines_b = self.cas.get_bytes(hash_b).decode("utf-8", errors="replace").splitlines(keepends=True) if hash_b else []
                file_diff = list(difflib.unified_diff(lines_a, lines_b, fromfile=f"a/{k}", tofile=f"b/{k}"))
                if file_diff:
                    diff_outputs.extend(file_diff)

        if not diff_outputs:
            return "No changes detected."
        return "".join(diff_outputs)

    def merge(self, source_branch: str) -> Tuple[bool, str]:
        """Perform a three-way merge from source_branch into current branch."""
        current_branch = self.dag.get_current_branch()
        if current_branch == source_branch:
            return False, f"Cannot merge branch '{source_branch}' into itself."

        source_sha = self.dag.get_branch_sha(source_branch)
        if not source_sha:
            return False, f"Branch '{source_branch}' not found."

        current_sha = self.dag.get_head_sha()
        if not current_sha:
            return False, "Current HEAD is not set."

        current_commit = self.dag.get_commit(current_sha)
        source_commit = self.dag.get_commit(source_sha)
        if not current_commit or not source_commit:
            return False, "Failed to load commits for merge."

        # Find common ancestor (simple BFS)
        visited: Set[str] = set()
        c = current_commit
        while c:
            visited.add(c.sha)
            c = self.dag.get_commit(c.parent_sha) if c.parent_sha else None

        ancestor: Optional[JanusCommit] = None
        s = source_commit
        while s:
            if s.sha in visited:
                ancestor = s
                break
            s = self.dag.get_commit(s.parent_sha) if s.parent_sha else None

        base_manifest = ancestor.manifest if ancestor else {}
        curr_manifest = current_commit.manifest
        src_manifest = source_commit.manifest

        merged_manifest = dict(curr_manifest)
        conflicts: List[str] = []

        all_keys = set(base_manifest.keys()).union(curr_manifest.keys()).union(src_manifest.keys())
        for k in all_keys:
            base_h = base_manifest.get(k)
            curr_h = curr_manifest.get(k)
            src_h = src_manifest.get(k)

            # Clean fast-forward or source-only modification
            if base_h == curr_h and src_h != curr_h:
                if src_h is not None:
                    merged_manifest[k] = src_h
                else:
                    merged_manifest.pop(k, None)
            elif curr_h != src_h and base_h != src_h and base_h != curr_h:
                conflicts.append(k)

        if conflicts:
            return False, f"Merge conflicts detected in: {', '.join(conflicts)}. Manual resolution required."

        # Apply merged manifest to workspace
        for k, blob_hash in merged_manifest.items():
            dest = self.workspace_dir / k
            self.cas.restore_file(blob_hash, dest)

        # Commit merged state
        merge_commit = self.dag.save_commit(
            manifest=merged_manifest,
            message=f"Merge branch '{source_branch}' into {current_branch}",
            author="janus",
        )
        return True, f"Successfully merged '{source_branch}' into '{current_branch}' [{merge_commit.sha[:7]}]."
