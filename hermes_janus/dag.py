"""Merkle-DAG and Causal Vector Clock Engine for hermes-janus.

Models the timeline of snapshots as an immutable directed acyclic graph (DAG).
Supports branching to explore alternative problem-solving paths, 3-way diffs,
and ASCII visual graph rendering.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class JanusCommit:
    sha: str
    parent_sha: Optional[str]
    branch: str
    message: str
    author: str  # "agent", "user", "system"
    timestamp: float
    vector_clock: Dict[str, int]
    manifest: Dict[str, str]  # relative_path -> blob_hash
    stats: Dict[str, int] = field(default_factory=lambda: {"added": 0, "modified": 0, "deleted": 0})

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> JanusCommit:
        data = json.loads(json_str)
        return cls(**data)


class MerkleDag:
    """Manages the graph of immutable commits, branch pointers, and HEAD."""

    def __init__(self, root_dir: Path | str) -> None:
        self.root_dir = Path(root_dir)
        self.commits_dir = self.root_dir / "commits"
        self.refs_dir = self.root_dir / "refs"
        self.branches_dir = self.refs_dir / "heads"

        self.commits_dir.mkdir(parents=True, exist_ok=True)
        self.branches_dir.mkdir(parents=True, exist_ok=True)

        self.head_file = self.root_dir / "HEAD"
        if not self.head_file.exists():
            self._write_head("ref: refs/heads/main")

    def _read_head_raw(self) -> str:
        if self.head_file.exists():
            return self.head_file.read_text(encoding="utf-8").strip()
        return "ref: refs/heads/main"

    def _write_head(self, ref_content: str) -> None:
        self.head_file.write_text(ref_content, encoding="utf-8")

    def get_current_branch(self) -> str:
        head = self._read_head_raw()
        if head.startswith("ref: refs/heads/"):
            return head.replace("ref: refs/heads/", "")
        return "detached"

    def get_head_sha(self) -> Optional[str]:
        head = self._read_head_raw()
        if head.startswith("ref: refs/heads/"):
            branch = head.replace("ref: refs/heads/", "")
            return self.get_branch_sha(branch)
        return head if head else None

    def get_branch_sha(self, branch_name: str) -> Optional[str]:
        branch_file = self.branches_dir / branch_name
        if branch_file.exists():
            sha = branch_file.read_text(encoding="utf-8").strip()
            return sha if sha else None
        return None

    def set_branch_sha(self, branch_name: str, sha: str) -> None:
        branch_file = self.branches_dir / branch_name
        branch_file.parent.mkdir(parents=True, exist_ok=True)
        branch_file.write_text(sha, encoding="utf-8")

    def switch_branch(self, branch_name: str) -> None:
        self._write_head(f"ref: refs/heads/{branch_name}")

    def list_branches(self) -> List[str]:
        if not self.branches_dir.exists():
            return []
        return [f.name for f in self.branches_dir.iterdir() if f.is_file()]

    def save_commit(
        self,
        manifest: Dict[str, str],
        message: str,
        parent_sha: Optional[str] = None,
        author: str = "agent",
        branch: Optional[str] = None,
    ) -> JanusCommit:
        """Create and persist a new immutable JanusCommit."""
        current_branch = branch or self.get_current_branch()
        parent = parent_sha if parent_sha is not None else self.get_head_sha()

        # Compute vector clock
        vector_clock: Dict[str, int] = {}
        if parent:
            parent_commit = self.get_commit(parent)
            if parent_commit:
                vector_clock = dict(parent_commit.vector_clock)
        vector_clock[current_branch] = vector_clock.get(current_branch, 0) + 1

        # Calculate diff stats compared to parent
        stats = {"added": 0, "modified": 0, "deleted": 0}
        if parent:
            parent_commit = self.get_commit(parent)
            if parent_commit:
                p_man = parent_commit.manifest
                all_keys = set(manifest.keys()).union(set(p_man.keys()))
                for k in all_keys:
                    if k not in p_man and k in manifest:
                        stats["added"] += 1
                    elif k in p_man and k not in manifest:
                        stats["deleted"] += 1
                    elif p_man[k] != manifest[k]:
                        stats["modified"] += 1
        else:
            stats["added"] = len(manifest)

        timestamp = time.time()
        # Merkle SHA computed from manifest, parent, timestamp, and message
        manifest_digest = hashlib.sha256(
            json.dumps(sorted(manifest.items())).encode("utf-8")
        ).hexdigest()
        raw_hash_input = f"{parent}|{manifest_digest}|{timestamp}|{message}|{author}"
        sha = hashlib.sha256(raw_hash_input.encode("utf-8")).hexdigest()

        commit = JanusCommit(
            sha=sha,
            parent_sha=parent,
            branch=current_branch,
            message=message,
            author=author,
            timestamp=timestamp,
            vector_clock=vector_clock,
            manifest=manifest,
            stats=stats,
        )

        self.commits_dir.mkdir(parents=True, exist_ok=True)
        self.branches_dir.mkdir(parents=True, exist_ok=True)
        commit_file = self.commits_dir / f"{sha}.json"
        commit_file.write_text(commit.to_json(), encoding="utf-8")

        # Update branch ref and HEAD
        if current_branch != "detached":
            self.set_branch_sha(current_branch, sha)
        else:
            self._write_head(sha)

        return commit

    def get_commit(self, sha_or_prefix: str) -> Optional[JanusCommit]:
        """Look up a commit by full SHA or short prefix (minimum 4 chars)."""
        if not sha_or_prefix:
            return None
        sha_clean = sha_or_prefix.strip().lower()
        exact_file = self.commits_dir / f"{sha_clean}.json"
        if exact_file.exists():
            return JanusCommit.from_json(exact_file.read_text(encoding="utf-8"))

        # Prefix search
        if len(sha_clean) >= 4:
            for f in self.commits_dir.glob(f"{sha_clean}*.json"):
                return JanusCommit.from_json(f.read_text(encoding="utf-8"))
        return None

    def get_history(self, start_sha: Optional[str] = None, limit: int = 50) -> List[JanusCommit]:
        """Traverse the ancestor commit chain backwards from start_sha or current HEAD."""
        curr_sha = start_sha or self.get_head_sha()
        history: List[JanusCommit] = []
        visited: Set[str] = set()

        while curr_sha and len(history) < limit:
            if curr_sha in visited:
                break
            visited.add(curr_sha)
            commit = self.get_commit(curr_sha)
            if not commit:
                break
            history.append(commit)
            curr_sha = commit.parent_sha

        return history

    def render_timeline_ascii(self, limit: int = 15) -> str:
        """Render a clean visual timeline of the execution branches."""
        history = self.get_history(limit=limit)
        if not history:
            return "[hermes-janus] Timeline is empty. No commits recorded yet."

        current_head = self.get_head_sha()
        current_branch = self.get_current_branch()

        lines = [
            f"=== Janus Merkle Timeline (Branch: {current_branch}) ===",
            ""
        ]

        for idx, commit in enumerate(history):
            short_sha = commit.sha[:7]
            is_head = commit.sha == current_head
            head_marker = " (HEAD -> " + commit.branch + ")" if is_head else f" ({commit.branch})"
            t_str = time.strftime("%H:%M:%S", time.localtime(commit.timestamp))
            diff_str = f"+{commit.stats['added']} ~{commit.stats['modified']} -{commit.stats['deleted']}"

            marker = "●" if idx == 0 else "○"
            lines.append(f"  {marker}  [{short_sha}] {t_str}{head_marker} | {commit.message}")
            lines.append(f"  │    author: {commit.author} | changes: {diff_str}")
            if idx < len(history) - 1:
                lines.append("  │")

        return "\n".join(lines)

    def get_all_referenced_blobs(self) -> Set[str]:
        """Collect all blob hashes currently referenced across all surviving commits."""
        referenced: Set[str] = set()
        for commit_file in self.commits_dir.glob("*.json"):
            try:
                commit = JanusCommit.from_json(commit_file.read_text(encoding="utf-8"))
                referenced.update(commit.manifest.values())
            except Exception:
                pass
        return referenced

    def prune_old_commits(self, max_history: int = 50) -> int:
        """Prune commits older than max_history per branch line."""
        surviving_commits: Set[str] = set()
        for branch in self.list_branches():
            curr = self.get_branch_sha(branch)
            depth = 0
            while curr and depth < max_history:
                surviving_commits.add(curr)
                commit = self.get_commit(curr)
                if not commit:
                    break
                curr = commit.parent_sha
                depth += 1

        pruned = 0
        for commit_file in self.commits_dir.glob("*.json"):
            sha = commit_file.stem
            if sha not in surviving_commits:
                try:
                    commit_file.unlink()
                    pruned += 1
                except Exception:
                    pass
        return pruned
