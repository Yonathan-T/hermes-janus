"""hermes-janus — Bi-directional temporal state engine & MVCC workspace for Hermes Agent.

Provides zero-copy Content-Addressable Storage (CAS), Write-Ahead Logging (WAL),
and Merkle-DAG branching for atomic workspace rollbacks and hypothesis exploration.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from .engine import JanusEngine

logger = logging.getLogger("hermes.plugins.janus")

# Cache engine instances per workspace directory
_engines: Dict[str, JanusEngine] = {}
_active_workspace: Optional[Path] = None
_active_tx_id: Optional[str] = None


def _resolve_workspace() -> Path:
    """Resolve the active workspace directory safely and dynamically."""
    # 1. Hermes documented public API for agent/session working directory
    try:
        from agent.runtime_cwd import resolve_agent_cwd
        p = resolve_agent_cwd()
        if p and p.is_dir():
            is_hermes_install = (
                p.name == "hermes-agent"
                or (p / ".bytecode-fingerprint").exists()
                or (p / "hermes_cli").is_dir()
            )
            if not is_hermes_install:
                return p.resolve()
    except Exception:
        pass

    # 2. Check environment variables passed by session / terminal
    for env_var in ("TERMINAL_CWD", "HERMES_WORKSPACE", "HERMES_SESSION_CWD"):
        val = os.environ.get(env_var)
        if val and Path(val).is_dir():
            return Path(val).resolve()

    # 3. Fallback to process cwd
    cwd = Path.cwd().resolve()
    is_hermes_install = (
        cwd.name == "hermes-agent"
        or (cwd / ".bytecode-fingerprint").exists()
        or (cwd / "hermes_cli").is_dir()
    )
    if is_hermes_install:
        term_cwd = os.environ.get("TERMINAL_CWD")
        if term_cwd and Path(term_cwd).is_dir():
            return Path(term_cwd).resolve()

    return cwd


def get_engine(target: Optional[str | Path] = None) -> JanusEngine:
    """Retrieve or initialize the JanusEngine for the target or active workspace."""
    global _active_workspace
    if target:
        target_path = Path(target).resolve()
        _active_workspace = target_path
    elif _active_workspace is not None:
        target_path = _active_workspace
    else:
        target_path = _resolve_workspace()

    key = str(target_path)
    if key not in _engines:
        _engines[key] = JanusEngine(target_path)
    return _engines[key]


# -----------------------------------------------------------------------------
# Lifecycle Hooks
# -----------------------------------------------------------------------------

MUTATING_TOOLS = {
    "write_file",
    "edit_file",
    "patch",
    "replace_in_file",
    "delete_file",
    "terminal",
    "execute_code",
    "bash",
}


def _on_pre_tool_call(tool_name: str, args: Dict[str, Any], **kwargs: Any) -> None:
    """Intercept mutating actions to record WAL intent before disk modification."""
    global _active_tx_id
    if tool_name not in MUTATING_TOOLS:
        return

    engine = get_engine()
    pre_state: Dict[str, Optional[str]] = {}

    # Extract target path if explicit
    path_arg = args.get("path") or args.get("file")
    if isinstance(path_arg, str) and path_arg:
        full_path = (engine.workspace_dir / path_arg).resolve()
        if full_path.exists() and full_path.is_file():
            try:
                pre_state[path_arg] = engine.cas.hash_file(full_path)
            except Exception:
                pass
        else:
            pre_state[path_arg] = None

    _active_tx_id = engine.wal.prepare(
        tool_name=tool_name,
        tool_args=args,
        pre_state_blobs=pre_state,
    )
    logger.debug("[janus] WAL PREPARED tx_id=%s tool=%s", _active_tx_id, tool_name)


def _on_post_tool_call(tool_name: str, args: Dict[str, Any], result: Any = None, **kwargs: Any) -> None:
    """Finalize WAL transaction and take snapshot if mutation occurred."""
    global _active_tx_id
    if tool_name not in MUTATING_TOOLS:
        return

    engine = get_engine()
    try:
        # Commit snapshot of new state
        commit = engine.snapshot(
            message=f"auto: after {tool_name}",
            author="agent",
        )
        if _active_tx_id:
            engine.wal.commit(_active_tx_id, commit_sha=commit.sha)
            logger.debug("[janus] WAL COMMITTED tx_id=%s commit=%s", _active_tx_id, commit.sha[:7])
    except Exception as e:
        logger.error("[janus] Error during post_tool_call: %s", e)
        if _active_tx_id:
            engine.wal.abort(_active_tx_id, error=str(e))
    finally:
        _active_tx_id = None


def _on_session_end(session_id: str = "", **kwargs: Any) -> None:
    """Check for any orphaned or uncommitted transactions at session end."""
    engine = get_engine()
    pending = engine.wal.get_pending_transactions()
    if pending:
        logger.warning("[janus] Found %d uncommitted WAL transactions at session end.", len(pending))


# -----------------------------------------------------------------------------
# Slash Command Handlers (/janus ...)
# -----------------------------------------------------------------------------

def _cmd_timeline(argv: List[str]) -> str:
    engine = get_engine()
    limit = 15
    if len(argv) > 1 and argv[1].isdigit():
        limit = int(argv[1])
    return engine.dag.render_timeline_ascii(limit=limit)


def _cmd_checkpoint(argv: List[str]) -> str:
    engine = get_engine()
    msg = " ".join(argv[1:]) if len(argv) > 1 else "Manual user checkpoint"
    commit = engine.snapshot(message=msg, author="user")
    return f"[janus] Checkpoint created: [{commit.sha[:7]}] on branch '{commit.branch}' — \"{msg}\""


def _cmd_rewind(argv: List[str]) -> str:
    engine = get_engine()
    target = argv[1] if len(argv) > 1 else "1"
    if target.isdigit():
        success, msg = engine.rewind(int(target))
    else:
        success, msg = engine.rewind(target)
    return f"[janus] {msg}"


def _cmd_branch(argv: List[str]) -> str:
    engine = get_engine()
    if len(argv) < 2:
        current = engine.dag.get_current_branch()
        branches = engine.dag.list_branches()
        b_list = ", ".join(f"*{b}" if b == current else b for b in branches)
        return f"[janus] Current branch: {current}\nAvailable branches: {b_list}\nUsage: /janus branch <name>"
    success, msg = engine.branch(argv[1])
    return f"[janus] {msg}"


def _cmd_diff(argv: List[str]) -> str:
    engine = get_engine()
    sha_a = argv[1] if len(argv) > 1 else None
    sha_b = argv[2] if len(argv) > 2 else None
    return engine.diff(sha_a, sha_b)


def _cmd_merge(argv: List[str]) -> str:
    engine = get_engine()
    if len(argv) < 2:
        return "[janus] Usage: /janus merge <branch_name>"
    success, msg = engine.merge(argv[1])
    return f"[janus] {msg}"


def _cmd_workspace(argv: List[str]) -> str:
    global _active_workspace
    if len(argv) > 1:
        target = Path(argv[1]).resolve()
        if not target.is_dir():
            return f"[janus] Error: Path '{target}' is not a directory."
        _active_workspace = target
        engine = get_engine(target)
        head = engine.dag.get_head_sha()
        commit = engine.dag.get_commit(head) if head else None
        files_count = len(commit.manifest) if commit else 0
        return f"[janus] Workspace set to: {target}\n  Current Branch: {engine.dag.get_current_branch()}\n  Tracked Files: {files_count}"

    engine = get_engine()
    return (
        f"[janus] Active Workspace: {engine.workspace_dir}\n"
        f"Usage: /janus workspace <path> to switch workspace explicitly."
    )


def _cmd_status(argv: List[str]) -> str:
    engine = get_engine()
    branch = engine.dag.get_current_branch()
    head = engine.dag.get_head_sha()
    short_head = head[:7] if head else "none"
    commit = engine.dag.get_commit(head) if head else None
    tracked_count = len(commit.manifest) if commit else 0
    return (
        f"[hermes-janus v1.0.0]\n"
        f"  Workspace: {engine.workspace_dir}\n"
        f"  Current Branch: {branch}\n"
        f"  HEAD: [{short_head}]\n"
        f"  Tracked Files: {tracked_count}"
    )


_COMMANDS: Dict[str, Callable[[List[str]], str]] = {
    "timeline": _cmd_timeline,
    "checkpoint": _cmd_checkpoint,
    "rewind": _cmd_rewind,
    "branch": _cmd_branch,
    "diff": _cmd_diff,
    "merge": _cmd_merge,
    "status": _cmd_status,
    "workspace": _cmd_workspace,
}

_HELP = """Hermes-Janus: Bi-directional temporal state engine & MVCC workspace

Subcommands:
  /janus timeline [n]          Render visual ASCII DAG of execution history
  /janus checkpoint [msg]      Create a named checkpoint
  /janus rewind [steps | sha]  Roll back workspace to an earlier commit
  /janus branch [name]         Create or switch to an execution branch
  /janus diff [sha_a] [sha_b]  Show changes between commits or current workspace
  /janus merge <branch>        Three-way merge an exploration branch
  /janus status                Display current branch, HEAD, and tracked files
  /janus workspace [path]      Show or set the active workspace directory
"""


def _handle_slash(raw_args: str) -> str:
    argv = raw_args.strip().split()
    if not argv or argv[0] in {"help", "-h", "--help"}:
        print(_HELP)
        return _HELP
    handler = _COMMANDS.get(argv[0])
    if handler is None:
        msg = f"Unknown subcommand: {argv[0]}\n\n{_HELP}"
        print(msg)
        return msg
    res = handler(argv)
    if res:
        print(res)
    return res


# -----------------------------------------------------------------------------
# Autonomous AI Tools
# -----------------------------------------------------------------------------

def tool_janus_checkpoint(message: str) -> str:
    """Create a named checkpoint of the workspace."""
    engine = get_engine()
    commit = engine.snapshot(message=message, author="agent")
    return f"Created checkpoint [{commit.sha[:7]}] on branch '{commit.branch}'."


def tool_janus_rewind(steps: int = 1, sha: Optional[str] = None) -> str:
    """Rewind workspace to previous state (steps back or commit sha)."""
    engine = get_engine()
    target = sha if sha else steps
    success, msg = engine.rewind(target)
    return msg


def tool_janus_branch(branch_name: str) -> str:
    """Fork timeline to explore an alternative hypothesis or implementation."""
    engine = get_engine()
    success, msg = engine.branch(branch_name)
    return msg


def tool_janus_timeline() -> str:
    """Inspect timeline and past execution branches."""
    engine = get_engine()
    return engine.dag.render_timeline_ascii(limit=10)


def tool_janus_diff() -> str:
    """View file differences between current state and previous commit."""
    engine = get_engine()
    return engine.diff()


def tool_janus_merge(branch_name: str) -> str:
    """Merge successful branch back into current branch."""
    engine = get_engine()
    success, msg = engine.merge(branch_name)
    return msg


# Tool schemas for Hermes Agent
_TOOL_DEFINITIONS = [
    (
        "janus_checkpoint",
        {
            "type": "object",
            "properties": {"message": {"type": "string", "description": "Reason for checkpoint"}},
            "required": ["message"],
        },
        tool_janus_checkpoint,
    ),
    (
        "janus_rewind",
        {
            "type": "object",
            "properties": {
                "steps": {"type": "integer", "description": "Number of commits to rewind", "default": 1},
                "sha": {"type": "string", "description": "Specific commit SHA to restore (optional)"},
            },
        },
        tool_janus_rewind,
    ),
    (
        "janus_branch",
        {
            "type": "object",
            "properties": {"branch_name": {"type": "string", "description": "Name of the branch to create/checkout"}},
            "required": ["branch_name"],
        },
        tool_janus_branch,
    ),
    (
        "janus_timeline",
        {"type": "object", "properties": {}},
        tool_janus_timeline,
    ),
    (
        "janus_diff",
        {"type": "object", "properties": {}},
        tool_janus_diff,
    ),
    (
        "janus_merge",
        {
            "type": "object",
            "properties": {"branch_name": {"type": "string", "description": "Branch to merge into current"}},
            "required": ["branch_name"],
        },
        tool_janus_merge,
    ),
]


# -----------------------------------------------------------------------------
# Plugin Registration Entrypoint
# -----------------------------------------------------------------------------

def register(ctx: Any) -> None:
    """Register hooks, tools, and slash commands with Hermes Agent."""
    ctx.register_hook("pre_tool_call", _on_pre_tool_call)
    ctx.register_hook("post_tool_call", _on_post_tool_call)
    ctx.register_hook("on_session_end", _on_session_end)

    ctx.register_command(
        "janus",
        handler=_handle_slash,
        description="Bi-directional temporal state engine (timeline, rewind, branch, merge).",
    )

    for name, schema, handler in _TOOL_DEFINITIONS:
        ctx.register_tool(
            name=name,
            toolset="janus",
            schema=schema,
            handler=handler,
        )

    logger.info("[hermes-janus] Plugin registered successfully.")
