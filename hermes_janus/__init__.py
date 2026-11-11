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

_engines: Dict[str, JanusEngine] = {}
_active_workspace: Optional[Path] = None
_active_tx_id: Optional[str] = None

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

_SYSTEM_PROMPT = """## Janus Temporal State Engine & Safe Exploration Guidelines
You have access to the Janus temporal state engine tools (`janus_*`) to safely explore hypotheses and self-correct:
1. `janus_branch(branch_name)`: Fork the workspace to an isolated exploration branch BEFORE attempting speculative refactors, risky multi-file edits, or experimental bug fixes.
2. `janus_checkpoint(message)`: Record a clean baseline restore point before executing complex transformations.
3. `janus_rewind(steps=1)`: If your changes introduce errors, fail compilation, or fail tests (`go test`, `pytest`), call `janus_rewind(1)` immediately to cleanly restore the workspace to the prior clean state and attempt an alternative approach.
4. `janus_merge(branch_name)`: Once your exploration branch passes all verification checks and tests, merge it back into the main branch.
5. `janus_diff()`: Inspect the exact file delta between your current changes and the prior checkpoint.
"""


def _resolve_workspace() -> Path:
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

    for env_var in ("TERMINAL_CWD", "HERMES_WORKSPACE", "HERMES_SESSION_CWD"):
        val = os.environ.get(env_var)
        if val and Path(val).is_dir():
            return Path(val).resolve()

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
    global _active_workspace
    if target:
        target_path = Path(target).resolve()
        _active_workspace = target_path
    elif _active_workspace is not None:
        target_path = _active_workspace
    else:
        target_path = _resolve_workspace()

    key = str(target_path)
    if key in _engines:
        if not _engines[key].janus_dir.exists():
            del _engines[key]
    if key not in _engines:
        _engines[key] = JanusEngine(target_path)
    return _engines[key]


def _on_pre_tool_call(tool_name: str, args: Dict[str, Any], **kwargs: Any) -> None:
    global _active_tx_id
    if tool_name not in MUTATING_TOOLS:
        return

    engine = get_engine()
    pre_state: Dict[str, Optional[str]] = {}

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
    global _active_tx_id
    if tool_name not in MUTATING_TOOLS:
        return

    engine = get_engine()
    try:
        target_info = ""
        target_path = args.get("path") or args.get("file") or args.get("filepath")
        if target_path and isinstance(target_path, str):
            target_info = f" {Path(target_path).name}"
        elif "command" in args and isinstance(args.get("command"), str):
            cmd = args["command"].strip().split("\n")[0]
            if len(cmd) > 30:
                cmd = cmd[:27] + "..."
            target_info = f" '{cmd}'"

        commit = engine.snapshot(
            message=f"auto: {tool_name}{target_info}",
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
    engine = get_engine()
    pending = engine.wal.get_pending_transactions()
    if pending:
        logger.warning("[janus] Found %d uncommitted WAL transactions at session end.", len(pending))
    info = engine.storage_info()
    if info["total_bytes"] > 50 * 1024 * 1024 or info["commit_count"] > 100:
        engine.gc()


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
    info = engine.storage_info()
    mb = info["total_bytes"] / (1024 * 1024)
    return (
        f"[hermes-janus v1.0.0]\n"
        f"  Workspace: {engine.workspace_dir}\n"
        f"  Current Branch: {branch}\n"
        f"  HEAD: [{short_head}]\n"
        f"  Tracked Files: {tracked_count}\n"
        f"  Storage: {mb:.2f} MB ({info['blob_count']} CAS blobs, {info['commit_count']} commits)"
    )


def _cmd_gc(argv: List[str]) -> str:
    engine = get_engine()
    result = engine.gc()
    freed_kb = result["bytes_freed"] / 1024
    return (
        f"[janus] Garbage collection complete:\n"
        f"  Pruned commits: {result['commits_pruned']}\n"
        f"  Removed blobs:  {result['blobs_removed']}\n"
        f"  Reclaimed:      {freed_kb:.1f} KB\n"
        f"  WAL compacted:  {result['wal_compacted']}"
    )


def _cmd_elect(argv: List[str]) -> str:
    if len(argv) < 3:
        return "[janus] Usage: /janus elect <branch_1> <branch_2> ... [--test <test_command>]"
    branches: List[str] = []
    test_cmd = ""
    idx = 1
    while idx < len(argv):
        if argv[idx] in ("--test", "-t") and idx + 1 < len(argv):
            test_cmd = " ".join(argv[idx + 1:]).strip()
            if (test_cmd.startswith('"') and test_cmd.endswith('"')) or (test_cmd.startswith("'") and test_cmd.endswith("'")):
                test_cmd = test_cmd[1:-1].strip()
            break
        branches.append(argv[idx])
        idx += 1

    engine = get_engine()
    res = engine.tournament(branches, test_command=test_cmd, auto_merge=True)
    lines = ["=== Janus Tournament Consensus ===", f"Candidates: {', '.join(branches)}"]
    for b, r in res["results"].items():
        status = "PASSED" if r["passed"] else "FAILED"
        lines.append(f"  [{status}] '{b}': {r['duration']}s — {r['output'][:60]}")
    if res["winner"]:
        lines.append(f"\n★ Elected Winner: '{res['winner']}'")
        if res["merged"]:
            lines.append(f"  {res['merge_message']}")
    else:
        lines.append("\n✗ No candidate passed the evaluation criteria.")
    return "\n".join(lines)


_COMMANDS: Dict[str, Callable[[List[str]], str]] = {
    "timeline": _cmd_timeline,
    "checkpoint": _cmd_checkpoint,
    "rewind": _cmd_rewind,
    "branch": _cmd_branch,
    "diff": _cmd_diff,
    "merge": _cmd_merge,
    "status": _cmd_status,
    "workspace": _cmd_workspace,
    "gc": _cmd_gc,
    "elect": _cmd_elect,
}

_HELP = """Hermes-Janus: Bi-directional temporal state engine & MVCC workspace

Subcommands:
  /janus timeline [n]          Render visual ASCII DAG of execution history
  /janus checkpoint [msg]      Create a named checkpoint
  /janus rewind [steps | sha]  Roll back workspace to an earlier commit
  /janus branch [name]         Create or switch to an execution branch
  /janus diff [sha_a] [sha_b]  Show changes between commits or current workspace
  /janus merge <branch>        Three-way merge an exploration branch
  /janus elect <b1> <b2> ...   Run tournament election between branches and merge the winner
  /janus status                Display branch, HEAD, tracked files, and disk usage
  /janus workspace [path]      Show or set the active workspace directory
  /janus gc                    Run Mark-and-Sweep garbage collection on stale blobs
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


def tool_janus_checkpoint(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    args = args or {}
    message = args.get("message", "Agent checkpoint")
    engine = get_engine()
    commit = engine.snapshot(message=message, author="agent")
    return f"Created checkpoint [{commit.sha[:7]}] on branch '{commit.branch}'."


def tool_janus_rewind(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    args = args or {}
    engine = get_engine()
    target = args.get("sha") or args.get("steps", 1)
    if isinstance(target, str) and target.isdigit():
        target = int(target)
    success, msg = engine.rewind(target)
    return msg


def tool_janus_branch(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    args = args or {}
    branch_name = args.get("branch_name", "experiment")
    engine = get_engine()
    success, msg = engine.branch(branch_name)
    return msg


def tool_janus_timeline(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    engine = get_engine()
    return engine.dag.render_timeline_ascii(limit=10)


def tool_janus_diff(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    engine = get_engine()
    return engine.diff()


def tool_janus_merge(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    args = args or {}
    branch_name = args.get("branch_name", "")
    if not branch_name:
        return "Error: branch_name is required for merge."
    engine = get_engine()
    success, msg = engine.merge(branch_name)
    return msg


def tool_janus_gc(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    engine = get_engine()
    result = engine.gc()
    freed_kb = result["bytes_freed"] / 1024
    return f"GC complete: pruned {result['commits_pruned']} old commits, removed {result['blobs_removed']} unreferenced blobs, freed {freed_kb:.1f} KB."


def tool_janus_elect(args: Dict[str, Any] = None, **kwargs: Any) -> str:
    args = args or {}
    candidates = args.get("candidates", [])
    if not isinstance(candidates, list) or len(candidates) < 2:
        return "Error: candidates must be a list with at least two branch names."
    test_command = args.get("test_command", "")
    auto_merge = args.get("auto_merge", True)
    engine = get_engine()
    res = engine.tournament(candidates, test_command=test_command, auto_merge=auto_merge)
    if res["winner"]:
        return f"Tournament complete. Winner elected: '{res['winner']}'. {res['merge_message']}"
    return "Tournament complete. No candidate branch met the passing criteria."


_TOOL_DEFINITIONS = [
    (
        "janus_checkpoint",
        {
            "name": "janus_checkpoint",
            "description": "Create a named checkpoint of the workspace to establish a restore point.",
            "parameters": {
                "type": "object",
                "properties": {
                    "message": {"type": "string", "description": "Reason for checkpoint"}
                },
                "required": ["message"],
            },
        },
        tool_janus_checkpoint,
    ),
    (
        "janus_rewind",
        {
            "name": "janus_rewind",
            "description": "Roll back workspace state by N steps or to a specific commit SHA.",
            "parameters": {
                "type": "object",
                "properties": {
                    "steps": {"type": "integer", "description": "Number of commits to roll back (default 1)"},
                    "sha": {"type": "string", "description": "Specific commit SHA to restore (optional)"},
                },
            },
        },
        tool_janus_rewind,
    ),
    (
        "janus_branch",
        {
            "name": "janus_branch",
            "description": "Fork timeline into an isolated exploration branch to test hypotheses safely.",
            "parameters": {
                "type": "object",
                "properties": {
                    "branch_name": {"type": "string", "description": "Name of the branch to create/checkout"}
                },
                "required": ["branch_name"],
            },
        },
        tool_janus_branch,
    ),
    (
        "janus_timeline",
        {
            "name": "janus_timeline",
            "description": "Inspect the visual ASCII timeline and past execution branches.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
        tool_janus_timeline,
    ),
    (
        "janus_diff",
        {
            "name": "janus_diff",
            "description": "View file differences between current workspace state and prior checkpoint.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
        tool_janus_diff,
    ),
    (
        "janus_merge",
        {
            "name": "janus_merge",
            "description": "Three-way merge a verified exploration branch back into the current branch.",
            "parameters": {
                "type": "object",
                "properties": {
                    "branch_name": {"type": "string", "description": "Branch to merge into current"}
                },
                "required": ["branch_name"],
            },
        },
        tool_janus_merge,
    ),
    (
        "janus_gc",
        {
            "name": "janus_gc",
            "description": "Trigger Mark-and-Sweep garbage collection to purge unreferenced CAS blobs and compact WAL.",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
        tool_janus_gc,
    ),
    (
        "janus_elect",
        {
            "name": "janus_elect",
            "description": "Run tournament election across candidate branches to elect and merge the winning implementation based on tests or metrics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "candidates": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of branch names to evaluate",
                    },
                    "test_command": {
                        "type": "string",
                        "description": "Optional command to run for evaluating each branch (e.g. 'go test ./...' or 'pytest')",
                    },
                    "auto_merge": {
                        "type": "boolean",
                        "description": "Whether to merge the elected winner into the base branch (default true)",
                    },
                },
                "required": ["candidates"],
            },
        },
        tool_janus_elect,
    ),
]


def register(ctx: Any) -> None:
    ctx.register_hook("pre_tool_call", _on_pre_tool_call)
    ctx.register_hook("post_tool_call", _on_post_tool_call)
    ctx.register_hook("on_session_end", _on_session_end)

    if hasattr(ctx, "register_system_prompt_section"):
        ctx.register_system_prompt_section(
            id="hermes-janus",
            content=_SYSTEM_PROMPT,
        )

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
