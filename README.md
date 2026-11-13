# hermes-janus

Janus is a bi-directional temporal state engine and Multi-Version Concurrency Control (MVCC) workspace plugin for [Hermes Agent](https://github.com/NousResearch/hermes-agent). It brings distributed consensus and database storage primitives—Content-Addressable Storage (CAS), Write-Ahead Logging (WAL), Merkle DAG execution branching, and Raft-style tournament leader election—directly into Hermes's execution loop.

Zero LLM calls for state operations: Janus operates deterministically on disk using standard library primitives, ensuring atomic rollback in under 5 milliseconds and zero-copy workspace snapshots.

```
                  ┌────────────────────────┐
                  │    Hermes Tool Call    │
                  │ (write_file, bash, etc)│
                  └───────────┬────────────┘
                              │
                    [ pre_tool_call hook ]
                              │
                ┌─────────────▼─────────────┐
                │   Write-Ahead Log (WAL)   │
                │  State: PREPARED (Intent) │
                └─────────────┬─────────────┘
                              │
                     Execute Tool on Disk
                              │
                    [ post_tool_call hook ]
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
  [ On Success ]                               [ On Failure ]
        │                                           │
 ┌──────▼──────────────────────┐             ┌──────▼────────────────────────┐
 │ Content-Addressable Store   │             │   WAL Abort / Instant Rewind  │
 │ - SHA-256 Blobs (Zero-Copy) │             │   - Revert to last clean CAS  │
 │ - Merkle DAG Snapshot       │             │   - Workspace restored in <5ms│
 │ - Vector Clock Incremented  │             └───────────────────────────────┘
 └─────────────────────────────┘
```

## What you get

A complete temporal workspace manager registered inside Hermes with:

- **Atomic Rollback (`/janus rewind`)** — Reverts single tool mutations or multi-turn failures back to a verified clean state.
- **Speculative Hypothesis Branches (`/janus branch`)** — Allows agents or developers to fork into isolated workspaces to test risky changes without touching the main branch.
- **Raft-Style Tournament Consensus (`/janus elect`)** — Evaluates competing candidate branches through automated test harnesses or benchmark metrics, elects the winning implementation, and auto-merges it into main.
- **Visual ASCII Merkle Timeline (`/janus timeline`)** — Renders the execution history, commit SHAs, and active branch heads in the chat window.
- **Mark-and-Sweep Garbage Collection (`/janus gc`)** — Scans reachable branch tips, purges unreferenced file blobs, and compacts the WAL to keep disk usage under 1 MB.

## Why Janus: Raft & Distributed State Mechanics

Autonomous coding agents typically suffer from destructive drift: when an agent executes multi-step refactors, failed terminal commands and half-edited files pollute the developer's working directory. Standard agents hallucinate patches over broken files or require developers to manually run destructive `git reset --hard` commands.

Janus models the local repository as a replicated state machine:

1. **Raft-Style Log Safety & WAL:** In distributed consensus systems like Raft, state changes must be appended to a durable log before being applied to the state machine. Janus enforces this via `pre_tool_call`: before `write_file`, `patch`, or mutating shell commands touch disk, the intent and previous file states are durably logged to `wal.jsonl`. If an operation crashes or fails validation, the uncommitted transaction is aborted.
2. **Zero-Copy Content-Addressable Storage (CAS):** File contents are stored by their SHA-256 hash in `.janus/objects/`. Snapshotting a 500-file project after an agent modifies 2 files takes under 5 milliseconds and consumes 0 additional bytes for the 498 unchanged files.
3. **Merkle DAG & Vector Clocks:** Snapshots form an immutable directed acyclic graph where each commit references parent SHAs and vector clock timestamps, capturing causal order across parallel agent sessions.
4. **Leader Election & Tournament Consensus:** In a multi-agent or multi-hypothesis setup, multiple candidates propose different architectural designs or bug fixes. Janus checks out candidate branches in isolation, executes the target test suite (`go test`, `pytest`, `cargo test`), measures latency and exit codes, elects the passing candidate with the best metrics, and merges it back into `main`.
5. **Mark-and-Sweep Storage Governance:** Agents generate high file churn. Janus prevents workspace bloat by discovering living branch heads, marking reachable CAS blobs, sweeping dead historical objects, and compacting the WAL log.

## Use Cases

- **Autonomous Error Recovery:** When Hermes runs a shell command or test suite that fails, the agent autonomously executes `janus_rewind` to return to the last known good state rather than patching over corrupted code.
- **A/B Architecture Exploration:** Test competing approaches (e.g. an in-memory sync.Map cache vs Redis) across separate branches, benchmark each independently, and merge the winner.
- **Multi-Agent Swarm Isolation:** Provide isolated execution sandboxes for concurrent subagents without file write collisions or Git index locks.
- **Safe Refactoring Guardrails:** Developers can checkpoint before asking the agent to perform large refactors, knowing any unwanted modifications can be reversed with a single command.

## Slash Commands

Janus exposes subcommands through `/janus` in the Hermes CLI and TUI:

| Command | Arguments | Description |
|---|---|---|
| `/janus status` | None | Displays active branch, HEAD commit, tracked files, and disk storage footprint. |
| `/janus timeline` | `[n]` | Renders an ASCII Merkle DAG showing execution branches and commits. |
| `/janus checkpoint` | `[message]` | Manually saves a named workspace snapshot. |
| `/janus rewind` | `[steps \| sha]` | Restores workspace files to a previous commit in under 5ms. |
| `/janus branch` | `[name]` | Creates or checks out an isolated execution branch. |
| `/janus diff` | `[sha_a] [sha_b]` | Shows unified diff between commits or against the current workspace. |
| `/janus merge` | `<branch>` | Performs a 3-way merge from an exploration branch into current. |
| `/janus elect` | `<b1> <b2> ... [--test <cmd>]` | Runs tournament election across candidate branches and merges the winner. |
| `/janus gc` | None | Runs mark-and-sweep garbage collection and compacts the WAL. |

## Autonomous Agent Tools

Hermes can autonomously invoke Janus tools via function calling:

| Tool Name | Parameters | Description |
|---|---|---|
| `janus_checkpoint` | `message` (str) | Records a named workspace restore point before risky modifications. |
| `janus_rewind` | `steps` (int), `sha` (str) | Automatically rolls back workspace to a verified clean state. |
| `janus_branch` | `branch_name` (str) | Forks execution into an isolated branch to test a hypothesis safely. |
| `janus_timeline` | None | Inspects the Merkle timeline and branch history. |
| `janus_diff` | None | Inspects uncommitted or inter-commit workspace file differences. |
| `janus_merge` | `branch_name` (str) | Merges a successful candidate branch back into current branch. |
| `janus_elect` | `candidates` (list), `test_command` (str) | Runs tournament election across branches and auto-merges the winner. |
| `janus_gc` | None | Purges unreferenced blobs and stale commits to maintain minimal disk overhead. |

## Workspace State, Privacy & Storage

To deliver instant atomic rollbacks without external daemons or cloud dependencies, Janus stores state locally within the active workspace:

1. **Local `.janus/` Directory:** Janus maintains a hidden `.janus/` folder in the root of the active workspace containing:
   - **Content-Addressable Storage (`.janus/store/`):** Immutable SHA-256 blobs of workspace files. When a snapshot is taken, full copies of tracked workspace files—**including `.env` or sensitive configuration files if present in the workspace**—are saved locally to guarantee exact bit-for-bit restore capability.
   - **Write-Ahead Log (`.janus/wal/`):** An append-only WAL (`wal.log`) that records mutating tool calls (`write_file`, `edit_file`, `terminal`, etc.) along with the tool arguments and pre-mutation hashes.
   - **Merkle DAG Commits (`.janus/dag/`):** JSON metadata files tracking commit history, branch pointers, and vector clocks.
2. **Automatic `.gitignore` Protection:** On initialization, Janus appends `.janus/` to the workspace `.gitignore` to ensure internal snapshots and WAL logs are never committed to your upstream Git repository.
3. **Preservation of Untracked Files:** On `/janus rewind` or branch checkout, Janus only restores and unlinks files that were previously tracked in Janus snapshots. Untracked user files are strictly preserved and never deleted.
4. **Local Data Isolation:** All snapshots, WAL logs, and CAS objects reside strictly on local disk; Janus never transmits workspace data or telemetry externally.

## Install

From the Nous plugin catalog:

```bash
hermes plugins install hermes-janus --enable
```

Or directly from repository:

```bash
git clone https://github.com/Yonathan-T/hermes-janus.git ~/.hermes/plugins/hermes-janus
```

Validate the installation against Hermes's catalog contract:

```bash
hermes plugins validate ~/.hermes/plugins/hermes-janus
```

Requires Hermes Agent >= 0.20.0.

## Repository Layout

```text
hermes-janus/
├── plugin.yaml                  # Plugin manifest, declared tools, and hooks
├── pyproject.toml               # Package build configuration
├── README.md                    # Documentation and architecture guide
├── hermes_janus/
│   ├── __init__.py              # Plugin entry point, slash commands, and tool registrations
│   ├── cas.py                   # Zero-copy Content-Addressable Storage (SHA-256)
│   ├── wal.py                   # Write-Ahead Log for durable mutation logging & compaction
│   ├── dag.py                   # Merkle Directed Acyclic Graph & vector clocks
│   └── engine.py                # State coordinator, tournament election, and 3-way merge
├── plugin-catalog/
│   └── hermes-janus.yaml        # Official Hermes Agent catalog admission manifest
└── tests/
    └── test_janus.py            # Standard library unittest suite (6/6 tests)
```

## Testing

Run the automated test suite using Python's standard library:

```bash
python -m unittest discover -s tests -v
```

Output:
```text
test_cas_storage (test_janus.TestJanus.test_cas_storage) ... ok
test_engine_rewind_and_branch (test_janus.TestJanus.test_engine_rewind_and_branch) ... ok
test_garbage_collection (test_janus.TestJanus.test_garbage_collection) ... ok
test_merkle_dag (test_janus.TestJanus.test_merkle_dag) ... ok
test_tournament_election (test_janus.TestJanus.test_tournament_election) ... ok
test_wal_durability (test_janus.TestJanus.test_wal_durability) ... ok

----------------------------------------------------------------------
Ran 6 tests in 1.118s

OK
```

## License

MIT
