# Hermes-Janus 🚪⏪⏩

> **Bi-directional temporal state engine & MVCC workspace for Hermes Agent.**  
> Powered by Content-Addressable Storage (CAS), Write-Ahead Logging (WAL), and Merkle-DAG execution branching.

---

## ⚡ The Problem: The Destructive Agent Loop

Every autonomous AI coding agent suffers from **destructive agent drift**:
1. The agent makes changes across multiple files and executes shell commands.
2. If tests fail or code breaks, standard agents lack transactional guarantees: they hallucinate workarounds, leave half-edited files behind, and pollute the user's workspace.
3. Developers are forced to either run `git reset --hard` manually (losing everything) or abandon the agent session.

---

## 🛡️ The Solution: Janus Architecture

**Janus** is the Roman god of doorways, beginnings, transitions, and time—with one face looking back into the past and one looking forward into the future.

`hermes-janus` brings distributed database and version-control principles into Hermes Agent's execution loop:

```
                  ┌────────────────────────┐
                  │   Hermes Tool Call     │
                  │ (write_file, bash, etc)│
                  └───────────┬────────────┘
                              │
                    [ pre_tool_call hook ]
                              │
               ┌──────────────▼──────────────┐
               │    Write-Ahead Log (WAL)    │
               │   State: PREPARED (Intent)  │
               └──────────────┬──────────────┘
                              │
                     Execute Tool on Disk
                              │
                    [ post_tool_call hook ]
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
  [ On Success ]                               [ On Failure ]
        │                                           │
 ┌──────▼───────────────────────┐             ┌─────▼─────────────────────────┐
 │ Content-Addressable Store    │             │   WAL Abort / Instant Rewind  │
 │ • SHA-256 Blobs (Zero-Copy)  │             │   • Revert to last clean CAS  │
 │ • Merkle DAG Snapshot Commit │             │   • Workspace restored in <5ms│
 │ • Vector Clock Incremented   │             └───────────────────────────────┘
 └──────────────────────────────┘
```

### Key Distributed Systems Concepts

1. **Content-Addressable Storage (CAS):**  
   Files are hashed using SHA-256 and stored as immutable blobs. Snapshots take **< 5 milliseconds** and virtually 0 bytes of extra disk space because unchanged files share identical hashes across commits.

2. **Write-Ahead Logging (WAL):**  
   Before any mutating action (`write_file`, `patch`, `terminal`) touches disk, the intent and previous file states are durably logged in `wal.jsonl`. If an operation fails or the session is interrupted, the transaction aborts cleanly.

3. **Merkle-DAG & Vector Clocks:**  
   Every snapshot is an immutable node in a directed acyclic graph. Vector clocks track logical causality across parallel agent branches.

4. **Hypothesis Branching & 3-Way Merge:**  
   Hermes (or the user) can fork into isolated branches (e.g., `hypothesis-a` vs `hypothesis-b`), evaluate alternative implementations, and merge winning solutions back into `main`.

---

## 🚀 Features & Commands

### In-Chat Slash Commands (`/janus`)

| Command | Description |
|---|---|
| `/janus timeline [n]` | Renders a visual ASCII Merkle-DAG of past commits and branches. |
| `/janus checkpoint [msg]` | Manually saves a named workspace checkpoint. |
| `/janus rewind [steps \| sha]` | Instantly rolls back the workspace to a previous commit. |
| `/janus branch [name]` | Forks or switches to an execution branch. |
| `/janus diff [sha_a] [sha_b]` | Shows a unified diff between commits or against current files. |
| `/janus merge <branch>` | Performs a 3-way merge from an exploration branch into current. |
| `/janus status` | Displays active branch, HEAD commit, tracked files, and disk storage usage. |
| `/janus gc` | Runs Mark-and-Sweep garbage collection on stale blobs and compacts the WAL. |

### Autonomous Agent Tools

Hermes can autonomously use Janus tools during complex tasks:
* `janus_checkpoint`: Create a named restore point before attempting risky refactors.
* `janus_rewind`: Automatically revert to a clean state if test suites fail.
* `janus_branch`: Fork execution into alternative solution paths.
* `janus_timeline`: Inspect past states and decision history.
* `janus_diff`: Review changes made during a task.
* `janus_merge`: Merge successful branch fixes.
* `janus_gc`: Sweep unreferenced blobs and prune stale commit history to keep workspace overhead minimal.

---

## 📊 Visual Timeline Example

```text
=== Janus Merkle Timeline (Branch: main) ===

  ●  [4f6f6d4] 17:32:23 (HEAD -> main) | Merge branch 'feature-x' into main
  │    author: janus | changes: +1 ~0 -0
  │
  ○  [773f8fa] 17:31:16 (main) | v1
  │    author: agent | changes: +1 ~0 -0
  │
  ○  [c81a29b] 17:31:00 (main) | Initial repository state
       author: system | changes: +0 ~0 -0
```

---

## 🧪 Testing & Validation

`hermes-janus` is built with **zero external dependencies** (using Python's standard library `hashlib`, `pathlib`, `shutil`, `json`, and `difflib`).

Run the automated test suite:
```bash
python -m unittest discover -s tests
```

Run official Hermes Agent plugin validation:
```bash
hermes plugins validate .
```

---

## 📄 License

MIT © 2026 Hermes Community
