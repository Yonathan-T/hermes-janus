"""
Hermes-Janus Live Demonstration
Demonstrates CAS snapshots, instant atomic rewind, and Merkle branching.
"""

import shutil
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from hermes_janus.engine import JanusEngine


def print_step(title):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def main():
    workspace = Path(__file__).resolve().parent / "demo_workspace"
    # Clean up previous demo run if any
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(exist_ok=True)

    print_step("STEP 1: Setting up Clean Project Workspace")
    app_file = workspace / "calculator.py"
    app_file.write_text(
        "def add(a, b):\n    return a + b\n\ndef divide(a, b):\n    return a / b\n",
        encoding="utf-8",
    )
    print(f"Created file: {app_file.name}")
    print("--- Content ---")
    print(app_file.read_text().strip())

    engine = JanusEngine(workspace)
    print(f"\nInitialized Janus Engine in: {workspace / '.janus'}")

    print_step("STEP 2: Creating Baseline Snapshot (CAS + Merkle DAG)")
    t0 = time.perf_counter()
    c1 = engine.snapshot("Initial working calculator state")
    t1 = time.perf_counter()
    print(f"Commit ID: [{c1.sha[:7]}] - '{c1.message}'")
    print(f"Zero-copy CAS Snapshot Latency: {(t1 - t0)*1000:.2f} ms")

    print_step("STEP 3: Simulating Agent Hallucination / Breaking Code")
    print("Hermes agent makes an edit and corrupts the divide logic:")
    app_file.write_text(
        "def add(a, b):\n    return a + b\n\ndef divide(a, b):\n    # BROKEN BY AGENT:\n    raise ZeroDivisionError('AGENT HALLUCINATED CODE')\n",
        encoding="utf-8",
    )
    print("--- Broken Content ---")
    print(app_file.read_text().strip())

    # Snapshot of the broken edit (captured automatically by Janus post-tool hook)
    c2 = engine.snapshot("Agent broke divide function")
    print(f"\nAuto-checkpoint: [{c2.sha[:7]}]")

    print_step("STEP 4: Testing the Broken Code")
    try:
        ns = {}
        exec(app_file.read_text(), ns)
        ns["divide"](10, 2)
        print("Test passed! (Unexpected)")
    except Exception as e:
        print(f"❌ TEST FAILED: {type(e).__name__}: {e}")

    print_step("STEP 5: Running Instant Atomic Rewind (/janus rewind 1)")
    t0 = time.perf_counter()
    ok, msg = engine.rewind(1)
    t1 = time.perf_counter()
    print(f"Status: {msg}")
    print(f"Atomic Rollback Latency: {(t1 - t0)*1000:.2f} ms")

    print("\n--- Restored Content ---")
    print(app_file.read_text().strip())

    # Retest after rollback
    ns_restored = {}
    exec(app_file.read_text(), ns_restored)
    res = ns_restored["divide"](10, 2)
    print(f"\n✅ TEST PASSED AFTER REWIND: divide(10, 2) = {res}")

    print_step("STEP 6: Speculative Merkle-DAG Branching (/janus branch)")
    ok, msg = engine.branch("feature-math")
    print(f"Branch created: {msg}")

    # Add a new feature on this branch
    app_file.write_text(
        app_file.read_text().strip() + "\n\ndef power(a, b):\n    return a ** b\n",
        encoding="utf-8",
    )
    c3 = engine.snapshot("Added power function on feature-math branch")
    print(f"Committed [{c3.sha[:7]}] on 'feature-math'")

    # Switch back to main and merge
    engine.branch("main")
    print("Switched back to 'main'")
    ok, merge_msg = engine.merge("feature-math")
    print(f"Merge status: {merge_msg}")

    print_step("STEP 7: Visual Execution Merkle Timeline (/janus timeline)")
    print(engine.dag.render_timeline_ascii())

    print("=" * 65)
    print("  🎉 JANUS DEMO SUCCEEDED — 100% Reliable Workspace Protection!")
    print("=" * 65)


if __name__ == "__main__":
    main()
